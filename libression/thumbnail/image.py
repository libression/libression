import io
import logging
import typing
import av
import cv2
import numpy
import httpx
import PIL.Image
import pillow_heif
import libression.config
import libression.entities.media

logger = logging.getLogger(__name__)

pillow_heif.register_heif_opener()


def _heif_thumbnail_from_pillow(
    byte_stream: typing.BinaryIO,
    width_in_pixels: int,
) -> bytes:
    logger.debug("Starting HEIF thumbnail generation")
    # First convert HEIF to numpy array
    with PIL.Image.open(byte_stream) as img:
        logger.debug(
            f"Opened HEIF image: format={img.format}, size={img.size}, mode={img.mode}"
        )

        # Convert to RGB (HEIF can be in various color spaces)
        img_rgb = img.convert("RGB")
        logger.debug("Converted image to RGB mode")

        # Convert to numpy array for OpenCV processing
        img_array = numpy.array(img_rgb)
        logger.debug(f"Converted to numpy array: shape={img_array.shape}")

        # Free up PIL Image memory
        img_rgb.close()

    # Use OpenCV for resizing (more memory efficient)
    height = int(img_array.shape[0] * width_in_pixels / img_array.shape[1])
    logger.debug(f"Resizing to {width_in_pixels}x{height}")
    resized = cv2.resize(
        img_array, (width_in_pixels, height), interpolation=cv2.INTER_AREA
    )

    # Convert from RGB to BGR (OpenCV format)
    resized_bgr = cv2.cvtColor(resized, cv2.COLOR_RGB2BGR)

    # Encode to JPEG
    _, buffer = cv2.imencode(".jpg", resized_bgr, [cv2.IMWRITE_JPEG_QUALITY, 85])
    result = buffer.tobytes()
    logger.debug(f"Successfully generated JPEG thumbnail: size={len(result)} bytes")

    return result


def _image_thumbnail_from_opencv(
    byte_stream: typing.BinaryIO,
    width_in_pixels: int,
) -> bytes | None:
    try:
        byte_stream.seek(0)

        content = byte_stream.read()
        if not content:
            raise RuntimeError("Empty bytestream? shouldn't be here?")

        # Read stream and convert to numpy array
        file_bytes = numpy.asarray(bytearray(content), dtype=numpy.uint8)
        img = cv2.imdecode(file_bytes, cv2.IMREAD_COLOR)

        if img is None:
            logger.error("Failed to decode image")
            return None  # Return empty bytes for invalid images

        # Calculate new height maintaining aspect ratio
        height = int(img.shape[0] * width_in_pixels / img.shape[1])
        resized = cv2.resize(
            img, (width_in_pixels, height), interpolation=cv2.INTER_AREA
        )

        # Encode to JPEG
        success, buffer = cv2.imencode(".jpg", resized, [cv2.IMWRITE_JPEG_QUALITY, 85])
        if not success:
            logger.error("Failed to encode image")
            return None  # Return empty bytes for invalid images

        return buffer.tobytes()
    finally:
        byte_stream.seek(0)


def _process_video_frames(
    container: av.container.Container,
    width_in_pixels: int,
    frame_count: int = libression.config.THUMBNAIL_FRAME_COUNT,
) -> bytes | None:
    try:
        if not container.streams.video:
            logger.error("No video stream found")
            return None

        stream = container.streams.video[0]
        frames: list[PIL.Image.Image] = []

        logger.debug(
            f"Video info: format={container.format.name}, "
            f"duration={stream.duration}, "
            f"frames={stream.frames}, "
            f"fps={stream.average_rate}, "
            f"rotation={stream.metadata.get('rotate', 0)}"
        )

        # Special handling for GIFs
        is_gif = container.format.name == "gif"
        if is_gif:
            # For GIFs, try to keep original frame timing
            frame_count = min(frame_count, stream.frames or 10)  # Limit frames
            frames = []
            durations = []

            for frame in container.decode(video=0):
                if len(frames) >= frame_count:
                    break

                # Resize frame
                height = int(frame.height * width_in_pixels / frame.width)
                frame = frame.reformat(width=width_in_pixels, height=height)
                frames.append(frame.to_image())

                # Get frame duration in milliseconds
                duration = frame.time_base * frame.pts * 1000
                durations.append(
                    int(duration) or 100
                )  # Default to 100ms if no duration

        else:
            logger.debug("Processing video file...")
            stream.thread_type = "AUTO"
            container.seek(0)

            # First count total frames
            total_frames = 0
            for _ in container.decode(video=0):
                total_frames += 1

            logger.debug(f"Total frames in video: {total_frames}")
            container.seek(0)

            # If we have less than requested frames, just get all of them
            if total_frames <= frame_count:
                frames = []
                for frame in container.decode(video=0):
                    height = int(frame.height * width_in_pixels / frame.width)
                    frame = frame.reformat(width=width_in_pixels, height=height)
                    pil_image = frame.to_image()

                    # Handle rotation
                    rotation = int(stream.metadata.get("rotate", 0))
                    if rotation == 90:
                        pil_image = pil_image.transpose(PIL.Image.ROTATE_270)
                    elif rotation == 270:
                        pil_image = pil_image.transpose(PIL.Image.ROTATE_90)
                    elif rotation == 180:
                        pil_image = pil_image.transpose(PIL.Image.ROTATE_180)

                    frames.append(pil_image)

            # If we have more frames than requested, get evenly spaced frames
            else:
                frame_interval = total_frames // (
                    frame_count - 1
                )  # -1 because we include last frame
                frame_positions = [i * frame_interval for i in range(frame_count - 1)]
                frame_positions.append(total_frames - 1)  # Last frame

                logger.debug(f"Getting frames at positions: {frame_positions}")

                frames = []
                current_frame = 0
                for frame in container.decode(video=0):
                    if current_frame in frame_positions:
                        height = int(frame.height * width_in_pixels / frame.width)
                        frame = frame.reformat(width=width_in_pixels, height=height)
                        pil_image = frame.to_image()

                        # Handle rotation
                        rotation = int(stream.metadata.get("rotate", 0))
                        if rotation == 90:
                            pil_image = pil_image.transpose(PIL.Image.ROTATE_270)
                        elif rotation == 270:
                            pil_image = pil_image.transpose(PIL.Image.ROTATE_90)
                        elif rotation == 180:
                            pil_image = pil_image.transpose(PIL.Image.ROTATE_180)

                        frames.append(pil_image)
                        if len(frames) >= frame_count:
                            break
                    current_frame += 1

            logger.debug(f"Collected {len(frames)} frames")

        if not frames:
            raise RuntimeError("No frames found in video/gif")

        # Create output GIF with explicit animation settings
        logger.debug(f"About to create GIF with {len(frames)} frames")
        gif_buffer = io.BytesIO()

        # Ensure we have exactly 5 frames
        if len(frames) < frame_count:
            logger.warning(f"Only got {len(frames)} frames, expected {frame_count}")
        elif len(frames) > frame_count:
            # Take first, last, and three evenly spaced frames in between
            indices = [
                0,
                len(frames) // 4,
                len(frames) // 2,
                (3 * len(frames)) // 4,
                len(frames) - 1,
            ]
            frames = [frames[i] for i in indices]

        # Convert frames to P mode
        processed_frames = []
        for i, frame in enumerate(frames):
            frame = frame.convert("P", palette=PIL.Image.Palette.ADAPTIVE)
            processed_frames.append(frame)
            logger.debug(f"Frame {i}: size={frame.size}, mode={frame.mode}")

        logger.debug(f"Processed {len(processed_frames)} frames for GIF")

        # Save all frames at once
        processed_frames[0].save(
            gif_buffer,
            format="GIF",
            save_all=True,
            append_images=processed_frames[1:],
            duration=200,
            loop=0,
        )

        return gif_buffer.getvalue()

    except Exception as e:
        logger.error(f"Error processing video frames: {e}")
        return None


def _video_thumbnail_from_av(
    byte_stream: typing.BinaryIO,
    width_in_pixels: int,
    frame_count: int = libression.config.THUMBNAIL_FRAME_COUNT,
) -> bytes | None:
    try:
        container = av.open(byte_stream)
        return _process_video_frames(container, width_in_pixels, frame_count)
    except (av.error.OSError, av.error.InvalidDataError) as e:
        logger.error(f"Error processing video/gif from byte stream: {e}")
        return None  # Return empty bytes for invalid images
    finally:
        if "container" in locals():
            container.close()


def _video_thumbnail_from_av_with_url(
    url: str,
    width_in_pixels: int,
    frame_count: int = libression.config.THUMBNAIL_FRAME_COUNT,
) -> bytes | None:
    try:
        container = av.open(url)
        return _process_video_frames(container, width_in_pixels, frame_count)
    except (av.error.OSError, av.error.InvalidDataError) as e:
        logger.error(f"Error processing video/gif from URL: {e}")
        return None  # Return empty bytes for invalid images
    finally:
        if "container" in locals():
            container.close()


def generate(
    byte_stream: typing.BinaryIO,
    width_in_pixels: int,
    mime_type: libression.entities.media.SupportedMimeType,
) -> bytes | None:
    if mime_type in libression.entities.media.HEIC_PROCESSING_MIME_TYPES:
        return _heif_thumbnail_from_pillow(byte_stream, width_in_pixels)
    elif mime_type in libression.entities.media.OPEN_CV_PROCESSING_MIME_TYPES:
        return _image_thumbnail_from_opencv(byte_stream, width_in_pixels)
    elif mime_type in libression.entities.media.AV_PROCESSING_MIME_TYPES:
        return _video_thumbnail_from_av(byte_stream, width_in_pixels)

    return None  # Return empty bytes for invalid images


def generate_from_presigned_url(
    presigned_url: str,
    original_mime_type: libression.entities.media.SupportedMimeType,
    width_in_pixels: int,
) -> bytes | None:
    if original_mime_type in libression.entities.media.AV_PROCESSING_MIME_TYPES:
        return _video_thumbnail_from_av_with_url(presigned_url, width_in_pixels)

    # Not video, so not as big, get entire file
    byte_stream: typing.BinaryIO | None = None

    try:
        response = httpx.get(presigned_url, verify=False, follow_redirects=True)
        response.raise_for_status()
        byte_stream = io.BytesIO(response.content)

        if original_mime_type in libression.entities.media.HEIC_PROCESSING_MIME_TYPES:
            return _heif_thumbnail_from_pillow(byte_stream, width_in_pixels)
        elif (
            original_mime_type
            in libression.entities.media.OPEN_CV_PROCESSING_MIME_TYPES
        ):
            return _image_thumbnail_from_opencv(byte_stream, width_in_pixels)
        return None  # Return empty bytes for invalid images
    finally:
        if "byte_stream" in locals() and byte_stream is not None:
            byte_stream.close()
