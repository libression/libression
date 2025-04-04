import io
import logging
import typing
import cv2
import numpy
import httpx
import PIL.Image
import pillow_heif
import libression.config
import libression.entities.media
import ffmpeg
import tempfile
import os
from typing import Optional
import shutil
import subprocess


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

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


def _video_thumbnail_from_ffmpeg(
    byte_stream: typing.BinaryIO,
    width_in_pixels: int,
    frame_count: int = libression.config.THUMBNAIL_FRAME_COUNT,
) -> bytes | None:
    try:
        # Ensure width is even
        width_in_pixels = (width_in_pixels // 2) * 2

        # Write input to temp file since ffmpeg-python needs a file path
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=True) as temp_input:
            temp_input.write(byte_stream.read())
            temp_input.flush()

            # Get input video information
            probe = ffmpeg.probe(temp_input.name)
            video_info = next(s for s in probe["streams"] if s["codec_type"] == "video")

            # Get dimensions considering rotation
            input_width = int(video_info["width"])
            input_height = int(video_info["height"])

            # Check for rotation
            rotation = 0
            if "side_data_list" in video_info:
                for data in video_info["side_data_list"]:
                    if data.get("rotation") is not None:
                        rotation = abs(int(data["rotation"]))

            # Swap dimensions if video is rotated 90 or 270 degrees
            if rotation in [90, 270]:
                input_width, input_height = input_height, input_width

            # Calculate output height maintaining aspect ratio
            output_height = max(
                (width_in_pixels * input_height // input_width) // 2 * 2, 2
            )

            logger.debug(
                f"Input dimensions: {input_width}x{input_height}, rotation: {rotation}"
            )
            logger.debug(f"Output dimensions: {width_in_pixels}x{output_height}")

            # Create temp output file
            with tempfile.NamedTemporaryFile(suffix=".mp4", delete=True) as temp_output:
                # Build the ffmpeg pipeline
                stream = ffmpeg.input(temp_input.name).output(
                    temp_output.name,
                    s=f"{width_in_pixels}x{output_height}",  # explicit dimensions
                    r=2,  # 2 fps
                    vcodec="mpeg4",  # simpler codec
                    pix_fmt="yuv420p",  # force pixel format
                    an=None,  # remove audio stream
                    y=None,  # overwrite output
                )

                # Get the ffmpeg command for debugging
                cmd = ffmpeg.compile(stream)
                logger.debug(f"FFmpeg command: {' '.join(cmd)}")

                # Run the ffmpeg command
                logger.debug("Running ffmpeg command...")
                ffmpeg.run(stream, capture_stdout=True, capture_stderr=True)

                # Read the result
                temp_output.seek(0)
                result = temp_output.read()
                logger.debug(f"Generated thumbnail video: size={len(result)} bytes")
                return result

    except ffmpeg.Error as e:
        logger.error(f"FFmpeg error: {e.stderr.decode() if e.stderr else str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error processing video: {e}")
        return None


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
        return _video_thumbnail_from_ffmpeg(byte_stream, width_in_pixels)

    return None  # Return empty bytes for invalid images


def _get_ffmpeg_path() -> Optional[str]:
    """Get the path to ffmpeg executable."""
    return shutil.which("ffmpeg")


def generate_video_thumbnail(
    input_path: str, width_pixels: int = 400, duration_seconds: int = 5, fps: int = 2
) -> bytes:
    """Generate a video thumbnail and return the bytes."""
    ffmpeg_path = _get_ffmpeg_path()
    if not ffmpeg_path:
        raise RuntimeError("FFmpeg is not installed or not found in PATH")

    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as temp_output:
        try:
            # Exactly match the working command
            cmd = [
                ffmpeg_path,
                "-noautorotate",
                "-i",
                input_path,
                "-vf",
                f"fps={fps},scale=w='if(lt(ih,iw),{width_pixels}*iw/ih,{width_pixels})':h='if(lt(ih,iw),{width_pixels},{width_pixels}*ih/iw)',crop={width_pixels}:{width_pixels}:(iw-{width_pixels})/2:(ih-{width_pixels})/2",
                "-t",
                str(duration_seconds),
                "-c:v",
                "libx264",
                "-an",
                "-pix_fmt",
                "yuv420p",
                "-y",
                temp_output.name,
            ]

            # Don't capture output in text mode, use binary
            process = subprocess.run(
                cmd,
                capture_output=True,
                text=False,  # Changed to handle binary output
            )

            if process.returncode != 0:
                raise RuntimeError(f"FFmpeg error: {process.stderr.decode()}")

            # Read the output file
            with open(temp_output.name, "rb") as f:
                return f.read()

        finally:
            # Clean up the temporary file
            if os.path.exists(temp_output.name):
                os.unlink(temp_output.name)


def create_square_video_thumbnail_from_presigned_url(
    presigned_url: str, width_pixels: int = 400, duration_seconds: int = 5, fps: int = 2
) -> Optional[bytes]:
    """Generate a square video thumbnail from a presigned URL"""
    try:
        return generate_video_thumbnail(
            presigned_url,
            width_pixels=width_pixels,
            duration_seconds=duration_seconds,
            fps=fps,
        )
    except Exception as e:
        logger.error(f"Error generating video thumbnail: {str(e)}")
        return None


def generate_from_presigned_url(
    presigned_url: str,
    original_mime_type: libression.entities.media.SupportedMimeType,
    width_in_pixels: int,
) -> bytes | None:
    if original_mime_type in libression.entities.media.AV_PROCESSING_MIME_TYPES:
        return create_square_video_thumbnail_from_presigned_url(
            presigned_url, width_in_pixels
        )

    # Not video, so handle as before
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
        return None
    finally:
        if byte_stream is not None:
            byte_stream.close()
