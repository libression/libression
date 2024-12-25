import cv2
import numpy
import io
import logging
import libression.entities.media
import PIL.Image
import pillow_heif
import av


logger = logging.getLogger(__name__)

pillow_heif.register_heif_opener()


def _heif_thumbnail_from_pillow(
    byte_stream: io.IOBase,
    width_in_pixels: int,
) -> bytes:
    # First convert HEIF to numpy array
    with PIL.Image.open(byte_stream) as img:
        # Convert to RGB (HEIF can be in various color spaces)
        img_rgb = img.convert('RGB')
        # Convert to numpy array for OpenCV processing
        img_array = numpy.array(img_rgb)
        # Free up PIL Image memory
        img_rgb.close()
    
    # Use OpenCV for resizing (more memory efficient)
    height = int(img_array.shape[0] * width_in_pixels / img_array.shape[1])
    resized = cv2.resize(img_array, (width_in_pixels, height), interpolation=cv2.INTER_AREA)
    
    # Convert from RGB to BGR (OpenCV format)
    resized_bgr = cv2.cvtColor(resized, cv2.COLOR_RGB2BGR)
    
    # Encode to JPEG
    _, buffer = cv2.imencode('.jpg', resized_bgr, [cv2.IMWRITE_JPEG_QUALITY, 85])
    return buffer.tobytes()


def _image_thumbnail_from_opencv(
    byte_stream: io.IOBase,
    width_in_pixels: int,
) -> bytes:
    # Read stream and convert to numpy array
    file_bytes = numpy.asarray(bytearray(byte_stream.read()), dtype=numpy.uint8)
    img = cv2.imdecode(file_bytes, cv2.IMREAD_COLOR)
    
    if img is None:
        return b""  # Return empty bytes for invalid images
        
    # Calculate new height maintaining aspect ratio
    height = int(img.shape[0] * width_in_pixels / img.shape[1])
    resized = cv2.resize(img, (width_in_pixels, height), interpolation=cv2.INTER_AREA)
    
    # Encode to JPEG
    _, buffer = cv2.imencode('.jpg', resized, [cv2.IMWRITE_JPEG_QUALITY, 85])
    return buffer.tobytes()


def _video_thumbnail_from_av(
    byte_stream: io.IOBase,
    width_in_pixels: int,
    frame_count: int = 5
) -> bytes | None:
    try:
        container = av.open(byte_stream)
        
        if not container.streams.video:
            logger.error("No video stream found")
            return None
            
        stream = container.streams.video[0]
        frames = []
        
        logger.debug(f"Video info: format={container.format.name}, "
                    f"duration={stream.duration}, "
                    f"frames={stream.frames}, "
                    f"fps={stream.average_rate}")
        
        # Special handling for GIFs
        is_gif = container.format.name == 'gif'
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
                durations.append(int(duration) or 100)  # Default to 100ms if no duration
                
        else:
            stream.thread_type = 'AUTO'
            container.seek(0)  # Reset to start
            
            # First try: Just get first few frames sequentially
            for frame in container.decode(video=0):
                height = int(frame.height * width_in_pixels / frame.width)
                frame = frame.reformat(width=width_in_pixels, height=height)
                frames.append(frame.to_image())
                if len(frames) >= 2:  # We only need 2 frames minimum
                    break
            
            # If that didn't work, try seeking
            if len(frames) < 2 and stream.duration and stream.duration > 0:
                logger.debug("Trying seek strategy")
                container.seek(stream.duration // 2)  # Try middle of video
                try:
                    frame = next(container.decode(video=0))
                    height = int(frame.height * width_in_pixels / frame.width)
                    frame = frame.reformat(width=width_in_pixels, height=height)
                    frames.append(frame.to_image())
                except StopIteration:
                    pass
        
        if not frames:
            raise RuntimeError("No frames found in video/gif")
            
        # Ensure we have at least 2 frames
        if len(frames) == 1:
            frames.append(frames[0])
            
        logger.debug(f"Extracted {len(frames)} frames")
        
        # Create output GIF with explicit animation settings
        gif_buffer = io.BytesIO()
        frames[0].save(
            gif_buffer,
            format='GIF',
            save_all=True,
            append_images=frames[1:],
            duration=1000,
            loop=0,
            disposal=2,
            version='GIF89a'
        )
        
        # Verify the GIF is animated
        gif_buffer.seek(0)
        test_gif = PIL.Image.open(gif_buffer)
        logger.debug(f"Output GIF info: animated={test_gif.is_animated}, "
                    f"n_frames={getattr(test_gif, 'n_frames', 1)}")
        
        if not test_gif.is_animated:
            logger.error("Failed to create animated GIF")
            # Force animation by duplicating frame
            gif_buffer = io.BytesIO()
            frames[0].save(
                gif_buffer,
                format='GIF',
                save_all=True,
                append_images=[frames[0]],  # Duplicate first frame
                duration=1000,
                loop=0,
                disposal=2,
                version='GIF89a'
            )
        
        return gif_buffer.getvalue()
        
    except av.AVError as e:
        logger.error(f"Error processing video/gif: {e}")
        return None
    finally:
        container.close()

def generate(
    byte_stream: io.IOBase,
    width_in_pixels: int,
    mime_type: libression.entities.media.SupportedMimeType,
) -> bytes | None:

    if isinstance(mime_type, libression.entities.media.HeifMimeType):
        return _heif_thumbnail_from_pillow(byte_stream, width_in_pixels)
    elif isinstance(mime_type, libression.entities.media.OpenCvProccessableImageMimeType):
        return _image_thumbnail_from_opencv(byte_stream, width_in_pixels)
    elif isinstance(mime_type, libression.entities.media.AvProccessableMimeType):
        return _video_thumbnail_from_av(byte_stream, width_in_pixels)

    return None
