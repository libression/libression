import cv2
import numpy as np
import io
import logging
import libression.entities.media as media
from PIL import Image
from pillow_heif import register_heif_opener
import av

logger = logging.getLogger(__name__)

register_heif_opener()

def _heif_thumbnail_from_pillow(
    byte_stream: io.IOBase,
    width_in_pixels: int,
) -> bytes:
    # First convert HEIF to numpy array
    with Image.open(byte_stream) as img:
        # Convert to RGB (HEIF can be in various color spaces)
        img_rgb = img.convert('RGB')
        # Convert to numpy array for OpenCV processing
        img_array = np.array(img_rgb)
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
    file_bytes = np.asarray(bytearray(byte_stream.read()), dtype=np.uint8)
    img = cv2.imdecode(file_bytes, cv2.IMREAD_COLOR)
    
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
    # Open container in streaming mode
    container = av.open(byte_stream)
    
    # Get video stream
    stream = container.streams.video[0]
    
    # Count total frames (fast operation in av)
    total_frames = stream.frames
    if not total_frames or total_frames <= 0:
        total_frames = 2  # Ensure we try to get at least 2 frames
    
    # Calculate frame interval (ensure we get at least 2 frames)
    interval = min(max(total_frames // frame_count, 1), total_frames - 1)
    
    frames = []
    for i, frame in enumerate(container.decode(video=0)):
        if i % interval == 0 or len(frames) < 2:  # Always take first 2 frames
            # Resize frame maintaining aspect ratio
            height = int(frame.height * width_in_pixels / frame.width)
            frame = frame.reformat(width=width_in_pixels, height=height)
            frames.append(frame.to_image())
            
            if len(frames) >= frame_count:
                break
        
    if not frames:
        raise RuntimeError("No frames found in video")
        
    # Ensure we have at least 2 frames
    if len(frames) == 1:
        frames.append(frames[0])  # Duplicate first frame if only one found
        
    # Save as animated GIF with explicit animation parameters
    gif_buffer = io.BytesIO()
    frames[0].save(
        gif_buffer,
        format='GIF',
        save_all=True,
        append_images=frames[1:],
        duration=1000,  # 1 second per frame
        loop=0,
        optimize=False,  # Avoid optimization that might merge identical frames
        disposal=2,      # Clear previous frame
        version='GIF89a' # Use modern GIF format with animation support
    )
    return gif_buffer.getvalue()

def generate(
    byte_stream: io.IOBase,
    width_in_pixels: int,
    mime_type: media.SupportedMimeType,
) -> bytes | None:

    if isinstance(mime_type, media.HeifMimeType):
        return _heif_thumbnail_from_pillow(byte_stream, width_in_pixels)
    elif isinstance(mime_type, media.OpenCvProccessableImageMimeType):
        return _image_thumbnail_from_opencv(byte_stream, width_in_pixels)
    elif isinstance(mime_type, media.AvProccessableMimeType):
        return _video_thumbnail_from_av(byte_stream, width_in_pixels)

    return None
