import pytest
import io
import numpy
import cv2
import libression.thumbnail
import libression.entities.media
from PIL import Image


# minimal_image parameterized fixture
@pytest.mark.parametrize("minimal_image,mime_type", [
    ("jpg", libression.entities.media.OpenCvProccessableImageMimeType.JPEG),
    ("png", libression.entities.media.OpenCvProccessableImageMimeType.PNG),
    ("tiff", libression.entities.media.OpenCvProccessableImageMimeType.TIFF),
    ("webp", libression.entities.media.OpenCvProccessableImageMimeType.WEBP),
    ("heic", libression.entities.media.HeifMimeType.HEIC),
    ("heif", libression.entities.media.HeifMimeType.HEIF),
], indirect=["minimal_image"])
def test_generate_image_thumbnail(minimal_image, mime_type):
    # Prepare a byte stream for the test
    byte_stream = io.BytesIO(minimal_image)

    # Generate the thumbnail
    thumbnail = libression.thumbnail.generate(byte_stream, 3, mime_type)

    # Convert thumbnail bytes back to image to check dimensions
    nparr = numpy.frombuffer(thumbnail, numpy.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    
    # Check width
    assert img.shape[1] == 3, f"Expected width {3}, got {img.shape[1]}"


# minimal_image parameterized fixture
@pytest.mark.parametrize("minimal_image,mime_type", [
    ("gif", libression.entities.media.AvProccessableMimeType.GIF),
    ("mp4", libression.entities.media.AvProccessableMimeType.MP4),
    ("mpeg", libression.entities.media.AvProccessableMimeType.MPEG),
    ("mov", libression.entities.media.AvProccessableMimeType.QUICKTIME),
    ("webm", libression.entities.media.AvProccessableMimeType.WEBM),
    ("avi", libression.entities.media.AvProccessableMimeType.X_MS_VIDEO),
], indirect=["minimal_image"])
def test_av_generate_video_thumbnail(minimal_image, mime_type):
    # Prepare a byte stream for the test
    byte_stream = io.BytesIO(minimal_image)

    # Generate the thumbnail
    thumbnail = libression.thumbnail.generate(byte_stream, 3, mime_type)

    # Basic checks
    assert thumbnail is not None, "Thumbnail should not be None"
    assert isinstance(thumbnail, bytes), "Thumbnail should be bytes"
    assert len(thumbnail) > 0, "Thumbnail should not be empty"

    # Check if it's a valid GIF
    gif_stream = io.BytesIO(thumbnail)
    gif = Image.open(gif_stream)
    assert gif.format == 'GIF', "Thumbnail should be a GIF"
    
    # Check dimensions
    assert gif.width == 3, f"Expected width 3, got {gif.width}"
    assert gif.is_animated, "GIF should be animated"
