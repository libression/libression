import io

import cv2
import numpy
import PIL.Image
import pytest

import libression.entities.media
import libression.thumbnail


# media_fixture_by_filename parameterized fixture
@pytest.mark.parametrize(
    "media_fixture_by_filename,mime_type",
    [
        ("minimal.jpg", libression.entities.media.SupportedMimeType.JPEG),
        ("minimal.png", libression.entities.media.SupportedMimeType.PNG),
        ("minimal.tiff", libression.entities.media.SupportedMimeType.TIFF),
        ("minimal.webp", libression.entities.media.SupportedMimeType.WEBP),
        ("minimal.heic", libression.entities.media.SupportedMimeType.HEIC),
        ("minimal.heif", libression.entities.media.SupportedMimeType.HEIF),
    ],
    indirect=["media_fixture_by_filename"],
)
def test_generate_image_thumbnail(media_fixture_by_filename, mime_type):
    # Prepare a byte stream for the test
    byte_stream = io.BytesIO(media_fixture_by_filename)

    # Generate the thumbnail
    thumbnail = libression.thumbnail.image.generate(byte_stream, 3, mime_type)

    # Convert thumbnail bytes back to image to check dimensions
    nparr = numpy.frombuffer(thumbnail, numpy.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

    # Check width
    assert img.shape[1] == 3, f"Expected width {3}, got {img.shape[1]}"


# media_fixture_by_filename parameterized fixture
@pytest.mark.parametrize(
    "media_fixture_by_filename,mime_type,expected_frame_count",
    [
        ("minimal.gif", libression.entities.media.SupportedMimeType.GIF, 2),
        ("minimal.mp4", libression.entities.media.SupportedMimeType.MP4, 2),
        ("minimal.mpeg", libression.entities.media.SupportedMimeType.MPEG, 2),
        ("minimal.mov", libression.entities.media.SupportedMimeType.QUICKTIME, 2),
        ("minimal.webm", libression.entities.media.SupportedMimeType.WEBM, 2),
        ("minimal.avi", libression.entities.media.SupportedMimeType.X_MS_VIDEO, 2),
        (
            "minimal_15_frames.gif",
            libression.entities.media.SupportedMimeType.GIF,
            libression.config.THUMBNAIL_FRAME_COUNT,
        ),
        (
            "minimal_15_frames.mp4",
            libression.entities.media.SupportedMimeType.MP4,
            libression.config.THUMBNAIL_FRAME_COUNT,
        ),
        (
            "minimal_15_frames.mpeg",
            libression.entities.media.SupportedMimeType.MPEG,
            libression.config.THUMBNAIL_FRAME_COUNT,
        ),
        (
            "minimal_15_frames.mov",
            libression.entities.media.SupportedMimeType.QUICKTIME,
            libression.config.THUMBNAIL_FRAME_COUNT,
        ),
        (
            "minimal_15_frames.avi",
            libression.entities.media.SupportedMimeType.X_MS_VIDEO,
            libression.config.THUMBNAIL_FRAME_COUNT,
        ),
    ],
    indirect=["media_fixture_by_filename"],
)
def test_av_generate_video_thumbnail(
    media_fixture_by_filename, mime_type, expected_frame_count
):
    # Prepare a byte stream for the test
    byte_stream = io.BytesIO(media_fixture_by_filename)

    # Generate the thumbnail
    thumbnail = libression.thumbnail.image.generate(byte_stream, 3, mime_type)

    # Basic checks
    assert thumbnail is not None, "Thumbnail should not be None"
    assert isinstance(thumbnail, bytes), "Thumbnail should be bytes"
    assert len(thumbnail) > 0, "Thumbnail should not be empty"

    # Check if it's a valid GIF
    gif_stream = io.BytesIO(thumbnail)
    gif = PIL.Image.open(gif_stream)
    assert gif.format == "GIF", "Thumbnail should be a GIF"

    # Check dimensions
    assert gif.width == 3, f"Expected width 3, got {gif.width}"
    assert gif.is_animated, "GIF should be animated"
    assert (
        gif.n_frames == expected_frame_count
    ), f"Expected {expected_frame_count} frames, got {gif.n_frames}"
