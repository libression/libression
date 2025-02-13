import io
import tempfile

import cv2
import numpy
import pytest
import ffmpeg

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
    "media_fixture_filename,mime_type,expected_fps,expected_duration,expected_frame_count",
    [
        ("minimal.gif", libression.entities.media.SupportedMimeType.GIF, 2, 1, 2),
        ("minimal.mp4", libression.entities.media.SupportedMimeType.MP4, 2, 1, 2),
        ("minimal.mpeg", libression.entities.media.SupportedMimeType.MPEG, 2, 1, 2),
        ("minimal.mov", libression.entities.media.SupportedMimeType.QUICKTIME, 2, 1, 2),
        ("minimal.webm", libression.entities.media.SupportedMimeType.WEBM, 2, 1, 2),
        (
            "minimal.avi",
            libression.entities.media.SupportedMimeType.X_MS_VIDEO,
            2,
            1,
            2,
        ),
        (
            "IMG_5059.mov",
            libression.entities.media.SupportedMimeType.QUICKTIME,
            2,
            2.5,
            5,
        ),
        (
            "minimal_15_frames.gif",
            libression.entities.media.SupportedMimeType.GIF,
            2,
            5,
            libression.config.THUMBNAIL_FRAME_COUNT,
        ),
        (
            "minimal_15_frames.mp4",
            libression.entities.media.SupportedMimeType.MP4,
            2,
            5,
            libression.config.THUMBNAIL_FRAME_COUNT,
        ),
        (
            "minimal_15_frames.mpeg",
            libression.entities.media.SupportedMimeType.MPEG,
            2,
            5,
            libression.config.THUMBNAIL_FRAME_COUNT,
        ),
        (
            "minimal_15_frames.mov",
            libression.entities.media.SupportedMimeType.QUICKTIME,
            2,
            5,
            libression.config.THUMBNAIL_FRAME_COUNT,
        ),
        (
            "minimal_15_frames.avi",
            libression.entities.media.SupportedMimeType.X_MS_VIDEO,
            2,
            5,
            libression.config.THUMBNAIL_FRAME_COUNT,
        ),
    ],
)
def test_generate_video_thumbnail(
    media_fixture_filename,
    mime_type,
    expected_fps,
    expected_duration,
    expected_frame_count,
    mock_http_file_server,
):
    # Generate the thumbnail
    thumbnail = libression.thumbnail.image.generate_from_presigned_url(
        mock_http_file_server.get_url(media_fixture_filename),
        mime_type,
        libression.config.THUMBNAIL_WIDTH_IN_PIXELS,
    )

    # Basic checks
    assert thumbnail
    assert isinstance(thumbnail, bytes), "Thumbnail should be bytes"

    # Write to temp file for ffmpeg to read
    with tempfile.NamedTemporaryFile(suffix=".mp4") as temp_file:
        temp_file.write(thumbnail)
        temp_file.flush()

        # Use ffmpeg to probe the file
        probe = ffmpeg.probe(temp_file.name)
        video_info = next(s for s in probe["streams"] if s["codec_type"] == "video")

        # Check dimensions
        assert int(video_info["width"]) == libression.config.THUMBNAIL_WIDTH_IN_PIXELS

        # Check FPS
        fps_parts = video_info["r_frame_rate"].split("/")
        actual_fps = int(fps_parts[0]) / int(fps_parts[1])
        assert (
            abs(actual_fps - expected_fps) < 0.1
        ), f"Expected {expected_fps} FPS, got {actual_fps}"

        # Check duration
        duration = float(probe["format"]["duration"])
        assert (
            abs(duration - expected_duration) < 0.1
        ), f"Expected {expected_duration}s duration, got {duration}s"

        # Check frame count
        if "nb_frames" in video_info:
            frame_count = int(video_info["nb_frames"])
            assert (
                frame_count == expected_frame_count
            ), f"Expected {expected_frame_count} frames, got {frame_count}"
