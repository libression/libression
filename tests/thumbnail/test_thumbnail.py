import hashlib
import io

import PIL.Image
import pytest

import libression.entities.io
import libression.thumbnail


@pytest.fixture
def sample_image_stream() -> libression.entities.io.FileStreams:
    # Create a simple test image in memory
    img = PIL.Image.new("RGB", (100, 100), color="red")
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format="JPEG")
    img_byte_arr.seek(0)

    file_stream = libression.entities.io.FileStream(
        file_stream=img_byte_arr,
        mime_type=libression.entities.media.OpenCvProccessableImageMimeType.JPEG,
        file_byte_size=len(img_byte_arr.getvalue()),
    )

    return libression.entities.io.FileStreams(
        file_streams={
            "sample_image.jpg": file_stream,
        }
    )


@pytest.fixture
def sample_image() -> bytes:
    """Create a sample image in memory with 4:3 aspect ratio"""
    width, height = 800, 600  # 4:3 aspect ratio
    img = PIL.Image.new("RGB", (width, height), color="red")
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format="JPEG", quality=95)
    return img_byte_arr.getvalue()


@pytest.fixture
def sample_file_stream(sample_image) -> libression.entities.io.FileStream:
    """Create FileStreams with multiple test images"""
    return libression.entities.io.FileStream(
        file_stream=io.BytesIO(sample_image),
        file_byte_size=len(sample_image),
        mime_type=libression.entities.media.OpenCvProccessableImageMimeType.JPEG,
    )


def test_generate_thumbnail_info_success(sample_file_stream):
    """Test successful generation of thumbnail components"""
    width = 100
    result = libression.thumbnail.generate_thumbnail_info(
        sample_file_stream,
        libression.entities.media.OpenCvProccessableImageMimeType.JPEG,
        width,
    )

    assert isinstance(result, libression.thumbnail.ThumbnailInfo)
    assert isinstance(result.thumbnail, bytes)
    assert isinstance(result.phash, str)
    assert isinstance(result.checksum, str)

    # Verify checksum matches the thumbnail
    expected_checksum = hashlib.sha256(result.thumbnail).hexdigest()
    assert result.checksum == expected_checksum


@pytest.fixture
def invalid_image_stream() -> libression.entities.io.FileStream:
    """Create FileStreams with invalid image data"""
    invalid_data = b"not an image"
    return libression.entities.io.FileStream(
        file_stream=io.BytesIO(invalid_data),
        file_byte_size=len(invalid_data),
        mime_type=libression.entities.media.OpenCvProccessableImageMimeType.JPEG,
    )


def test_generate_thumbnail_info_invalid_image(invalid_image_stream):
    """Test handling of invalid image data"""
    width = 100
    result = libression.thumbnail.generate_thumbnail_info(
        invalid_image_stream,
        libression.entities.media.OpenCvProccessableImageMimeType.JPEG,
        width,
    )

    assert isinstance(result, libression.thumbnail.ThumbnailInfo)
    assert result.thumbnail == b"", "Invalid image should result in empty bytes"
    assert isinstance(result.phash, str)
    assert result.checksum is None, "Invalid image should have no checksum"


@pytest.fixture
def large_image() -> bytes:
    """Create a large test image with 4:3 aspect ratio"""
    width, height = 2000, 1500  # Same 4:3 aspect ratio
    img = PIL.Image.new("RGB", (width, height), color="blue")
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format="JPEG", quality=95)
    return img_byte_arr.getvalue()


def test_generate_thumbnail_info_respects_width(sample_image, large_image):
    """Test that thumbnails are generated with correct width"""
    width = 100
    stream = libression.entities.io.FileStream(
        file_stream=io.BytesIO(large_image),
        file_byte_size=len(large_image),
        mime_type=libression.entities.media.OpenCvProccessableImageMimeType.JPEG,
    )

    result = libression.thumbnail.generate_thumbnail_info(
        stream,
        libression.entities.media.OpenCvProccessableImageMimeType.JPEG,
        width,
    )

    # Load thumbnail into PIL to check dimensions
    img = PIL.Image.open(io.BytesIO(result.thumbnail))
    assert img.width == width
    # Calculate expected height maintaining aspect ratio
    expected_height = int(width * 3 / 4)  # For 4:3 aspect ratio
    assert img.height == expected_height, "Height should maintain 4:3 aspect ratio"
    assert (
        abs(img.width / img.height - 4 / 3) < 0.01
    ), "Aspect ratio should be preserved"


def test_generate_thumbnail_info_stream_position(sample_file_stream):
    """Test that stream positions are preserved"""
    width = 100

    # Record initial positions
    initial_position = sample_file_stream.file_stream.tell()

    # Ensure all streams start at position 0
    assert (
        sample_file_stream.file_stream.tell() == 0
    ), "Stream should start at position 0"

    libression.thumbnail.generate_thumbnail_info(
        sample_file_stream,
        libression.entities.media.OpenCvProccessableImageMimeType.JPEG,
        width,
    )

    # Verify positions are restored
    assert (
        sample_file_stream.file_stream.tell() == initial_position
    ), "Stream position changed from initial position"

    # Verify stream is still readable
    sample_file_stream.file_stream.seek(0)
    data = sample_file_stream.file_stream.read()
    assert len(data) > 0, "Stream should still be readable"
