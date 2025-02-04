import hashlib
import io

import PIL.Image
import pytest
from unittest.mock import patch, Mock

import libression.entities.io
import libression.thumbnail


@pytest.fixture
def sample_image() -> bytes:
    """Create a sample image in memory with 4:3 aspect ratio"""
    width, height = 800, 600  # 4:3 aspect ratio
    img = PIL.Image.new("RGB", (width, height), color="red")
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format="JPEG", quality=95)
    return img_byte_arr.getvalue()


@pytest.fixture
def mock_presigned_url() -> str:
    return "https://example.com/test.jpg"


@pytest.fixture
def mock_http_response(sample_image):
    """Mock httpx.get response"""
    mock_response = Mock()
    mock_response.content = sample_image
    mock_response.raise_for_status = Mock()
    return mock_response


@pytest.fixture
def large_image() -> bytes:
    """Create a large test image with 4:3 aspect ratio"""
    width, height = 2000, 1500  # Same 4:3 aspect ratio
    img = PIL.Image.new("RGB", (width, height), color="blue")
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format="JPEG", quality=95)
    return img_byte_arr.getvalue()


def test_generate_thumbnail_info_success(mock_presigned_url, mock_http_response):
    """Test successful generation of thumbnail components"""
    width = 100

    with patch("httpx.get", return_value=mock_http_response):
        result = libression.thumbnail.generate_thumbnail_info(
            mock_presigned_url,
            libression.entities.media.SupportedMimeType.JPEG,
            width,
        )

    assert isinstance(result, libression.thumbnail.ThumbnailInfo)
    assert isinstance(result.thumbnail, bytes)
    assert isinstance(result.phash, str)
    assert isinstance(result.checksum, str)

    # Verify checksum matches the thumbnail
    expected_checksum = hashlib.sha256(result.thumbnail).hexdigest()
    assert result.checksum == expected_checksum


def test_generate_thumbnail_info_invalid_image(mock_presigned_url):
    """Test handling of invalid image data"""
    width = 100

    # Mock response with invalid image data
    mock_response = Mock()
    mock_response.content = b"not an image"
    mock_response.raise_for_status = Mock()

    with patch("httpx.get", return_value=mock_response):
        result = libression.thumbnail.generate_thumbnail_info(
            mock_presigned_url,
            libression.entities.media.SupportedMimeType.JPEG,
            width,
        )

    assert isinstance(result, libression.thumbnail.ThumbnailInfo)
    assert result.thumbnail is None, "Invalid image should result in None"
    assert result.phash is None
    assert result.checksum is None


def test_generate_thumbnail_info_respects_width(mock_presigned_url, large_image):
    """Test that thumbnails are generated with correct width"""
    width = 100

    mock_response = Mock()
    mock_response.content = large_image
    mock_response.raise_for_status = Mock()

    with patch("httpx.get", return_value=mock_response):
        result = libression.thumbnail.generate_thumbnail_info(
            mock_presigned_url,
            libression.entities.media.SupportedMimeType.JPEG,
            width,
        )

    # Load thumbnail into PIL to check dimensions
    img = PIL.Image.open(io.BytesIO(result.thumbnail))
    assert img.width == width
