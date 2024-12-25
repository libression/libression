import io
import pytest
import PIL.Image
import hashlib

import libression.entities.io
import libression.thumbnail

@pytest.fixture
def sample_image_stream() -> libression.entities.io.FileStreams:
    # Create a simple test image in memory
    img = PIL.Image.new('RGB', (100, 100), color='red')
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format='JPEG')
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
    img = PIL.Image.new('RGB', (width, height), color='red')
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format='JPEG', quality=95)
    return img_byte_arr.getvalue()

@pytest.fixture
def sample_file_streams(sample_image) -> libression.entities.io.FileStreams:
    """Create FileStreams with multiple test images"""
    streams = {
        "test1.jpg": libression.entities.io.FileStream(
            file_stream=io.BytesIO(sample_image),
            file_byte_size=len(sample_image),
            mime_type=libression.entities.media.OpenCvProccessableImageMimeType.JPEG
        ),
        "test2.jpg": libression.entities.io.FileStream(
            file_stream=io.BytesIO(sample_image),
            file_byte_size=len(sample_image),
            mime_type=libression.entities.media.OpenCvProccessableImageMimeType.JPEG
        )
    }
    return libression.entities.io.FileStreams(file_streams=streams)

def test_generate_thumbnail_components_success(sample_file_streams):
    """Test successful generation of thumbnail components"""
    width = 100
    results = libression.thumbnail.generate_thumbnail_components(sample_file_streams, width)
    
    assert len(results) == 2
    assert "test1.jpg" in results
    assert "test2.jpg" in results
    
    for file_key, components in results.items():
        assert isinstance(components, libression.thumbnail.ThumbnailComponents)
        assert isinstance(components.thumbnail, bytes)
        assert isinstance(components.phash, str)
        assert isinstance(components.checksum, str)
        
        # Verify checksum matches the thumbnail
        expected_checksum = hashlib.sha256(components.thumbnail).hexdigest()
        assert components.checksum == expected_checksum

def test_generate_thumbnail_components_consistent_phash(sample_file_streams):
    """Test that phash is consistent for identical images"""
    width = 100
    results = libression.thumbnail.generate_thumbnail_components(sample_file_streams, width)
    
    # Same image should produce same phash
    assert results["test1.jpg"].phash == results["test2.jpg"].phash

@pytest.fixture
def invalid_image_streams() -> libression.entities.io.FileStreams:
    """Create FileStreams with invalid image data"""
    invalid_data = b"not an image"
    streams = {
        "invalid.jpg": libression.entities.io.FileStream(
            file_stream=io.BytesIO(invalid_data),
            file_byte_size=len(invalid_data),
            mime_type=libression.entities.media.OpenCvProccessableImageMimeType.JPEG
        )
    }
    return libression.entities.io.FileStreams(file_streams=streams)

def test_generate_thumbnail_components_invalid_image(invalid_image_streams):
    """Test handling of invalid image data"""
    width = 100
    results = libression.thumbnail.generate_thumbnail_components(invalid_image_streams, width)
    
    components = results["invalid.jpg"]
    assert isinstance(components, libression.thumbnail.ThumbnailComponents)
    assert components.thumbnail == b"", "Invalid image should result in empty bytes"
    assert isinstance(components.phash, str)
    assert components.checksum is None, "Invalid image should have no checksum"

@pytest.fixture
def large_image() -> bytes:
    """Create a large test image with 4:3 aspect ratio"""
    width, height = 2000, 1500  # Same 4:3 aspect ratio
    img = PIL.Image.new('RGB', (width, height), color='blue')
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format='JPEG', quality=95)
    return img_byte_arr.getvalue()

def test_generate_thumbnail_components_respects_width(sample_image, large_image):
    """Test that thumbnails are generated with correct width"""
    width = 100
    streams = libression.entities.io.FileStreams(file_streams={
        "small.jpg": libression.entities.io.FileStream(
            file_stream=io.BytesIO(sample_image),
            file_byte_size=len(sample_image),
            mime_type=libression.entities.media.OpenCvProccessableImageMimeType.JPEG
        ),
        "large.jpg": libression.entities.io.FileStream(
            file_stream=io.BytesIO(large_image),
            file_byte_size=len(large_image),
            mime_type=libression.entities.media.OpenCvProccessableImageMimeType.JPEG
        )
    })
    
    results = libression.thumbnail.generate_thumbnail_components(streams, width)
    
    for components in results.values():
        # Load thumbnail into PIL to check dimensions
        img = PIL.Image.open(io.BytesIO(components.thumbnail))
        assert img.width == width
        # Calculate expected height maintaining aspect ratio
        expected_height = int(width * 3/4)  # For 4:3 aspect ratio
        assert img.height == expected_height, "Height should maintain 4:3 aspect ratio"
        assert abs(img.width/img.height - 4/3) < 0.01, "Aspect ratio should be preserved"

def test_generate_thumbnail_components_stream_position(sample_file_streams):
    """Test that stream positions are preserved"""
    width = 100
    
    # Record initial positions
    initial_positions = {
        key: stream.file_stream.tell()
        for key, stream in sample_file_streams.file_streams.items()
    }
    
    # Ensure all streams start at position 0
    for stream in sample_file_streams.file_streams.values():
        assert stream.file_stream.tell() == 0, "Stream should start at position 0"
    
    results = libression.thumbnail.generate_thumbnail_components(sample_file_streams, width)
    
    # Verify positions are restored
    for key, stream in sample_file_streams.file_streams.items():
        current_pos = stream.file_stream.tell()
        assert current_pos == initial_positions[key], \
            f"Stream position for {key} changed from {initial_positions[key]} to {current_pos}"
        
        # Verify stream is still readable
        stream.file_stream.seek(0)
        data = stream.file_stream.read()
        assert len(data) > 0, f"Stream {key} should still be readable"

def test_generate_thumbnail_components_empty_streams():
    """Test handling of empty FileStreams"""
    width = 100
    empty_streams = libression.entities.io.FileStreams(file_streams={})
    results = libression.thumbnail.generate_thumbnail_components(empty_streams, width)
    assert len(results) == 0
