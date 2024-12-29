import io

import PIL.Image
import PIL.ImageDraw
import PIL.ImageEnhance
import PIL.ImageSequence
import pytest

import libression.thumbnail.phash


@pytest.mark.parametrize("minimal_image", ["jpg", "png"], indirect=True)
def test_phash_static_image_generation(minimal_image):
    """Test hash generation for static images."""
    hash_value = libression.thumbnail.phash.phash_from_thumbnail(minimal_image)
    assert isinstance(hash_value, str)
    assert len(hash_value) == 4  # 4x4 grid = 16 bits = 4 hex chars
    assert all(c in "0123456789abcdef" for c in hash_value)


@pytest.mark.parametrize("minimal_image", ["gif"], indirect=True)
def test_phash_animated_image_generation(minimal_image):
    """Test hash generation for animated images."""
    hash_value = libression.thumbnail.phash.phash_from_thumbnail(minimal_image)
    assert isinstance(hash_value, str)

    frames = hash_value.split(",")
    assert len(frames) >= 1
    assert all(len(f) == 4 for f in frames)
    assert all(all(c in "0123456789abcdef" for c in f) for f in frames)


@pytest.mark.parametrize(
    "pixels,expected_length",
    [
        (3, 3),  # 9 bits -> 3 hex chars
        (4, 4),  # 16 bits -> 4 hex chars
        (5, 7),  # 25 bits -> 7 hex chars
    ],
)
@pytest.mark.parametrize("minimal_image", ["jpg"], indirect=True)
def test_phash_different_sizes(minimal_image, pixels, expected_length):
    """Test different grid sizes."""
    hash_value = libression.thumbnail.phash.phash_from_thumbnail(
        minimal_image, pixels=pixels
    )
    assert len(hash_value) == expected_length


@pytest.mark.parametrize("minimal_image", ["jpg"], indirect=True)
def test_phash_rotation_invariance(minimal_image):
    """Test that rotated images produce the same hash."""
    img = PIL.Image.open(io.BytesIO(minimal_image))

    # Get hash of original
    original_hash = libression.thumbnail.phash.phash_from_thumbnail(minimal_image)

    # Test 90, 180, 270 degree rotations
    for angle in [90, 180, 270]:
        rotated = img.rotate(angle)
        rotated_bytes = io.BytesIO()
        rotated.save(rotated_bytes, format="JPEG")
        rotated_hash = libression.thumbnail.phash.phash_from_thumbnail(
            rotated_bytes.getvalue()
        )

        assert original_hash == rotated_hash, f"Hash should match for {angle}° rotation"


def test_phash_comparison():
    """Test hash comparison functions."""
    # Create test images
    img1 = PIL.Image.new("L", (100, 100), color=128)
    draw1 = PIL.ImageDraw.Draw(img1)
    draw1.rectangle([20, 20, 80, 80], fill=200)

    img2 = img1.copy()  # Same as img1
    img3 = PIL.Image.new("L", (100, 100), color=128)  # Different
    draw3 = PIL.ImageDraw.Draw(img3)
    draw3.rectangle([40, 40, 60, 60], fill=200)

    def to_bytes(img):
        buffer = io.BytesIO()
        img.save(buffer, format="PNG")
        return buffer.getvalue()

    hash1 = libression.thumbnail.phash.phash_from_thumbnail(to_bytes(img1))
    hash2 = libression.thumbnail.phash.phash_from_thumbnail(to_bytes(img2))
    hash3 = libression.thumbnail.phash.phash_from_thumbnail(to_bytes(img3))

    assert libression.thumbnail.phash.compare_thumbnail_hashes(hash1, hash2)
    assert not libression.thumbnail.phash.compare_thumbnail_hashes(hash1, hash3)

    results = libression.thumbnail.phash.batch_compare_hashes(hash1, [hash2, hash3])
    assert results == [True, False]


def test_phash_error_handling():
    """Test error handling for invalid inputs."""
    # Invalid image data
    assert libression.thumbnail.phash.phash_from_thumbnail(b"invalid data") == ""

    # Empty input
    assert libression.thumbnail.phash.phash_from_thumbnail(b"") == ""

    # None input
    assert (
        libression.thumbnail.phash.phash_from_thumbnail(None) == ""
    )  # Changed from raises to return empty

    # Hash comparison with invalid inputs
    assert not libression.thumbnail.phash.compare_thumbnail_hashes("", "abc")
    assert not libression.thumbnail.phash.compare_thumbnail_hashes("abc", "")
    assert not libression.thumbnail.phash.compare_thumbnail_hashes("", "")
    assert not libression.thumbnail.phash.compare_thumbnail_hashes(
        None, "abc"
    )  # Add None test
    assert not libression.thumbnail.phash.compare_thumbnail_hashes(
        "abc", None
    )  # Add None test


def test_phash_pixel_value_invariance():
    """Test that relative pixel values matter more than absolute values."""
    # Create two images with different brightness but same relative values
    img1 = PIL.Image.new("L", (100, 100))
    draw1 = PIL.ImageDraw.Draw(img1)
    draw1.rectangle([20, 20, 80, 80], fill=128)  # Mid-gray square

    img2 = PIL.Image.new("L", (100, 100))
    draw2 = PIL.ImageDraw.Draw(img2)
    draw2.rectangle([20, 20, 80, 80], fill=192)  # Lighter gray square

    def to_bytes(img):
        buffer = io.BytesIO()
        img.save(buffer, format="PNG")
        return buffer.getvalue()

    hash1 = libression.thumbnail.phash.phash_from_thumbnail(to_bytes(img1))
    hash2 = libression.thumbnail.phash.phash_from_thumbnail(to_bytes(img2))

    assert hash1 == hash2  # Should match as relative brightness is same
