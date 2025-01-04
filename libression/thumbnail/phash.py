import io
import logging

import numpy
import PIL

logger = logging.getLogger(__name__)


def _hash_single_image(image: PIL.Image.Image, pixels: int) -> str:
    """
    Generate perceptual hash from a single image (averaged over 4 rotations)

    Args:
        image: PIL Image to hash
        pixels: Size of square grid (e.g., 4 means 4x4 grid, resulting in 16-bit hash)

    Returns:
        Hex string hash (length depends on pixels parameter)
    """
    # Convert to numpy array
    img_array = numpy.array(image.convert("L"))

    # Get all 4 rotations
    rot90 = numpy.rot90(img_array)
    rot180 = numpy.rot90(rot90)
    rot270 = numpy.rot90(rot180)
    arrays = [img_array, rot90, rot180, rot270]

    # Calculate bit length for the grid
    bit_length = pixels * pixels
    hex_length = (bit_length + 3) // 4  # Round up to nearest hex char

    # Resize and hash all rotations
    hashes = []
    for arr in arrays:
        h, w = arr.shape
        y_coords = numpy.linspace(0, h - 1, pixels).astype(int)
        x_coords = numpy.linspace(0, w - 1, pixels).astype(int)
        resized = arr[y_coords][:, x_coords]
        bits = (resized > resized.mean()).flatten()
        hash_int = bits.dot(2 ** numpy.arange(bit_length)[::-1])
        hashes.append(hash_int)

    # Use minimum hash value (canonical rotation)
    hash_int = min(hashes)
    return hex(hash_int)[2:].zfill(hex_length)


def phash_from_thumbnail(thumbnail_bytes: bytes, pixels: int = 4) -> str:
    """
    Generate rotation-invariant perceptual hash from thumbnail.

    Args:
        thumbnail_bytes: Image bytes to hash
        pixels: Size of square grid (default 4 for 4x4 grid)

    Returns:
        Hash string, with multiple hashes separated by commas for GIFs
    """
    img: PIL.Image.Image | None = None

    try:
        img = PIL.Image.open(io.BytesIO(thumbnail_bytes))

        if img.format == "GIF" and getattr(img, "is_animated", True):
            frames = list(PIL.ImageSequence.Iterator(img))
            if len(frames) == 1:
                return _hash_single_image(frames[0], pixels)
            elif len(frames) == 2:
                return (
                    _hash_single_image(frames[0], pixels)
                    + ","
                    + _hash_single_image(frames[1], pixels)
                )
            else:
                mid = len(frames) // 2
                return (
                    _hash_single_image(frames[0], pixels)
                    + ","
                    + _hash_single_image(frames[mid], pixels)
                    + ","
                    + _hash_single_image(frames[-1], pixels)
                )
        else:
            return _hash_single_image(img, pixels)

    except Exception as e:
        logger.error(f"Error generating hash: {e}")
        return ""
    finally:
        if img:
            img.close()


def compare_thumbnail_hashes(hash1: str, hash2: str) -> bool:
    """Compare thumbnail hashes (exact match only)."""
    if not hash1 or not hash2:
        return False

    frames1 = hash1.split(",")
    frames2 = hash2.split(",")

    # Different number of frames - compare first frames
    if len(frames1) != len(frames2):
        return frames1[0] == frames2[0]

    # Same number of frames - all must match
    return all(h1 == h2 for h1, h2 in zip(frames1, frames2))


def batch_compare_hashes(target_hash: str, hash_list: list[str]) -> list[bool]:
    """Efficiently compare one hash against many."""
    if not target_hash or not hash_list:
        return [False] * len(hash_list)

    return [compare_thumbnail_hashes(target_hash, h) for h in hash_list]
