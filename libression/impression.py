from enum import Enum
from typing import Optional
import io
import logging
import botocore.response
from pillow_heif import register_heif_opener
from PIL import Image, ImageOps

register_heif_opener()
from libression import config, s3

logger = logging.getLogger(__name__)


class FileExtension(Enum):
    JPG = "jpg"
    JPEG = "jpeg"
    PNG = "png"
    TIF = "tif"
    TIFF = "tiff"
    HEIC = "heic"
    # # DESC ORDER OF COUNT IN LIBRARY!
    # MOV = "mov"
    # MP4 = "mp4"
    # M4V = "m4v"
    # AAE = "aae"
    # BMP = "bmp"
    # AVI = "avi"
    # THREEGP = "3gp"
    # MPG = "mpg"
    # MTS = "mts"
    # THM = "thm"
    # DNG = "dng"
    # WEBP = "webp"
    # GIF = "gif"
    # WMV = "wmv"


def to_cache_preloaded(
    cache_key: str,
    raw_content: botocore.response.StreamingBody,
    file_format: str,
    cache_bucket: str,
) -> Optional[bytes]:

    cached_content = _generate_cache(
        raw_content,
        file_format=FileExtension(file_format),
    )

    s3.put(
        key=cache_key,
        body=cached_content,
        bucket_name=cache_bucket,
    )
    logging.info(f"saved cache {cache_key}")
    return cached_content


def _generate_cache(
    original_contents: botocore.response.StreamingBody,
    file_format: FileExtension,
    width: int = config.CACHE_WIDTH,
) -> Optional[bytes]:

    image = Image.open(original_contents)
    if file_format in [FileExtension.JPEG, FileExtension.JPG]:
        image = ImageOps.exif_transpose(image)

    return _shrink_image(image.convert('RGB'), fixed_width=width)


def _shrink_image(original_image: Image, fixed_width: int):

    width_percent = (fixed_width / float(original_image.size[1]))
    height = int((float(original_image.size[0]) * float(width_percent)))
    original_image.thumbnail((fixed_width, height))
    buf = io.BytesIO()
    original_image.save(buf, format='JPEG')
    byte_im = buf.getvalue()

    return byte_im
