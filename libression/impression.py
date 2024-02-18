from enum import Enum
from typing import Optional
import io
import pyheif
import logging
import botocore

from PIL import Image, ImageOps

from libression import config, s3_old

logger = logging.getLogger(__name__)


class FileFormat(Enum):
    jpg = "jpg"
    jpeg = "jpeg"
    png = "png"
    tif = "tif"
    tiff = "tiff"
    heic = "heic"


def to_cache_preloaded(
    cache_key: str,
    raw_content: bytes,
    file_format: str,
    cache_bucket: str,
) -> Optional[bytes]:

    try:
        cached_content = _generate_cache(
            raw_content,
            file_format=FileFormat(file_format),
        )

        s3.put(
            key=cache_key,
            body=cached_content,
            bucket_name=cache_bucket,
        )
        logging.info(f"saved cache {cache_key}")
        return cached_content

    except Exception as e:
        logger.info(f"Exception: {e}, can't read key {cache_key}...")
        return None


def _generate_cache(
    original_contents: botocore.response.StreamingBody,
    file_format: FileFormat,
    width: int = config.CACHE_WIDTH,
) -> Optional[bytes]:

    image = None

    if file_format in [FileFormat.heic]:
        i = pyheif.read_heif(original_contents.read())
        image = Image.frombytes(mode=i.mode, size=i.size, data=i.data)
    else:
        image = Image.open(original_contents)
        if file_format in [FileFormat.jpeg, FileFormat.jpg]:
            image = ImageOps.exif_transpose(image)

    if image is None:
        return None
    else:
        return _shrink_image(image.convert('RGB'), fixed_width=width)


def _shrink_image(original_image: Image, fixed_width: int):

    width_percent = (fixed_width / float(original_image.size[1]))
    height = int((float(original_image.size[0]) * float(width_percent)))
    original_image.thumbnail((fixed_width, height))
    buf = io.BytesIO()
    original_image.save(buf, format='JPEG')
    byte_im = buf.getvalue()

    return byte_im
