import io
import logging

from botocore.response import StreamingBody
from wand.image import Image

logger = logging.getLogger(__name__)


def _get_reduced_image_size(original_image: Image, width: int = 128) -> tuple[int, int]:

    width_percent = (width / float(original_image.width))
    height = int((float(original_image.height) * float(width_percent)))
    return width, height


def generate(
        original_contents: StreamingBody,
        key: str,
        width: int = 200,
) -> bytes:

    try:
        with Image(file=original_contents) as img:
            converted = img.convert("jpg")
            scaled_width, scaled_height = _get_reduced_image_size(converted, width=width)
            converted.resize(width=scaled_width, height=scaled_height)

            buf = io.BytesIO()
            converted.save(file=buf)
            byte_im = buf.getvalue()
            return byte_im

    except Exception as e:
        logger.info(f"Exception: {e}, can't read key {key}...")
