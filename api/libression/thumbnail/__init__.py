import hashlib
import typing

import libression.entities.io

from .image import generate_from_presigned_url
from .phash import phash_from_thumbnail


class ThumbnailInfo(typing.NamedTuple):
    thumbnail: bytes | None
    phash: str | None
    checksum: str | None
    raw_file_found: bool = True


def generate_thumbnail_info(
    presigned_url: str,
    original_mime_type: libression.entities.media.SupportedMimeType,
    width_in_pixels: int,
) -> ThumbnailInfo:
    thumbnail = generate_from_presigned_url(
        presigned_url,
        width_in_pixels=width_in_pixels,
        original_mime_type=original_mime_type,
    )

    if not thumbnail:
        return ThumbnailInfo(
            thumbnail=None,  # don't store empty thumbnails
            phash=None,
            checksum=None,
        )

    # Skip phash and checksum for videos
    if original_mime_type.value.startswith("video/"):
        return ThumbnailInfo(
            thumbnail=thumbnail,
            phash=None,
            checksum=None,
        )

    # For non-videos, generate phash and checksum
    phash = phash_from_thumbnail(thumbnail)
    checksum = hashlib.sha256(thumbnail).hexdigest()

    return ThumbnailInfo(
        thumbnail=thumbnail,
        phash=phash,
        checksum=checksum,
    )
