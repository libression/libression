import hashlib
import typing

import libression.entities.io

from .image import generate
from .phash import phash_from_thumbnail


class ThumbnailInfo(typing.NamedTuple):
    thumbnail: bytes | None
    phash: str | None
    checksum: str | None


def generate_thumbnail_info(
    original_file_stream: libression.entities.io.FileStream,
    thumbnail_mime_type: libression.entities.media.SupportedMimeType,
    width_in_pixels: int,
) -> ThumbnailInfo:
    # Save initial position
    initial_pos = original_file_stream.file_stream.tell()

    try:
        thumbnail = generate(
            original_file_stream.file_stream,
            width_in_pixels=width_in_pixels,
            mime_type=thumbnail_mime_type,
        )
        phash: str | None = None
        if thumbnail is not None:
            phash = phash_from_thumbnail(thumbnail)

        # Only calculate checksum if we have a valid thumbnail
        checksum: str | None = None
        if thumbnail and thumbnail != b"":
            checksum = hashlib.sha256(thumbnail).hexdigest()

        return ThumbnailInfo(
            thumbnail=thumbnail,
            phash=phash,
            checksum=checksum,
        )

    finally:
        # Always restore stream position, even if an error occurs
        original_file_stream.file_stream.seek(initial_pos)
