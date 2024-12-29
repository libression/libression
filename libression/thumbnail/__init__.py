import hashlib
import typing

import libression.entities.io

from .image import generate
from .phash import phash_from_thumbnail


class ThumbnailComponents(typing.NamedTuple):
    thumbnail: bytes | None
    phash: str | None
    checksum: str | None


def generate_thumbnail_components(
    file_streams: libression.entities.io.FileStreams,
    width_in_pixels: int,
) -> dict[str, ThumbnailComponents]:
    results = {}

    for file_key, file_stream in file_streams.file_streams.items():
        # Save initial position
        initial_pos = file_stream.file_stream.tell()
        mime_type = file_stream.mime_type
        if mime_type is None:
            raise ValueError(f"No mime type for file {file_key}")

        try:
            thumbnail = generate(
                file_stream.file_stream,
                width_in_pixels=width_in_pixels,
                mime_type=mime_type,
            )
            phash: str | None = None
            if thumbnail is not None:
                phash = phash_from_thumbnail(thumbnail)

            # Only calculate checksum if we have a valid thumbnail
            checksum: str | None = None
            if thumbnail and thumbnail != b"":
                checksum = hashlib.sha256(thumbnail).hexdigest()

            results[file_key] = ThumbnailComponents(thumbnail, phash, checksum)
        finally:
            # Always restore stream position, even if an error occurs
            file_stream.file_stream.seek(initial_pos)

    return results
