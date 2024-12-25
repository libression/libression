import typing
import hashlib
from .image import generate
from .phash import phash_from_thumbnail
import libression.entities.io


class ThumbnailComponents(typing.NamedTuple):
    thumbnail: bytes
    phash: str
    checksum: str | None


def generate_thumbnail_components(
    file_stream: libression.entities.io.FileStreams,
    width_in_pixels: int,
) -> dict[str, ThumbnailComponents]:

    results = {}

    for file_key, file_stream in file_stream.file_streams.items():
        # Save initial position
        initial_pos = file_stream.file_stream.tell()
        try:
            thumbnail = generate(
                file_stream.file_stream,
                width_in_pixels=width_in_pixels,
                mime_type=file_stream.mime_type,
            )
            phash = phash_from_thumbnail(thumbnail)
            
            # Only calculate checksum if we have a valid thumbnail
            checksum = None
            if thumbnail and thumbnail != b"":
                checksum = hashlib.sha256(thumbnail).hexdigest()
                
            results[file_key] = ThumbnailComponents(thumbnail, phash, checksum)
        finally:
            # Always restore stream position, even if an error occurs
            file_stream.file_stream.seek(initial_pos)

    return results
