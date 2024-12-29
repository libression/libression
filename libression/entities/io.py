import dataclasses
import datetime
import typing

import libression.entities.media


@dataclasses.dataclass
class FileKeyMapping:
    source_key: str
    destination_key: str


@dataclasses.dataclass
class FileStream:
    file_stream: typing.BinaryIO
    file_byte_size: int
    mime_type: libression.entities.media.SupportedMimeType | None = None


@dataclasses.dataclass
class FileStreams:
    file_streams: dict[str, FileStream]


@dataclasses.dataclass
class GetUrlsResponse:
    urls: dict[str, str]


@dataclasses.dataclass
class ListDirectoryObject:
    """File information from WebDAV"""

    filename: str
    absolute_path: str
    size: int  # bytes
    modified: datetime.datetime
    is_dir: bool


@typing.runtime_checkable
class IOHandler(typing.Protocol):
    """
    all methods can be bulk operations
    use bytestream if possible to reduce memory usage
    filepaths validated by the IOHandler implementation (can be s3://, https://, etc.)
    best to have fully qualified filepaths
    """

    async def upload(self, file_streams: FileStreams, chunk_byte_size: int) -> None: ...

    def get_readonly_urls(
        self, file_keys: typing.Sequence[str], expires_in_seconds: int
    ) -> GetUrlsResponse: ...

    async def delete(self, file_keys: typing.Sequence[str]) -> None: ...

    async def list_objects(
        self, dirpath: str, subfolder_contents: bool = False
    ) -> list[ListDirectoryObject]:
        """List all objects in the "directory"."""
        ...

    async def copy(
        self,
        file_key_mappings: typing.Sequence[FileKeyMapping],
        delete_source: bool,  # False: copy, True: paste
        chunk_byte_size: int,
        allow_missing: bool = False,
    ) -> None: ...
