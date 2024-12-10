import datetime
import dataclasses
import typing


@dataclasses.dataclass
class FileStreams:
    file_streams: dict[str, typing.IO[bytes]]


@dataclasses.dataclass
class GetUrlsResponse:
    urls: dict[str, str]


@dataclasses.dataclass
class ListDirectoryObject:
    """File information from WebDAV"""
    filename: str
    absolute_path: str
    size: int
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

    # Needs to be defined by IOHandler implementation
    def get(self, file_keys: typing.Sequence[str]) -> FileStreams:
        """Get multiple objects as streams."""
        ...


    def upload(self, file_streams: FileStreams) -> None:
        """
        Upload multiple streams.
        filepaths and streams must be same length
        """
        ...

    def delete(self, file_keys: typing.Sequence[str]) -> None:
        """Delete multiple objects."""
        ...

    def list_objects(self, dirpath: str) -> list[ListDirectoryObject]:
        """List all objects in the "directory"."""
        ...

    def get_urls(self, file_keys: typing.Sequence[str]) -> GetUrlsResponse:
        ...

    # Optional overrides if needed
    def copy(
        self,
        source_file_keys: typing.Sequence[str],
        destination_file_keys: typing.Sequence[str],
    ) -> None:
        """
        default implementation does pass data between client/server
        best to override if possible to just have the server do the entire copy
        """
        streams = self.get(source_file_keys)
        self.upload(destination_file_keys, streams)

    def move(
        self,
        source_file_keys: typing.Sequence[str],
        destination_file_keys: typing.Sequence[str],
    ) -> None:
        """
        default implementation does pass data between client/server
        best to override if possible to just have the server do the entire move
        """
        self.copy(source_file_keys, destination_file_keys)
        self.delete(source_file_keys)
