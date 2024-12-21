import datetime
import dataclasses
import typing


@dataclasses.dataclass
class FileKeyMapping:
    source_key: str
    destination_key: str


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

    # Needs to be defined by IOHandler implementation
    def get(self, file_keys: typing.Iterable[str]) -> FileStreams:
        """Get multiple objects as streams."""
        ...


    def upload(self, file_streams: FileStreams) -> None:
        """
        Upload multiple streams.
        filepaths and streams must be same length
        """
        ...

    def delete(self, file_keys: typing.Iterable[str]) -> None:
        """Delete multiple objects."""
        ...

    def list_objects(self, dirpath: str, subfolder_contents: bool = False) -> list[ListDirectoryObject]:
        """List all objects in the "directory"."""
        ...

    def get_readonly_urls(self, file_keys: typing.Iterable[str]) -> GetUrlsResponse:
        ...

    # Optional overrides if needed
    def copy(
        self,
        file_key_mappings: typing.Iterable[FileKeyMapping],
    ) -> None:
        """
        default implementation does pass data between client/server
        best to override if possible to just have the server do the entire copy
        """
        for file_key_mapping in file_key_mappings:
            streams = self.get([file_key_mapping.source_key])
            self.upload(
                FileStreams(
                    file_streams={
                        file_key_mapping.destination_key: streams.file_streams[file_key_mapping.source_key]
                    }
                )
            )

    def move(
        self,
        file_key_mappings: typing.Iterable[FileKeyMapping],
    ) -> None:
        """
        default implementation does pass data between client/server
        best to override if possible to just have the server do the entire move
        """
        self.copy(file_key_mappings)
        self.delete([file_key_mapping.source_key for file_key_mapping in file_key_mappings])
