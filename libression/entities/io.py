import datetime
import typing
import urllib.parse
import libression.config
import libression.entities.media
import libression.entities.base
import pydantic


class FileKeyMapping(pydantic.BaseModel):
    source_key: str = pydantic.Field(
        description="""
        Source file key, normalised to unquoted and without leading slash
        """
    )
    destination_key: str = pydantic.Field(
        description="""
        Destination file key, normalised to unquoted and without leading slash
        """
    )

    @pydantic.model_validator(mode="before")
    def normalise_keys(cls, v: typing.Any) -> typing.Any:
        if isinstance(v, str):
            return urllib.parse.unquote(v.lstrip("/"))
        return v

    @staticmethod
    def validate_mappings(mappings: typing.Sequence["FileKeyMapping"]) -> None:
        """
        Validates that there are no overlapping or duplicate keys in the mappings.

        Args:
            mappings: Sequence of FileKeyMapping objects to validate

        Raises:
            ValueError: If any validation fails
        """
        source_keys = [m.source_key for m in mappings]
        dest_keys = [m.destination_key for m in mappings]

        # Check for duplicate source keys
        if len(source_keys) != len(set(source_keys)):
            raise ValueError("Duplicate source keys found in mappings")

        # Check for duplicate destination keys
        if len(dest_keys) != len(set(dest_keys)):
            raise ValueError("Duplicate destination keys found in mappings")

        # Check for overlapping keys between source and destination
        if set(source_keys) & set(dest_keys):
            raise ValueError("Source and destination keys overlap")


class FileStreamInfo(typing.NamedTuple):
    file_stream: typing.BinaryIO
    mime_type: libression.entities.media.SupportedMimeType | None = None


class FileStreamInfos(typing.NamedTuple):
    file_streams: dict[str, FileStreamInfo]


class GetUrlsResponse(pydantic.BaseModel):
    """
    Response containing base URL and paths for each file key.
    Allows clients to reconstruct URLs using appropriate base URL for their context.
    """

    base_url: str = pydantic.Field(
        description="Base URL (e.g., 'https://webdav:443' or 'https://localhost:8443')"
    )
    paths: dict[str, str] = pydantic.Field(
        description="Mapping of file keys to their paths"
    )


class ListDirectoryObject(pydantic.BaseModel):
    """File information (eg from WebDAV)"""

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

    async def upload(
        self,
        file_streams: FileStreamInfos,
        chunk_byte_size: int = libression.config.DEFAULT_CHUNK_BYTE_SIZE,
    ) -> list[libression.entities.base.FileActionResponse]: ...

    def get_readonly_urls(
        self, file_keys: typing.Sequence[str], expires_in_seconds: int
    ) -> GetUrlsResponse: ...

    async def delete(
        self, file_keys: typing.Sequence[str]
    ) -> list[libression.entities.base.FileActionResponse]: ...

    async def list_objects(
        self, dirpath: str, subfolder_contents: bool = False
    ) -> list[ListDirectoryObject]:
        """List all objects in the "directory"."""
        ...

    async def copy(
        self,
        file_key_mappings: typing.Sequence[FileKeyMapping],
        delete_source: bool,  # False: copy, True: paste
        overwrite_existing: bool = True,
    ) -> list[libression.entities.base.FileActionResponse]: ...
