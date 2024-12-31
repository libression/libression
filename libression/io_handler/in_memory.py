import fs.memoryfs
import typing
import fs.path
from libression.entities.io import (
    FileStreams,
    GetUrlsResponse,
    ListDirectoryObject,
    FileKeyMapping,
)


class InMemoryIOHandler:
    """
    A memory-based implementation of IOHandler ... mainly for testing/development
    Best to use others (s3/webdav/etc.) for production
    """

    def __init__(self):
        self.fs = fs.memoryfs.MemoryFS()
        self.base_url = "memory://"

    async def upload(self, file_streams: FileStreams, chunk_byte_size: int) -> None:
        for file_key, stream in file_streams.file_streams.items():
            # Ensure directory exists
            dir_path = fs.path.dirname(file_key)
            if dir_path:
                self.fs.makedirs(dir_path, recreate=True)

            # Write file
            with self.fs.open(file_key, "wb") as f:
                f.write(stream.file_stream.read())

    def get_readonly_urls(
        self, file_keys: typing.Sequence[str], expires_in_seconds: int
    ) -> GetUrlsResponse:
        urls = {
            key: f"memory://{key}?expires={expires_in_seconds}" for key in file_keys
        }
        return GetUrlsResponse(urls=urls)

    async def delete(
        self, file_keys: typing.Sequence[str], raise_on_error: bool = True
    ) -> None:
        for key in file_keys:
            try:
                self.fs.remove(key)
            except fs.errors.ResourceNotFound:
                if raise_on_error:
                    raise

    async def list_objects(
        self, dirpath: str = "", subfolder_contents: bool = False
    ) -> list[ListDirectoryObject]:
        results = []
        for path in self.fs.walk.files(dirpath):
            info = self.fs.getinfo(path, namespaces=["details"])
            results.append(
                ListDirectoryObject(
                    filename=fs.path.basename(path),
                    absolute_path=path,
                    size=info.size,
                    modified=info.modified,
                    is_dir=False,
                )
            )
        return results

    async def copy(
        self,
        file_key_mappings: typing.Sequence[FileKeyMapping],
        delete_source: bool,
        chunk_byte_size: int,
        allow_missing: bool = False,
    ) -> None:
        for mapping in file_key_mappings:
            try:
                # Ensure destination directory exists
                dest_dir = fs.path.dirname(mapping.destination_key)
                if dest_dir:
                    self.fs.makedirs(dest_dir, recreate=True)

                # Copy file
                self.fs.copy(
                    mapping.source_key, mapping.destination_key, overwrite=True
                )

                # Delete source if move operation
                if delete_source:
                    self.fs.remove(mapping.source_key)

            except fs.errors.ResourceNotFound:
                if not allow_missing:
                    raise
