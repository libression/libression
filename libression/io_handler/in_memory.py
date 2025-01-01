import fs.memoryfs
import typing
import fs.path
import libression.entities.io
import datetime


class InMemoryIOHandler(libression.entities.io.IOHandler):
    """
    A memory-based implementation of IOHandler ... mainly for testing/development
    Best to use others (s3/webdav/etc.) for production
    """

    def __init__(self):
        self.fs = fs.memoryfs.MemoryFS()
        self.base_url = "memory://"
        self.files: dict[str, tuple[bytes, datetime.datetime]] = {}

    async def upload(
        self,
        file_streams: libression.entities.io.FileStreams,
        chunk_byte_size: int = libression.entities.io.DEFAULT_CHUNK_BYTE_SIZE,
    ) -> None:
        now = datetime.datetime.now()
        for key, stream in file_streams.file_streams.items():
            self.files[key] = (stream.file_stream.read(), now)
            stream.file_stream.seek(0)  # Reset stream position for potential reuse

    def get_readonly_urls(
        self,
        file_keys: typing.Sequence[str],
        expires_in_seconds: int,
    ) -> libression.entities.io.GetUrlsResponse:
        return libression.entities.io.GetUrlsResponse(
            urls={key: f"memory://{key}" for key in file_keys}
        )

    async def delete(
        self, file_keys: typing.Sequence[str], raise_on_error: bool = True
    ) -> None:
        for key in file_keys:
            try:
                del self.files[key]
            except KeyError:
                if raise_on_error:
                    raise

    async def list_objects(
        self, dirpath: str = "", subfolder_contents: bool = False
    ) -> list[libression.entities.io.ListDirectoryObject]:
        results = []
        now = datetime.datetime.now()

        # Normalize dirpath for comparison
        dirpath = dirpath.strip("/")
        prefix = f"{dirpath}/" if dirpath else ""

        # First collect all directories from file paths
        directories = set()
        for path in self.files.keys():
            parts = path.split("/")
            # Add each directory level
            for i in range(len(parts) - 1):
                dir_path = "/".join(parts[: i + 1])
                if not dirpath:
                    # For root listing, only add top-level directories
                    if "/" not in dir_path:
                        directories.add(dir_path)
                elif path.startswith(prefix):
                    # For nested listing, add relevant subdirectories
                    rel_path = (
                        dir_path[len(prefix) :]
                        if dir_path.startswith(prefix)
                        else dir_path
                    )
                    if subfolder_contents or "/" not in rel_path:
                        directories.add(dir_path)

        # Add directory entries
        for dir_path in directories:
            if (not dirpath and "/" not in dir_path) or (
                dirpath and dir_path.startswith(prefix)
            ):
                dir_name = dir_path.split("/")[-1]
                results.append(
                    libression.entities.io.ListDirectoryObject(
                        filename=dir_name,
                        absolute_path=dir_path,
                        size=0,
                        modified=now,
                        is_dir=True,
                    )
                )

        # Add file entries
        for path, (content, modified) in self.files.items():
            # For root directory listing
            if not dirpath:
                if not subfolder_contents and "/" in path:
                    continue
            # For specific directory listing
            else:
                if not path.startswith(prefix):
                    continue
                if not subfolder_contents and "/" in path[len(prefix) :]:
                    continue

            filename = path.split("/")[-1]
            results.append(
                libression.entities.io.ListDirectoryObject(
                    filename=filename,
                    absolute_path=path,
                    size=len(content),
                    modified=modified,
                    is_dir=False,
                )
            )

        return results

    async def copy(
        self,
        file_key_mappings: typing.Sequence[libression.entities.io.FileKeyMapping],
        delete_source: bool,
        allow_missing: bool = False,
        overwrite_existing: bool = True,
    ) -> None:
        now = datetime.datetime.now()
        for mapping in file_key_mappings:
            if mapping.source_key not in self.files:
                if not allow_missing:
                    raise ValueError(f"Source file {mapping.source_key} not found")
                continue

            if mapping.destination_key in self.files and not overwrite_existing:
                raise ValueError(
                    f"Destination file {mapping.destination_key} already exists"
                )

            content, _ = self.files[mapping.source_key]  # Get original content
            self.files[mapping.destination_key] = (
                content,
                now,
            )  # New timestamp for copy
            if delete_source:
                del self.files[mapping.source_key]
