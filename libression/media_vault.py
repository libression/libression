import io
import logging
import libression.db.client
import libression.entities.io
import libression.thumbnail
import libression.entities.media
import typing
import asyncio
import httpx

logger = logging.getLogger(__name__)


DEFAULT_CACHE_STATIC_SUFFIX = "_thumbnail.jpg"
DEFAULT_CACHE_DYNAMIC_SUFFIX = "_thumbnail.gif"


class ThumbnailFile(typing.NamedTuple):
    key: str
    thumbnail_mime_type: libression.entities.media.SupportedMimeType
    original_mime_type: libression.entities.media.SupportedMimeType


def _thumbnail_type_from_mime_type(
    mime_type_enum: libression.entities.media.SupportedMimeType,
) -> libression.entities.media.SupportedMimeType | None:
    """
    images -> jpeg
    videos -> gif
    """
    if isinstance(
        mime_type_enum,
        (
            libression.entities.media.OpenCvProccessableImageMimeType,
            libression.entities.media.HeifMimeType,
        ),
    ):
        return libression.entities.media.OpenCvProccessableImageMimeType.JPEG

    elif isinstance(
        mime_type_enum,
        libression.entities.media.AvProccessableMimeType,
    ):
        return libression.entities.media.AvProccessableMimeType.GIF

    return None


def thumbnail_file_from_original_file(
    file_key: str,
    original_mime_type: libression.entities.media.SupportedMimeType | None,
    default_cache_static_suffix: str = DEFAULT_CACHE_STATIC_SUFFIX,
    default_cache_dynamic_suffix: str = DEFAULT_CACHE_DYNAMIC_SUFFIX,
) -> ThumbnailFile | None:
    if original_mime_type is None:
        return None

    thumbnail_mime_type = _thumbnail_type_from_mime_type(original_mime_type)
    if thumbnail_mime_type is None:
        return None

    if (
        original_mime_type
        == libression.entities.media.OpenCvProccessableImageMimeType.JPEG
    ):
        return ThumbnailFile(
            key=f"{file_key}_{default_cache_static_suffix}",
            thumbnail_mime_type=thumbnail_mime_type,
            original_mime_type=original_mime_type,
        )
    elif original_mime_type == libression.entities.media.AvProccessableMimeType.GIF:
        return ThumbnailFile(
            key=f"{file_key}_{default_cache_dynamic_suffix}",
            thumbnail_mime_type=thumbnail_mime_type,
            original_mime_type=original_mime_type,
        )
    return None


class StreamWrapper(io.RawIOBase):
    def __init__(self, response: httpx.Response, chunk_size: int = 8192):
        self.response = response
        self.chunk_size = chunk_size
        self._iterator = response.iter_bytes(chunk_size)
        self._buffer = b""

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            return b"".join(self._iterator)

        while len(self._buffer) < size:
            try:
                self._buffer += next(self._iterator)
            except StopIteration:
                break

        result = self._buffer[:size]
        self._buffer = self._buffer[size:]
        return result


async def _get_file_stream(url: str) -> libression.entities.io.FileStream:
    """
    Get a file stream from a URL using httpx + StreamWrapper
    FileStream includes best guess mime type enum
    """

    async with httpx.AsyncClient() as client:
        response = await client.stream("GET", url)
        response.raise_for_status()

        content_length = int(response.headers.get("content-length", 0))

        # mime type enum (first headers, fallback from filename)
        mime_type_enum: libression.entities.media.SupportedMimeType | None = None
        content_type = response.headers.get("content-type")
        if content_type is not None:
            mime_type_enum = libression.entities.media.SupportedMimeType.from_value(
                content_type
            )

        if mime_type_enum is None:  # fallback from filename
            mime_type_enum = libression.entities.media.SupportedMimeType.from_filename(
                url
            )

        stream = typing.cast(
            typing.BinaryIO, io.BufferedReader(StreamWrapper(response))
        )

        return libression.entities.io.FileStream(
            file_stream=stream, file_byte_size=content_length, mime_type=mime_type_enum
        )


class MediaVault:
    def __init__(
        self,
        data_io_handler: libression.entities.io.IOHandler,
        cache_io_handler: libression.entities.io.IOHandler,
        db_client: libression.db.client.DBClient,
        thumbnail_width_in_pixels: int,
        chunk_byte_size: int,
    ):
        self.data_io_handler = data_io_handler
        self.cache_io_handler = cache_io_handler
        self.thumbnail_width_in_pixels = thumbnail_width_in_pixels
        self.db_client = db_client
        self.chunk_byte_size = chunk_byte_size

    # Actions should be direct API call functions/handlers
    async def _save_thumbnail(
        self,
        file_key: str,
        presigned_url_expires_in_seconds: int,
    ) -> tuple[
        libression.thumbnail.ThumbnailInfo,
        ThumbnailFile | None,
    ]:
        """
        Get original file stream ( and mime type enum )
        Generate thumbnail
        Save thumbnail (to cache, not DB)
        """

        original_url = self.data_io_handler.get_readonly_urls(
            [file_key],
            expires_in_seconds=presigned_url_expires_in_seconds,
        ).urls[file_key]

        # Get file stream ( and mime type enum )
        original_filestream = await _get_file_stream(original_url)

        thumbnail_file = thumbnail_file_from_original_file(
            file_key=file_key,
            original_mime_type=original_filestream.mime_type,
        )

        if thumbnail_file is None:
            return libression.thumbnail.ThumbnailInfo(
                thumbnail=None,
                phash=None,
                checksum=None,
            ), None  # No valid thumbnail mime or file_key ... bail

        # Generate thumbnail
        thumbnail_info = libression.thumbnail.generate_thumbnail_info(
            original_filestream,
            thumbnail_mime_type=thumbnail_file.thumbnail_mime_type,
            width_in_pixels=self.thumbnail_width_in_pixels,
        )

        if (thumbnail_bytes := thumbnail_info.thumbnail) is None:
            return libression.thumbnail.ThumbnailInfo(
                thumbnail=None,
                phash=None,
                checksum=None,
            ), None

        thumbnail_file_stream = libression.entities.io.FileStream(
            # stream is small so we can use BytesIO
            file_stream=io.BytesIO(thumbnail_bytes),
            file_byte_size=len(thumbnail_bytes),
            mime_type=thumbnail_file.thumbnail_mime_type,
        )

        # Save thumbnail
        await self.cache_io_handler.upload(
            libression.entities.io.FileStreams(
                {thumbnail_file.key: thumbnail_file_stream}
            ),
            chunk_byte_size=self.chunk_byte_size,
        )

        return thumbnail_info, thumbnail_file

    async def get_files_info(
        self,
        file_keys: list[str],
        presigned_url_expires_in_seconds: int = 60 * 60 * 24 * 30,
        force_cache_refresh: bool = False,
        max_concurrent_tasks: int = 5,
    ) -> list[libression.entities.db.DBFileEntry]:
        """
        Get presigned URLs for thumbnails, generating them if needed.

        Args:
            file_keys: List of file keys to get thumbnails for
            presigned_url_expires_in_seconds: How long the URLs should be valid
            force_cache_refresh: Whether to force regeneration of thumbnails
        """
        if not file_keys:
            return []

        # Get existing entries from DB
        file_entries_from_db = []

        if not force_cache_refresh:
            file_entries_from_db.extend(
                self.db_client.get_file_entries_by_file_keys(file_keys)
            )
            # If record exists but thumbnail is null, it was tried before so skip
            found_file_keys = {
                file_entry.file_key
                for file_entry in file_entries_from_db
                if file_entry.thumbnail_key is not None
            }
            file_keys_to_refresh = [
                file_key for file_key in file_keys if file_key not in found_file_keys
            ]
        else:
            file_keys_to_refresh = file_keys

        # Generate new thumbnails if needed
        if file_keys_to_refresh:
            # Process thumbnails in parallel to avoid memory issues
            new_entries = []

            for batch in [
                file_keys_to_refresh[i : i + max_concurrent_tasks]
                for i in range(0, len(file_keys_to_refresh), max_concurrent_tasks)
            ]:
                batch_file_keys = list(batch)  # copy to avoid mutating original
                tasks = []

                for file_key in batch:
                    tasks.append(
                        self._save_thumbnail(
                            file_key=file_key,
                            presigned_url_expires_in_seconds=presigned_url_expires_in_seconds,
                        )
                    )

                if tasks:
                    # Wait for batch to complete and collect results
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    # Process results and prepare DB entries
                    for file_key, result in zip(batch_file_keys, results):
                        if isinstance(result, BaseException):
                            logger.error(
                                f"Error generating thumbnail for file_key {file_key}: {str(result)}"
                            )
                            # Register failure
                            new_entries.append(
                                libression.entities.db.new_db_file_entry(
                                    file_key=file_key,
                                    thumbnail_key=None,  # Indicates failed attempt
                                    mime_type=None,  # Not sure what was detected but set to None
                                )
                            )
                            continue

                        thumbnail_info, thumbnail_file = result
                        if thumbnail_file is None:
                            raise ValueError(
                                f"Shouldn't be here... thumbnail file is None for file_key {file_key}"
                            )

                        # Register success
                        new_entries.append(
                            libression.entities.db.new_db_file_entry(
                                file_key=file_key,
                                thumbnail_key=thumbnail_file.key,
                                thumbnail_mime_type=thumbnail_file.thumbnail_mime_type.value,
                                thumbnail_checksum=thumbnail_info.checksum,
                                thumbnail_phash=thumbnail_info.phash,
                                mime_type=thumbnail_file.original_mime_type.value,
                            )
                        )

            # Batch register all new entries to DB
            if new_entries:
                self.db_client.register_file_action(new_entries)

        # Get updated entries including new thumbnails
        return self.db_client.get_file_entries_by_file_keys(file_keys)

    def get_thumbnail_presigned_urls(
        self,
        thumbnail_keys: list[str],
        presigned_url_expires_in_seconds: int = 60 * 60 * 24 * 30,
    ) -> libression.entities.io.GetUrlsResponse:
        return self.cache_io_handler.get_readonly_urls(
            thumbnail_keys,
            expires_in_seconds=presigned_url_expires_in_seconds,
        )

    def get_data_presigned_urls(
        self,
        file_keys: list[str],
        presigned_url_expires_in_seconds: int = 60 * 60 * 24 * 30,
    ) -> libression.entities.io.GetUrlsResponse:
        return self.data_io_handler.get_readonly_urls(
            file_keys,
            expires_in_seconds=presigned_url_expires_in_seconds,
        )

    async def delete(
        self,
        file_entries: list[libression.entities.db.DBFileEntry],
    ) -> None:
        await self.data_io_handler.delete(
            [file_entry.file_key for file_entry in file_entries if file_entry.file_key]
        )
        await self.cache_io_handler.delete(
            [
                file_entry.thumbnail_key
                for file_entry in file_entries
                if file_entry.thumbnail_key
            ]
        )

        deletion_db_entries = [
            libression.entities.db.existing_db_file_entry(
                file_key=file_entry.file_key,
                file_entity_uuid=file_entry.file_entity_uuid,
                action_type=libression.entities.db.DBFileAction.DELETE,
            )
            for file_entry in file_entries
        ]
        self.db_client.register_file_action(deletion_db_entries)

    async def copy(
        self,
        file_key_mappings: list[libression.entities.io.FileKeyMapping],
        delete_source: bool,
    ) -> list[libression.entities.db.DBFileEntry]:
        """
        Assume:
        - No duplications in file names
        - All cache is present
        """

        existing_db_entries = self.db_client.get_file_entries_by_file_keys(
            [file_key_mapping.source_key for file_key_mapping in file_key_mappings]
        )  # assume all entries exists...if not we need to handle that

        await self.data_io_handler.copy(
            file_key_mappings,
            delete_source=delete_source,
        )
        await self.cache_io_handler.copy(
            file_key_mappings,
            delete_source=delete_source,
        )

        if len(file_key_mappings) != len(existing_db_entries):
            raise ValueError(
                "File key mappings and existing DB entries must be the same length"
            )

        sorted_file_key_mappings = sorted(file_key_mappings, key=lambda x: x.source_key)
        sorted_existing_db_entries = sorted(
            existing_db_entries, key=lambda x: x.file_key
        )

        if delete_source:
            file_action_entries = [
                libression.entities.db.existing_db_file_entry(
                    file_key=file_key_mapping.destination_key,  # new location
                    file_entity_uuid=file_entry.file_entity_uuid,
                    action_type=libression.entities.db.DBFileAction.MOVE,
                    thumbnail_key=file_entry.thumbnail_key,
                    thumbnail_mime_type=file_entry.thumbnail_mime_type,
                    thumbnail_checksum=file_entry.thumbnail_checksum,
                    thumbnail_phash=file_entry.thumbnail_phash,
                    mime_type=file_entry.mime_type,
                    tags=file_entry.tags,
                )
                for file_entry, file_key_mapping in zip(
                    sorted_existing_db_entries, sorted_file_key_mappings
                )
            ]
            registered_entries = self.db_client.register_file_action(
                file_action_entries
            )

        else:  # new file_entity_uuid but retain tags for new copy!
            file_action_entries = [
                libression.entities.db.new_db_file_entry(
                    file_key=file_key_mapping.destination_key,  # new location
                    thumbnail_key=file_entry.thumbnail_key,
                    thumbnail_mime_type=file_entry.thumbnail_mime_type,
                    thumbnail_checksum=file_entry.thumbnail_checksum,
                    thumbnail_phash=file_entry.thumbnail_phash,
                    mime_type=file_entry.mime_type,
                    tags=file_entry.tags,
                )
                for file_entry, file_key_mapping in zip(
                    sorted_existing_db_entries, sorted_file_key_mappings
                )
            ]
            registered_entries = self.db_client.register_file_action(
                file_action_entries
            )
            self.db_client.register_file_tags(
                registered_entries
            )  # register tags for new file_entity_uuid

        return registered_entries

    # TODO: FINISH!!! + ADD TESTS
    def upload_media(self) -> None:
        """
        Needs to allow specification of file keys
        upload to data_io_handler
        return get_thumbnail_presigned_urls
        """
        raise NotImplementedError("TODO")

    def search_by_tags(self):
        pass

    def edit_tags(self):
        pass

    # TODO: cover for errors + account for when external action (deletion/moves) how we should handle...
