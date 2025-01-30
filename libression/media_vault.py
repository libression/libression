import io
import logging
import concurrent.futures
import base64
import libression.db.client
import libression.entities.io
import libression.thumbnail
import libression.entities.media
import typing
import asyncio
import httpx
import libression.entities.base
import libression.config

logger = logging.getLogger(__name__)


DEFAULT_CACHE_STATIC_SUFFIX = "thumbnail.jpg"
DEFAULT_CACHE_DYNAMIC_SUFFIX = "thumbnail.gif"


class _InterimFileProcessContext(typing.NamedTuple):
    file_entry: libression.entities.db.DBFileEntry
    file_key_mapping: libression.entities.io.FileKeyMapping


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
    if mime_type_enum in libression.entities.media.HEIC_PROCESSING_MIME_TYPES:
        return libression.entities.media.SupportedMimeType.JPEG
    if mime_type_enum in libression.entities.media.OPEN_CV_PROCESSING_MIME_TYPES:
        return libression.entities.media.SupportedMimeType.JPEG
    if mime_type_enum in libression.entities.media.AV_PROCESSING_MIME_TYPES:
        return libression.entities.media.SupportedMimeType.GIF

    return None


def thumbnail_file_from_original_file(
    file_key: str,
    mime_type: libression.entities.media.SupportedMimeType | None,
    default_cache_static_suffix: str = DEFAULT_CACHE_STATIC_SUFFIX,
    default_cache_dynamic_suffix: str = DEFAULT_CACHE_DYNAMIC_SUFFIX,
) -> ThumbnailFile | None:
    if mime_type is None:
        return None  # hopeless

    thumbnail_mime_type = _thumbnail_type_from_mime_type(mime_type)
    if thumbnail_mime_type is None:
        return None

    if thumbnail_mime_type == libression.entities.media.SupportedMimeType.JPEG:
        return ThumbnailFile(
            key=f"{file_key}_{default_cache_static_suffix}",
            thumbnail_mime_type=thumbnail_mime_type,
            original_mime_type=mime_type,
        )
    elif thumbnail_mime_type == libression.entities.media.SupportedMimeType.GIF:
        return ThumbnailFile(
            key=f"{file_key}_{default_cache_dynamic_suffix}",
            thumbnail_mime_type=thumbnail_mime_type,
            original_mime_type=mime_type,
        )
    return None


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
    def _generate_thumbnail(
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

        readonly_url_response = self.data_io_handler.get_readonly_urls(
            [file_key],
            expires_in_seconds=presigned_url_expires_in_seconds,
        )

        original_url = (
            f"{readonly_url_response.base_url}/{readonly_url_response.paths[file_key]}"
        )

        url_header_response = httpx.head(
            original_url,
            verify=False,
            follow_redirects=True,
        )
        url_header_response.raise_for_status()

        original_mime_type = libression.entities.media.SupportedMimeType.best_guess(
            filename=file_key,
            given_mime_type_str=url_header_response.headers.get("content-type"),
        )

        thumbnail_file = thumbnail_file_from_original_file(
            file_key=file_key,
            mime_type=original_mime_type,
        )

        if thumbnail_file is None:
            return libression.thumbnail.ThumbnailInfo(
                thumbnail=None,
                phash=None,
                checksum=None,
            ), None

        # Generate thumbnail
        thumbnail_info = libression.thumbnail.generate_thumbnail_info(
            presigned_url=original_url,
            thumbnail_mime_type=thumbnail_file.thumbnail_mime_type,
            width_in_pixels=self.thumbnail_width_in_pixels,
        )

        return thumbnail_info, thumbnail_file

    async def _save_thumbnail_to_cache(
        self,
        thumbnail_info: libression.thumbnail.ThumbnailInfo,
        thumbnail_file: ThumbnailFile | None,
    ) -> None:
        if thumbnail_file is None:
            return None  # No thumbnail ...

        if (thumbnail_bytes := thumbnail_info.thumbnail) is None:
            raise ValueError("Thumbnail should exist at this point...")

        thumbnail_file_stream = libression.entities.io.FileStreamInfo(
            file_stream=io.BytesIO(thumbnail_bytes),
            mime_type=thumbnail_file.thumbnail_mime_type,
        )

        # Save thumbnail
        await self.cache_io_handler.upload(
            libression.entities.io.FileStreamInfos(
                {thumbnail_file.key: thumbnail_file_stream}
            ),
            chunk_byte_size=self.chunk_byte_size,
        )

    async def get_files_info(
        self,
        file_keys: list[str],
        presigned_url_expires_in_seconds: int = 60 * 60 * 24 * 30,
        max_concurrent_tasks: int = 5,
    ) -> list[libression.entities.db.DBFileEntry]:
        """
        Get presigned URLs for thumbnails, generating them if needed.

        Args:
            file_keys: List of file keys to get thumbnails for
            presigned_url_expires_in_seconds: How long the URLs should be valid
            force_cache_refresh: Whether to force regeneration of thumbnails

        TODO: refactor to enable force_cache_refresh (to update against existing entity_uuid! that was not working previously...so took out...)
        """
        if not file_keys:
            return []

        # Get existing entries from DB
        file_entries_from_db = []

        file_entries_from_db.extend(
            self.db_client.get_file_entries_by_file_keys(file_keys)
        )
        # remove any found keys from list of new db entries
        found_file_keys = {file_entry.file_key for file_entry in file_entries_from_db}
        file_keys_to_refresh = [
            file_key for file_key in file_keys if file_key not in found_file_keys
        ]

        # Generate new thumbnails if needed
        if file_keys_to_refresh:
            # Process thumbnails in parallel to avoid memory issues
            for batch in [
                file_keys_to_refresh[i : i + max_concurrent_tasks]
                for i in range(0, len(file_keys_to_refresh), max_concurrent_tasks)
            ]:
                batch_file_keys = list(batch)  # copy to avoid mutating original

                thumbnail_results: dict[
                    str, tuple[libression.thumbnail.ThumbnailInfo, ThumbnailFile | None]
                ] = {}

                # Generate the thumbnails in parallel (failed thumbnails will be Nones)
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=max_concurrent_tasks
                ) as executor:
                    future_to_key = {
                        executor.submit(
                            self._generate_thumbnail,
                            file_key=file_key,
                            presigned_url_expires_in_seconds=presigned_url_expires_in_seconds,
                        ): file_key
                        for file_key in batch_file_keys
                    }

                    for future in concurrent.futures.as_completed(future_to_key):
                        result = future.result()  # result is key (easier index?)
                        associated_file_key = future_to_key[future]
                        thumbnail_results[associated_file_key] = result

                # Save the thumbnails in parallel (to cache)
                saving_tasks = []

                for file_key, (
                    thumbnail_info,
                    thumbnail_file,
                ) in thumbnail_results.items():
                    saving_tasks.append(
                        self._save_thumbnail_to_cache(
                            thumbnail_info=thumbnail_info,
                            thumbnail_file=thumbnail_file,
                        )
                    )

                if saving_tasks:
                    await asyncio.gather(*saving_tasks, return_exceptions=True)

                # Register the file actions to DB
                data_to_register: list[libression.entities.db.DBFileEntry] = []

                for file_key, (
                    thumbnail_info,
                    thumbnail_file,
                ) in thumbnail_results.items():
                    mime_type = (
                        thumbnail_file.original_mime_type.value
                        if thumbnail_file is not None
                        else None
                    )

                    thumbnail_key: str | None = None
                    thumbnail_mime_type: str | None = None
                    thumbnail_checksum: str | None = None
                    thumbnail_phash: str | None = None

                    if thumbnail_file is not None:  # fill thumbnail data if exists
                        thumbnail_key = thumbnail_file.key
                        thumbnail_checksum = thumbnail_info.checksum
                        thumbnail_phash = thumbnail_info.phash

                        thumbnail_mime_type_enum = thumbnail_file.thumbnail_mime_type
                        if thumbnail_mime_type_enum is None:
                            raise ValueError(
                                f"Thumbnail exists but thumbnail mime type is None for file_key {file_key}"
                            )

                        thumbnail_mime_type = thumbnail_mime_type_enum.value

                    row_to_register = libression.entities.db.new_db_file_entry(
                        file_key=file_key,
                        thumbnail_key=thumbnail_key,
                        thumbnail_mime_type=thumbnail_mime_type,
                        thumbnail_checksum=thumbnail_checksum,
                        thumbnail_phash=thumbnail_phash,
                        mime_type=mime_type,
                    )
                    data_to_register.append(row_to_register)

                # Batch register all new entries to DB
                if data_to_register:
                    self.db_client.register_file_action(data_to_register)

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
    ) -> list[libression.entities.base.FileActionResponse]:
        data_delete_responses = await self.data_io_handler.delete(
            [file_entry.file_key for file_entry in file_entries if file_entry.file_key],
        )

        await self.cache_io_handler.delete(
            [
                file_entry.thumbnail_key
                for file_entry in file_entries
                if file_entry.thumbnail_key
            ],
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

        return data_delete_responses

    def _get_cache_key_mappings(
        self,
        entries_mappings_dict: dict[str, _InterimFileProcessContext],
    ) -> list[tuple[str, libression.entities.io.FileKeyMapping | None]]:
        """
        Key is file_key, value is old cache_key with new cache_key (if exists)
        """

        output: list[tuple[str, libression.entities.io.FileKeyMapping | None]] = []

        for file_key, file_entry_with_mapping in entries_mappings_dict.items():
            old_thumbnail_key = file_entry_with_mapping.file_entry.thumbnail_key
            if old_thumbnail_key is None:
                output.append((file_key, None))
            else:
                if old_thumbnail_key.endswith(DEFAULT_CACHE_STATIC_SUFFIX):
                    new_thumbnail_key = f"{file_entry_with_mapping.file_key_mapping.destination_key}_{DEFAULT_CACHE_STATIC_SUFFIX}"
                elif old_thumbnail_key.endswith(DEFAULT_CACHE_DYNAMIC_SUFFIX):
                    new_thumbnail_key = f"{file_entry_with_mapping.file_key_mapping.destination_key}_{DEFAULT_CACHE_DYNAMIC_SUFFIX}"
                else:
                    raise ValueError(
                        f"Thumbnail key {old_thumbnail_key} does not end with {DEFAULT_CACHE_STATIC_SUFFIX} or {DEFAULT_CACHE_DYNAMIC_SUFFIX}"
                    )

                output.append(
                    (
                        file_key,
                        libression.entities.io.FileKeyMapping(
                            source_key=old_thumbnail_key,
                            destination_key=new_thumbnail_key,
                        ),
                    )
                )

        return output

    async def copy(
        self,
        file_key_mappings: list[libression.entities.io.FileKeyMapping],
        delete_source: bool,
    ) -> list[libression.entities.base.FileActionResponse]:
        """
        Assume:
        - No duplications in file names
        """
        # Prep for copy (align db, create cache destination keys)
        existing_db_entries = self.db_client.get_file_entries_by_file_keys(
            [file_key_mapping.source_key for file_key_mapping in file_key_mappings]
        )

        if len(file_key_mappings) != len(existing_db_entries):
            raise ValueError(
                "File key mappings and existing DB entries must be the same length"
            )

        file_entries_with_key_mappings = dict()

        file_key_mappings_dict = {x.source_key: x for x in file_key_mappings}
        existing_db_entries_dict = {x.file_key: x for x in existing_db_entries}

        for file_key in file_key_mappings_dict.keys():
            file_entries_with_key_mappings[file_key] = _InterimFileProcessContext(
                file_entry=existing_db_entries_dict[file_key],
                file_key_mapping=file_key_mappings_dict[file_key],
            )

        cache_key_mappings = self._get_cache_key_mappings(
            file_entries_with_key_mappings,
        )

        # Copy data
        data_copy_responses = await self.data_io_handler.copy(
            file_key_mappings,
            delete_source=delete_source,
        )

        await self.cache_io_handler.copy(
            [x[1] for x in cache_key_mappings if x[1] is not None],
            delete_source=delete_source,
        )  # currently ignore ...

        # Register db
        file_actions_to_register = []

        cache_key_mappings_dict = {x[0]: x[1] for x in cache_key_mappings}
        data_copy_responses_dict = {x.file_key: x for x in data_copy_responses}

        #######  *********************** CHECK WHICH FILE KEY?!? not sure if source/destination mixup?
        for file_key, file_entry_with_mapping in file_entries_with_key_mappings.items():
            data_copy_response = data_copy_responses_dict[file_key]

            if not data_copy_response.success:
                row = libression.entities.db.existing_db_file_entry(
                    file_key=file_key,
                    file_entity_uuid=file_entry_with_mapping.file_entry.file_entity_uuid,
                    action_type=libression.entities.db.DBFileAction.MISSING,
                )
                file_actions_to_register.append(row)
            else:
                new_thumbnail_key: str | None = None

                if new_thumbnail_file_key_mapping := cache_key_mappings_dict.get(
                    file_key
                ):
                    new_thumbnail_key = new_thumbnail_file_key_mapping.destination_key

                if delete_source:  # MOVE
                    row = libression.entities.db.existing_db_file_entry(
                        # New location
                        file_key=file_entry_with_mapping.file_key_mapping.destination_key,
                        thumbnail_key=new_thumbnail_key,
                        action_type=libression.entities.db.DBFileAction.MOVE,
                        # Preserve the rest
                        file_entity_uuid=file_entry_with_mapping.file_entry.file_entity_uuid,
                        thumbnail_mime_type=file_entry_with_mapping.file_entry.thumbnail_mime_type,
                        thumbnail_checksum=file_entry_with_mapping.file_entry.thumbnail_checksum,
                        thumbnail_phash=file_entry_with_mapping.file_entry.thumbnail_phash,
                        mime_type=file_entry_with_mapping.file_entry.mime_type,
                        tags=file_entry_with_mapping.file_entry.tags,
                    )
                    file_actions_to_register.append(row)
                else:  # COPY
                    row = libression.entities.db.new_db_file_entry(
                        file_key=file_entry_with_mapping.file_key_mapping.destination_key,  # new location
                        thumbnail_key=new_thumbnail_key,
                        thumbnail_mime_type=file_entry_with_mapping.file_entry.thumbnail_mime_type,
                        thumbnail_checksum=file_entry_with_mapping.file_entry.thumbnail_checksum,
                        thumbnail_phash=file_entry_with_mapping.file_entry.thumbnail_phash,
                        mime_type=file_entry_with_mapping.file_entry.mime_type,
                        tags=file_entry_with_mapping.file_entry.tags,
                    )
                    file_actions_to_register.append(row)

            registered_entries = self.db_client.register_file_action(
                file_actions_to_register
            )

            if not delete_source:  # COPY, so new file_entity_uuids need tags copied
                self.db_client.register_file_tags(
                    [
                        libression.entities.db.DBTagEntry(
                            file_entity_uuid=entry.file_entity_uuid,
                            tags=entry.tags,
                        )
                        for entry in registered_entries
                    ]
                )  # register tags for new file_entity_uuid

        return data_copy_responses

    async def upload_media(
        self,
        upload_entries: list[libression.entities.base.UploadEntry],
        target_dir_key: str,
        max_concurrent_uploads: int,
        presigned_url_expires_in_seconds: int = 60 * 60 * 24 * 30,
    ) -> list[libression.entities.db.DBFileEntry]:
        """
        Uploads media files to a target directory, generates thumbnails, and registers them in the DB.

        Args:
            upload_entries: List of UploadEntry objects containing base64-encoded files and filenames
            target_dir_key: Directory key where files should be uploaded
            presigned_url_expires_in_seconds: How long the returned URLs should be valid
            max_concurrent_uploads: Maximum number of concurrent file uploads

        Returns:
            List of DBFileEntry objects for the uploaded files
        """
        # Normalize directory key
        normalized_dir_key = (
            "" if not target_dir_key else f"{target_dir_key.rstrip('/')}/"
        )

        # Process files in batches to limit memory usage
        all_file_keys = []

        for batch in [
            upload_entries[i : i + max_concurrent_uploads]
            for i in range(0, len(upload_entries), max_concurrent_uploads)
        ]:
            # Create file streams for current batch
            file_stream_infos = {}
            for entry in batch:
                file_key = f"{normalized_dir_key}{entry.filename}"
                all_file_keys.append(file_key)

                # Convert base64 to BytesIO without loading entire file into memory
                file_stream = io.BytesIO(base64.b64decode(entry.file_source))

                file_stream_infos[file_key] = libression.entities.io.FileStreamInfo(
                    file_stream=file_stream,
                    mime_type=libression.entities.media.SupportedMimeType.best_guess(
                        filename=entry.filename,
                        given_mime_type_str=None,
                    ),
                )

            # Upload current batch
            await self.data_io_handler.upload(
                libression.entities.io.FileStreamInfos(file_stream_infos),
                chunk_byte_size=self.chunk_byte_size,
            )

            # Clean up streams
            for info in file_stream_infos.values():
                info.file_stream.close()

        # Generate thumbnails and register in DB for all uploaded files
        return await self.get_files_info(
            file_keys=all_file_keys,
            presigned_url_expires_in_seconds=presigned_url_expires_in_seconds,
        )

    # TODO: cover for errors + account for when external action (deletion/moves) how we should handle...
