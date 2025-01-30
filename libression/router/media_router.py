import contextlib
import fastapi
import typing
import pydantic
import libression.config
import libression.entities.io
import libression.entities.base
import libression.entities.db
import libression.io_handler.webdav
from libression.media_vault import MediaVault
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


############################################################
# --- Pydantic Models ---
# Only define new ones. Existing:
# - libression.entities.io.FileKeyMapping
# - libression.entities.base.FileActionResponse
# - libression.entities.base.UploadEntry
############################################################


class FileEntry(pydantic.BaseModel):
    """
    Identical to DBFileEntry, but
    - without action_type
    - file_entity_uuid is always filled
    - without action_created_at
    - without tags_created_at
    """

    file_key: str
    file_entity_uuid: str
    thumbnail_key: str | None
    thumbnail_mime_type: str | None
    thumbnail_checksum: str | None
    thumbnail_phash: str | None
    mime_type: str | None
    tags: list[str]


class FileEntries(pydantic.BaseModel):
    files: list[FileEntry]


class UploadRequest(pydantic.BaseModel):
    files: list[libression.entities.base.UploadEntry]
    target_dir: str


class GetFilesInfoRequest(pydantic.BaseModel):
    file_keys: list[str]
    force_refresh: bool = False  # Not used yet...


class GetFileUrlsRequest(pydantic.BaseModel):
    file_keys: list[str]


class CopyRequest(pydantic.BaseModel):
    file_mappings: list[libression.entities.io.FileKeyMapping]
    delete_source: bool = False


class DeleteRequest(pydantic.BaseModel):
    file_entries: list[FileEntry]


class SearchByTagsRequest(pydantic.BaseModel):
    include_tag_groups: list[list[str]]
    exclude_tags: list[str]


class TagEntries(pydantic.BaseModel):
    tag_entries: list[libression.entities.db.DBTagEntry]


class ShowDirContentsRequest(pydantic.BaseModel):
    dir_key: str = pydantic.Field(
        description="dir to show contents/subdirs of ... no slashes at the beginning or end"
    )
    subfolder_contents: bool = False


class ShowDirContentsResponse(pydantic.BaseModel):
    dir_contents: list[libression.entities.io.ListDirectoryObject]


############################################################
# --- Lifespans ---
############################################################


@contextlib.asynccontextmanager
async def lifespan(app: fastapi.FastAPI) -> typing.AsyncGenerator[None, None]:
    # Initialize MediaVault

    data_io_handler = libression.io_handler.webdav.WebDAVIOHandler(
        base_url=libression.config.WEBDAV_BASE_URL,
        url_path=libression.config.WEBDAV_URL_PATH,
        presigned_url_path=libression.config.WEBDAV_PRESIGNED_URL_PATH,
        verify_ssl=libression.config.WEBDAV_VERIFY_SSL,
    )
    cache_io_handler = libression.io_handler.webdav.WebDAVIOHandler(
        base_url=libression.config.WEBDAV_BASE_URL,
        url_path=libression.config.WEBDAV_CACHE_URL_PATH,
        presigned_url_path=libression.config.WEBDAV_CACHE_PRESIGNED_URL_PATH,
        verify_ssl=libression.config.WEBDAV_VERIFY_SSL,
    )
    db_client = libression.db.client.DBClient(
        db_path=libression.config.DB_PATH,
    )

    app.state.media_vault = MediaVault(
        data_io_handler=data_io_handler,
        cache_io_handler=cache_io_handler,
        db_client=db_client,
        thumbnail_width_in_pixels=libression.config.THUMBNAIL_WIDTH_IN_PIXELS,
        chunk_byte_size=libression.config.DEFAULT_CHUNK_BYTE_SIZE,
    )

    yield

    # Cleanup (if needed)
    app.state.media_vault = None


############################################################
# --- Endpoints ---
############################################################

router = fastapi.APIRouter(prefix="/libression/v1")


@router.post("/upload", response_model=FileEntries)
async def upload_media(
    request: fastapi.Request,
    upload_request: UploadRequest,
) -> FileEntries:
    """
    Upload media files and generate thumbnails.
    Meant for low volume uploads only...needs optimising for high volume.
    Preferably upload big files directly into io source
    """

    logger.info(f"Starting upload request for target dir: {upload_request.target_dir}")
    logger.info(f"Number of files to upload: {len(upload_request.files)}")
    logger.info(
        f"WebDAV Base URL: {request.app.state.media_vault.data_io_handler.base_url}"
    )
    logger.info(
        f"WebDAV URL Path: {request.app.state.media_vault.data_io_handler.url_path}"
    )

    result = await request.app.state.media_vault.upload_media(
        upload_entries=upload_request.files,
        target_dir_key=upload_request.target_dir,
        max_concurrent_uploads=libression.config.MEDIA_VAULT_MAX_CONCURRENT_UPLOADS,
    )

    return FileEntries(
        files=[FileEntry.model_validate(entry.to_dict()) for entry in result]
    )


@router.post("/files_info", response_model=FileEntries)
async def get_files_info(
    request: fastapi.Request,
    get_files_info_request: GetFilesInfoRequest,
) -> FileEntries:
    """Get file information including thumbnail details."""
    result = await request.app.state.media_vault.get_files_info(
        file_keys=get_files_info_request.file_keys,
    )
    return FileEntries(
        files=[FileEntry.model_validate(entry.to_dict()) for entry in result]
    )


@router.post("/thumbnails_urls", response_model=libression.entities.io.GetUrlsResponse)
async def get_thumbnail_urls(
    request: fastapi.Request,
    get_file_urls_request: GetFileUrlsRequest,
) -> libression.entities.io.GetUrlsResponse:
    """Get presigned URLs for thumbnails."""
    return request.app.state.media_vault.get_thumbnail_presigned_urls(
        get_file_urls_request.file_keys
    )


@router.post("/files_urls", response_model=libression.entities.io.GetUrlsResponse)
async def get_file_urls(
    request: fastapi.Request,
    get_file_urls_request: GetFileUrlsRequest,
) -> libression.entities.io.GetUrlsResponse:
    """Get presigned URLs for original files."""
    return request.app.state.media_vault.get_data_presigned_urls(
        get_file_urls_request.file_keys
    )


@router.post("/copy", response_model=list[libression.entities.base.FileActionResponse])
async def copy_files(
    request: fastapi.Request,
    copy_request: CopyRequest,
) -> list[libression.entities.base.FileActionResponse]:
    """Copy or move files with their thumbnails."""
    mappings = [
        libression.entities.io.FileKeyMapping(
            source_key=mapping.source_key,
            destination_key=mapping.destination_key,
        )
        for mapping in copy_request.file_mappings
    ]
    return await request.app.state.media_vault.copy(
        file_key_mappings=mappings,
        delete_source=copy_request.delete_source,
    )


@router.post(
    "/delete", response_model=list[libression.entities.base.FileActionResponse]
)
async def delete_files(
    request: fastapi.Request,
    delete_request: DeleteRequest,
) -> list[libression.entities.base.FileActionResponse]:
    """Delete files and their thumbnails."""
    entries = [
        libression.entities.db.DBFileEntry(
            file_key=entry.file_key,
            file_entity_uuid=entry.file_entity_uuid,
            action_type=libression.entities.db.DBFileAction.DELETE,
        )
        for entry in delete_request.file_entries
    ]

    return await request.app.state.media_vault.delete(file_entries=entries)


@router.post("/update_tags")
def update_tags(
    request: fastapi.Request,
    tag_entries_request: TagEntries,
) -> None:
    """
    Update tags for files (must include existing tags to keep + new tags)
    """

    return request.app.state.media_vault.db_client.register_file_tags(
        tag_entries_request.tag_entries,
    )


@router.post("/search_by_tags", response_model=FileEntries)
def search_by_tags(
    request: fastapi.Request,
    search_by_tags_request: SearchByTagsRequest,
) -> FileEntries:
    output = request.app.state.media_vault.db_client.get_file_entries_by_tags(
        include_tag_groups=search_by_tags_request.include_tag_groups,
        exclude_tags=search_by_tags_request.exclude_tags,
    )
    return FileEntries(
        files=[
            FileEntry.model_validate(
                entry.to_dict(),
            )
            for entry in output
        ]
    )


@router.post(
    "/show_dir_contents",
    response_model=ShowDirContentsResponse,
)
async def show_dir_contents(
    request: fastapi.Request,
    show_dir_contents_request: ShowDirContentsRequest,
) -> ShowDirContentsResponse:
    list_obj_output = await request.app.state.media_vault.data_io_handler.list_objects(
        dirpath=show_dir_contents_request.dir_key,
        subfolder_contents=show_dir_contents_request.subfolder_contents,
    )

    return ShowDirContentsResponse(
        dir_contents=list_obj_output,
    )
