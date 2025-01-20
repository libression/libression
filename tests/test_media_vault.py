import pytest
import uuid
import io
import base64
import httpx
import libression.entities.io
import libression.entities.base
import libression.entities.media
import libression.entities.db
import libression.db.client
from libression.media_vault import MediaVault


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler_fixture_name", ["docker_webdav_io_handler"])
async def test_get_files_info_empty_input(
    db_client, io_handler_fixture_name, request: pytest.FixtureRequest
):
    io_handler = request.getfixturevalue(io_handler_fixture_name)
    media_vault = MediaVault(
        data_io_handler=io_handler,
        cache_io_handler=io_handler,
        db_client=db_client,
        thumbnail_width_in_pixels=200,
        chunk_byte_size=8192,
    )

    result = await media_vault.get_files_info([])

    assert result == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "io_handler_fixture_name, minimal_image",
    [
        ("docker_webdav_io_handler", "png"),
    ],
    indirect=["minimal_image"],
)
async def test_get_files_info_existing_thumbnails(
    db_client,
    io_handler_fixture_name,
    request: pytest.FixtureRequest,
    minimal_image,
):
    png_file_key = f"{uuid.uuid4()}.png"

    io_handler = request.getfixturevalue(io_handler_fixture_name)
    media_vault = MediaVault(
        data_io_handler=io_handler,
        cache_io_handler=io_handler,
        db_client=db_client,
        thumbnail_width_in_pixels=200,
        chunk_byte_size=8192,
    )

    byte_stream = io.BytesIO(minimal_image)
    await io_handler.upload(
        libression.entities.io.FileStreamInfos(
            file_streams={
                png_file_key: libression.entities.io.FileStreamInfo(
                    file_stream=byte_stream,
                    mime_type=libression.entities.media.SupportedMimeType.PNG,
                )
            }
        )
    )

    result = await media_vault.get_files_info([png_file_key])
    assert len(result) == 1
    assert result[0].thumbnail_key is not None
    assert result[0].thumbnail_mime_type == "image/jpeg"
    assert result[0].thumbnail_checksum is not None
    assert result[0].thumbnail_phash is not None

    # Teardown
    await media_vault.delete([result[0]])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "io_handler_fixture_name, minimal_image",
    [
        ("docker_webdav_io_handler", "png"),
    ],
    indirect=["minimal_image"],
)
async def test_delete_files(
    db_client,
    io_handler_fixture_name,
    request: pytest.FixtureRequest,
    minimal_image,
    dummy_folder_name,
):
    io_handler = request.getfixturevalue(io_handler_fixture_name)
    media_vault = MediaVault(
        data_io_handler=io_handler,
        cache_io_handler=io_handler,
        db_client=db_client,
        thumbnail_width_in_pixels=200,
        chunk_byte_size=8192,
    )

    png_file_key = f"{uuid.uuid4()}.png"

    await io_handler.upload(
        libression.entities.io.FileStreamInfos(
            file_streams={
                png_file_key: libression.entities.io.FileStreamInfo(
                    file_stream=io.BytesIO(minimal_image),
                    mime_type=libression.entities.media.SupportedMimeType.PNG,
                ),
                f"{dummy_folder_name}/{png_file_key}": libression.entities.io.FileStreamInfo(
                    file_stream=io.BytesIO(minimal_image),  # fresh bytesIO copy
                    mime_type=libression.entities.media.SupportedMimeType.PNG,
                ),
            }
        )
    )

    files_info = await media_vault.get_files_info(
        [png_file_key, f"{dummy_folder_name}/{png_file_key}"]
    )
    entries = db_client.get_file_entries_by_file_keys([x.file_key for x in files_info])
    db_entry_count_before = len(entries)  # (should be 2)
    io_object_count_before = len(
        await io_handler.list_objects(subfolder_contents=True)
    )  # (should be 4)

    # Act
    await media_vault.delete(files_info)

    # Verify DB client called with correct deletion entries
    entries = db_client.get_file_entries_by_file_keys(files_info[0].file_key)
    assert len(entries) == db_entry_count_before - 2  # 2 less entries

    # Verify io
    assert (
        len(await io_handler.list_objects(subfolder_contents=True))
        == io_object_count_before - 4
    )  # 4 less objects


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "io_handler_fixture_name, minimal_image",
    [
        ("docker_webdav_io_handler", "png"),
    ],
    indirect=["minimal_image"],
)
async def test_copy_files(
    db_client,
    io_handler_fixture_name,
    request: pytest.FixtureRequest,
    minimal_image,
    dummy_folder_name,
):
    io_handler = request.getfixturevalue(io_handler_fixture_name)
    media_vault = MediaVault(
        data_io_handler=io_handler,
        cache_io_handler=io_handler,
        db_client=db_client,
        thumbnail_width_in_pixels=200,
        chunk_byte_size=8192,
    )

    png_file_key = f"{uuid.uuid4()}.png"

    await io_handler.upload(
        libression.entities.io.FileStreamInfos(
            file_streams={
                png_file_key: libression.entities.io.FileStreamInfo(
                    file_stream=io.BytesIO(minimal_image),
                    mime_type=libression.entities.media.SupportedMimeType.PNG,
                ),
                f"{dummy_folder_name}/{png_file_key}": libression.entities.io.FileStreamInfo(
                    file_stream=io.BytesIO(minimal_image),  # fresh bytesIO copy
                    mime_type=libression.entities.media.SupportedMimeType.PNG,
                ),
            }
        )
    )

    files_info = await media_vault.get_files_info(
        [png_file_key, f"{dummy_folder_name}/{png_file_key}"]
    )

    # Act
    result = await media_vault.copy(
        [
            libression.entities.io.FileKeyMapping(
                source_key=png_file_key,
                destination_key=f"{dummy_folder_name}/{dummy_folder_name}/{png_file_key}",
            ),
            libression.entities.io.FileKeyMapping(
                source_key=f"{dummy_folder_name}/{png_file_key}",
                destination_key=f"{dummy_folder_name}/{dummy_folder_name}/{dummy_folder_name}/{png_file_key}",
            ),
        ],
        delete_source=False,  # Copy
    )

    # Assert
    assert len([x for x in result if x.success]) == 2
    assert len([x for x in result if x.error is not None]) == 0

    # Verify DB client called with correct deletion entries
    original_entries = db_client.get_file_entries_by_file_keys(
        [png_file_key, f"{dummy_folder_name}/{png_file_key}"]
    )  # Original entries
    assert len(original_entries) == 2  # didn't delete original

    destination_keys = [
        f"{dummy_folder_name}/{dummy_folder_name}/{png_file_key}",
        f"{dummy_folder_name}/{dummy_folder_name}/{dummy_folder_name}/{png_file_key}",
    ]

    copied_entries = db_client.get_file_entries_by_file_keys(
        destination_keys
    )  # New entries
    assert len(copied_entries) == 2
    assert all(
        x.action_type == libression.entities.db.DBFileAction.CREATE
        for x in copied_entries
    )
    assert all(x.thumbnail_key is not None for x in copied_entries)
    # New file_entity_uuid created (new copies)
    assert (
        len(
            set([x.file_entity_uuid for x in files_info]).intersection(
                set([x.file_entity_uuid for x in copied_entries])
            )
        )
        == 0
    )

    # Teardown
    await media_vault.delete(copied_entries)
    await media_vault.delete(original_entries)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "io_handler_fixture_name, minimal_image",
    [
        ("docker_webdav_io_handler", "png"),
    ],
    indirect=["minimal_image"],
)
async def test_move_files(
    db_client,
    io_handler_fixture_name,
    request: pytest.FixtureRequest,
    minimal_image,
    dummy_folder_name,
):
    io_handler = request.getfixturevalue(io_handler_fixture_name)
    media_vault = MediaVault(
        data_io_handler=io_handler,
        cache_io_handler=io_handler,
        db_client=db_client,
        thumbnail_width_in_pixels=200,
        chunk_byte_size=8192,
    )

    png_file_key = f"{uuid.uuid4()}.png"

    await io_handler.upload(
        libression.entities.io.FileStreamInfos(
            file_streams={
                png_file_key: libression.entities.io.FileStreamInfo(
                    file_stream=io.BytesIO(minimal_image),
                    mime_type=libression.entities.media.SupportedMimeType.PNG,
                ),
                f"{dummy_folder_name}/{png_file_key}": libression.entities.io.FileStreamInfo(
                    file_stream=io.BytesIO(minimal_image),  # fresh bytesIO copy
                    mime_type=libression.entities.media.SupportedMimeType.PNG,
                ),
            }
        )
    )

    original_files_info = await media_vault.get_files_info(
        [png_file_key, f"{dummy_folder_name}/{png_file_key}"]
    )

    # Act
    result = await media_vault.copy(
        [
            libression.entities.io.FileKeyMapping(
                source_key=png_file_key,
                destination_key=f"{dummy_folder_name}/{dummy_folder_name}/{png_file_key}",
            ),
            libression.entities.io.FileKeyMapping(
                source_key=f"{dummy_folder_name}/{png_file_key}",
                destination_key=f"{dummy_folder_name}/{dummy_folder_name}/{dummy_folder_name}/{png_file_key}",
            ),
        ],
        delete_source=True,  # Move
    )

    # Assert
    assert len([x for x in result if x.success]) == 2
    assert len([x for x in result if x.error is not None]) == 0

    # Verify DB client called with correct deletion entries
    original_entries = db_client.get_file_entries_by_file_keys(
        [png_file_key, f"{dummy_folder_name}/{png_file_key}"]
    )  # Original entries
    assert len(original_entries) == 0  # deleted original

    destination_keys = [
        f"{dummy_folder_name}/{dummy_folder_name}/{png_file_key}",
        f"{dummy_folder_name}/{dummy_folder_name}/{dummy_folder_name}/{png_file_key}",
    ]

    copied_entries = db_client.get_file_entries_by_file_keys(
        destination_keys
    )  # New entries
    assert len(copied_entries) == 2
    assert all(
        x.action_type == libression.entities.db.DBFileAction.MOVE
        for x in copied_entries
    )
    assert all(x.thumbnail_key is not None for x in copied_entries)
    # New file_entity_uuid created (new copies)
    assert (
        len(
            set([x.file_entity_uuid for x in original_files_info]).intersection(
                set([x.file_entity_uuid for x in copied_entries])
            )
        )
        == 2
    )

    # Teardown
    await media_vault.delete(copied_entries)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "io_handler_fixture_name, delete_source, minimal_image, has_data_in_io, has_cache_in_io, original_count,success_count, overlap_file_entity_uuid_count",
    [
        # 1 good entry + the following (problematic)
        ("docker_webdav_io_handler", True, "png", True, False, 0, 1 + 1, 2),  # MOVE
        ("docker_webdav_io_handler", True, "png", False, True, 0, 1, 1),
        ("docker_webdav_io_handler", True, "png", False, False, 0, 1, 1),
        ("docker_webdav_io_handler", False, "png", True, False, 2, 1 + 1, 0),  # COPY
        ("docker_webdav_io_handler", False, "png", False, True, 1, 1, 0),
        ("docker_webdav_io_handler", False, "png", False, False, 1, 1, 0),
    ],
    indirect=["minimal_image"],
)
async def test_copy_files_with_missing_files(
    db_client,
    io_handler_fixture_name,
    request: pytest.FixtureRequest,
    minimal_image,
    delete_source,
    has_data_in_io,
    has_cache_in_io,
    original_count,
    success_count,
    overlap_file_entity_uuid_count,
    dummy_folder_name,
):
    io_handler = request.getfixturevalue(io_handler_fixture_name)
    media_vault = MediaVault(
        data_io_handler=io_handler,
        cache_io_handler=io_handler,
        db_client=db_client,
        thumbnail_width_in_pixels=200,
        chunk_byte_size=8192,
    )

    png_file_key = f"{dummy_folder_name}/{uuid.uuid4()}.png"
    missing_files_key = f"{dummy_folder_name}/{uuid.uuid4()}_missing_files.png"

    await io_handler.upload(
        libression.entities.io.FileStreamInfos(
            file_streams={
                png_file_key: libression.entities.io.FileStreamInfo(
                    file_stream=io.BytesIO(minimal_image),
                    mime_type=libression.entities.media.SupportedMimeType.PNG,
                ),
                missing_files_key: libression.entities.io.FileStreamInfo(
                    file_stream=io.BytesIO(minimal_image),  # fresh bytesIO copy
                    mime_type=libression.entities.media.SupportedMimeType.PNG,
                ),
            }
        )
    )

    original_files_info = await media_vault.get_files_info(
        [png_file_key, missing_files_key]
    )  # render/register to db

    # delete from io (without updating db) ... like external action
    if not has_data_in_io:
        await media_vault.data_io_handler.delete([original_files_info[1].file_key])
    if not has_cache_in_io:
        await media_vault.cache_io_handler.delete(
            [original_files_info[1].thumbnail_key or ""]  # "" just to shut lint up
        )

    # Act
    result = await media_vault.copy(
        [
            libression.entities.io.FileKeyMapping(
                source_key=png_file_key,
                destination_key=f"{dummy_folder_name}/{png_file_key}",
            ),
            libression.entities.io.FileKeyMapping(
                source_key=missing_files_key,
                destination_key=f"{dummy_folder_name}/{missing_files_key}",
            ),
        ],
        delete_source=delete_source,
    )

    # Assert
    assert len([x for x in result if x.success]) == success_count
    assert len([x for x in result if x.error is not None]) == 2 - success_count

    # Verify DB
    original_entries = db_client.get_file_entries_by_file_keys(
        [png_file_key, missing_files_key]
    )  # Original entries
    assert len(original_entries) == original_count

    copied_entries = db_client.get_file_entries_by_file_keys(
        [
            f"{dummy_folder_name}/{png_file_key}",  # new good entry
            f"{dummy_folder_name}/{missing_files_key}",  # new problematic entry
        ]
    )  # New entries
    assert len(copied_entries) == success_count
    assert all(x.thumbnail_key is not None for x in copied_entries)
    # New file_entity_uuid created (new copies)
    assert (
        len(
            set([x.file_entity_uuid for x in original_files_info]).intersection(
                set([x.file_entity_uuid for x in copied_entries])
            )
        )
        == overlap_file_entity_uuid_count  # successfully moved files have same file_entity_uuid (failed not in list)
    )

    # Teardown
    await media_vault.delete(copied_entries)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "io_handler_fixture_name, minimal_image, has_data_in_io, has_cache_in_io, success_count",
    [
        # 1 good entry + the following (problematic)
        ("docker_webdav_io_handler", "png", True, False, 1 + 1),
        ("docker_webdav_io_handler", "png", False, True, 1),
        ("docker_webdav_io_handler", "png", False, False, 1),
    ],
    indirect=["minimal_image"],
)
async def test_move_copy_with_missing_files(
    db_client,
    io_handler_fixture_name,
    request: pytest.FixtureRequest,
    minimal_image,
    has_data_in_io,
    has_cache_in_io,
    dummy_folder_name,
    success_count,
):
    io_handler = request.getfixturevalue(io_handler_fixture_name)
    media_vault = MediaVault(
        data_io_handler=io_handler,
        cache_io_handler=io_handler,
        db_client=db_client,
        thumbnail_width_in_pixels=200,
        chunk_byte_size=8192,
    )

    png_file_key = f"{dummy_folder_name}/{uuid.uuid4()}.png"
    missing_files_key = f"{dummy_folder_name}/{uuid.uuid4()}_missing_files.png"

    await io_handler.upload(
        libression.entities.io.FileStreamInfos(
            file_streams={
                png_file_key: libression.entities.io.FileStreamInfo(
                    file_stream=io.BytesIO(minimal_image),
                    mime_type=libression.entities.media.SupportedMimeType.PNG,
                ),
                missing_files_key: libression.entities.io.FileStreamInfo(
                    file_stream=io.BytesIO(minimal_image),  # fresh bytesIO copy
                    mime_type=libression.entities.media.SupportedMimeType.PNG,
                ),
            }
        )
    )

    original_files_info = await media_vault.get_files_info(
        [png_file_key, missing_files_key]
    )  # render/register to db

    # delete from io (without updating db) ... like external action
    if not has_data_in_io:
        await media_vault.data_io_handler.delete([original_files_info[1].file_key])
    if not has_cache_in_io:
        await media_vault.cache_io_handler.delete(
            [original_files_info[1].thumbnail_key or ""]  # "" just to shut lint up
        )

    # Act
    result = await media_vault.copy(
        [
            libression.entities.io.FileKeyMapping(
                source_key=png_file_key,
                destination_key=f"{dummy_folder_name}/{png_file_key}",
            ),
            libression.entities.io.FileKeyMapping(
                source_key=missing_files_key,
                destination_key=f"{dummy_folder_name}/{missing_files_key}",
            ),
        ],
        delete_source=True,  # Move
    )

    # Assert
    assert len([x for x in result if x.success]) == success_count
    assert len([x for x in result if x.error is not None]) == 2 - success_count

    # Verify DB client called with correct deletion entries
    original_entries = db_client.get_file_entries_by_file_keys(
        [png_file_key, missing_files_key]
    )  # Original entries
    assert len(original_entries) == 0  # deleted original

    copied_entries = db_client.get_file_entries_by_file_keys(
        [
            f"{dummy_folder_name}/{png_file_key}",
            f"{dummy_folder_name}/{missing_files_key}",
        ]
    )  # New entries
    assert len(copied_entries) == success_count
    assert all(
        x.action_type == libression.entities.db.DBFileAction.MOVE
        for x in copied_entries
    )
    assert all(x.thumbnail_key is not None for x in copied_entries)
    # New file_entity_uuid created (new copies)
    assert (
        len(
            set([x.file_entity_uuid for x in original_files_info]).intersection(
                set([x.file_entity_uuid for x in copied_entries])
            )
        )
        == success_count  # successfully moved files have same file_entity_uuid (failed not in list)
    )

    # Teardown
    await media_vault.delete(copied_entries)


@pytest.mark.asyncio
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "io_handler_fixture_name, minimal_image",
    [
        ("docker_webdav_io_handler", "png"),
    ],
    indirect=["minimal_image"],
)
async def test_get_presigned_urls(
    db_client,
    io_handler_fixture_name,
    request: pytest.FixtureRequest,
    minimal_image,
    dummy_folder_name,
):
    io_handler = request.getfixturevalue(io_handler_fixture_name)
    media_vault = MediaVault(
        data_io_handler=io_handler,
        cache_io_handler=io_handler,
        db_client=db_client,
        thumbnail_width_in_pixels=200,
        chunk_byte_size=8192,
    )

    png_file_key = f"{uuid.uuid4()}.png"

    await io_handler.upload(
        libression.entities.io.FileStreamInfos(
            file_streams={
                png_file_key: libression.entities.io.FileStreamInfo(
                    file_stream=io.BytesIO(minimal_image),
                    mime_type=libression.entities.media.SupportedMimeType.PNG,
                ),
                f"{dummy_folder_name}/{png_file_key}": libression.entities.io.FileStreamInfo(
                    file_stream=io.BytesIO(minimal_image),  # fresh bytesIO copy
                    mime_type=libression.entities.media.SupportedMimeType.PNG,
                ),
            }
        )
    )

    original_files_info = await media_vault.get_files_info(
        [png_file_key, f"{dummy_folder_name}/{png_file_key}"]
    )

    # Act
    data_presigned_response = media_vault.get_data_presigned_urls(
        [x.file_key for x in original_files_info]
    )

    thumbnail_presigned_response = media_vault.get_thumbnail_presigned_urls(
        [x.thumbnail_key for x in original_files_info if x.thumbnail_key is not None]
    )

    # Assert
    assert (
        len(data_presigned_response.paths)
        == len(thumbnail_presigned_response.paths)
        == 2
    )  # 2 files + thumbnails

    data_url_responses = [
        httpx.get(
            f"{data_presigned_response.base_url}/{data_presigned_response.paths[x]}",
            verify=False,
        )
        for x in data_presigned_response.paths.keys()
    ]
    thumbnail_url_responses = [
        httpx.get(
            f"{thumbnail_presigned_response.base_url}/{thumbnail_presigned_response.paths[x]}",
            verify=False,
        )
        for x in thumbnail_presigned_response.paths.keys()
    ]

    assert all(x.status_code == 200 for x in data_url_responses)
    assert all(x.status_code == 200 for x in thumbnail_url_responses)

    assert all(x.content is not None for x in data_url_responses)
    assert all(x.content is not None for x in thumbnail_url_responses)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "io_handler_fixture_name, minimal_image",
    [
        ("docker_webdav_io_handler", "png"),
    ],
    indirect=["minimal_image"],
)
async def test_upload_media(
    db_client,
    io_handler_fixture_name,
    request: pytest.FixtureRequest,
    minimal_image,
    dummy_folder_name,
):
    io_handler = request.getfixturevalue(io_handler_fixture_name)
    media_vault = MediaVault(
        data_io_handler=io_handler,
        cache_io_handler=io_handler,
        db_client=db_client,
        thumbnail_width_in_pixels=200,
        chunk_byte_size=8192,
    )

    # Prepare test files
    upload_entries = [
        libression.entities.base.UploadEntry(
            filename="test1.png",
            file_source=base64.b64encode(minimal_image).decode("utf-8"),
        ),
        libression.entities.base.UploadEntry(
            filename="test2.png",
            file_source=base64.b64encode(minimal_image).decode("utf-8"),
        ),
    ]

    # Act
    result = await media_vault.upload_media(
        upload_entries=upload_entries,
        target_dir_key=dummy_folder_name,
        max_concurrent_uploads=10,
    )

    # Assert
    assert len(result) == 2
    for entry in result:
        assert entry.file_key.startswith(f"{dummy_folder_name}/")
        assert entry.thumbnail_key is not None
        assert entry.thumbnail_mime_type == "image/jpeg"
        assert entry.thumbnail_checksum is not None
        assert entry.thumbnail_phash is not None
        assert entry.action_type == libression.entities.db.DBFileAction.CREATE

    # Verify files exist in storage
    data_urls = media_vault.get_data_presigned_urls(
        [entry.file_key for entry in result]
    )
    thumbnail_urls = media_vault.get_thumbnail_presigned_urls(
        [entry.thumbnail_key for entry in result if entry.thumbnail_key is not None]
    )

    assert len(data_urls.paths) == len(thumbnail_urls.paths) == 2

    # Verify URLs are accessible
    for url in [*data_urls.paths.values(), *thumbnail_urls.paths.values()]:
        response = httpx.get(
            f"{data_urls.base_url}/{url}",
            verify=False,
        )
        assert response.status_code == 200
        assert response.content is not None

    # Cleanup
    await media_vault.delete(result)


# TODO: add:
# - test upload_media
# - test delete_media (MISSING file in io)
# - test copy_media (MISSING file in io)
# - test move_media (MISSING file in io)
