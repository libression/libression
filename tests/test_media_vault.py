import pytest
import uuid
import io
import httpx
import libression.entities.io
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
        libression.entities.io.FileStreams(
            file_streams={
                png_file_key: libression.entities.io.FileStream(
                    file_stream=byte_stream,
                    mime_type=libression.entities.media.SupportedMimeType.PNG,
                    file_byte_size=len(minimal_image),
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
    await media_vault.delete([result[0]], raise_on_error=True)


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
        libression.entities.io.FileStreams(
            file_streams={
                png_file_key: libression.entities.io.FileStream(
                    file_stream=io.BytesIO(minimal_image),
                    mime_type=libression.entities.media.SupportedMimeType.PNG,
                    file_byte_size=len(minimal_image),
                ),
                f"{dummy_folder_name}/{png_file_key}": libression.entities.io.FileStream(
                    file_stream=io.BytesIO(minimal_image),  # fresh bytesIO copy
                    mime_type=libression.entities.media.SupportedMimeType.PNG,
                    file_byte_size=len(minimal_image),
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
        libression.entities.io.FileStreams(
            file_streams={
                png_file_key: libression.entities.io.FileStream(
                    file_stream=io.BytesIO(minimal_image),
                    mime_type=libression.entities.media.SupportedMimeType.PNG,
                    file_byte_size=len(minimal_image),
                ),
                f"{dummy_folder_name}/{png_file_key}": libression.entities.io.FileStream(
                    file_stream=io.BytesIO(minimal_image),  # fresh bytesIO copy
                    mime_type=libression.entities.media.SupportedMimeType.PNG,
                    file_byte_size=len(minimal_image),
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

    # Verify DB client called with correct deletion entries
    original_entries = db_client.get_file_entries_by_file_keys(
        [png_file_key, f"{dummy_folder_name}/{png_file_key}"]
    )  # Original entries
    assert len(original_entries) == 2  # didn't delete original

    copied_entries = db_client.get_file_entries_by_file_keys(
        [x.file_key for x in result]
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
    await media_vault.delete(copied_entries, raise_on_error=True)
    await media_vault.delete(original_entries, raise_on_error=True)


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
        libression.entities.io.FileStreams(
            file_streams={
                png_file_key: libression.entities.io.FileStream(
                    file_stream=io.BytesIO(minimal_image),
                    mime_type=libression.entities.media.SupportedMimeType.PNG,
                    file_byte_size=len(minimal_image),
                ),
                f"{dummy_folder_name}/{png_file_key}": libression.entities.io.FileStream(
                    file_stream=io.BytesIO(minimal_image),  # fresh bytesIO copy
                    mime_type=libression.entities.media.SupportedMimeType.PNG,
                    file_byte_size=len(minimal_image),
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

    # Verify DB client called with correct deletion entries
    original_entries = db_client.get_file_entries_by_file_keys(
        [png_file_key, f"{dummy_folder_name}/{png_file_key}"]
    )  # Original entries
    assert len(original_entries) == 0  # deleted original

    copied_entries = db_client.get_file_entries_by_file_keys(
        [x.file_key for x in result]
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
    await media_vault.delete(copied_entries, raise_on_error=True)


#### IGNORE BELOW


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
        libression.entities.io.FileStreams(
            file_streams={
                png_file_key: libression.entities.io.FileStream(
                    file_stream=io.BytesIO(minimal_image),
                    mime_type=libression.entities.media.SupportedMimeType.PNG,
                    file_byte_size=len(minimal_image),
                ),
                f"{dummy_folder_name}/{png_file_key}": libression.entities.io.FileStream(
                    file_stream=io.BytesIO(minimal_image),  # fresh bytesIO copy
                    mime_type=libression.entities.media.SupportedMimeType.PNG,
                    file_byte_size=len(minimal_image),
                ),
            }
        )
    )

    original_files_info = await media_vault.get_files_info(
        [png_file_key, f"{dummy_folder_name}/{png_file_key}"]
    )

    # Act
    data_presigned_urls = media_vault.get_data_presigned_urls(
        [x.file_key for x in original_files_info]
    )

    thumbnail_presigned_urls = media_vault.get_thumbnail_presigned_urls(
        [x.thumbnail_key for x in original_files_info if x.thumbnail_key is not None]
    )

    # Assert
    assert (
        len(data_presigned_urls.urls) == len(thumbnail_presigned_urls.urls) == 2
    )  # 2 files + thumbnails

    data_url_responses = [
        httpx.get(x, verify=False) for x in data_presigned_urls.urls.values()
    ]
    thumbnail_url_responses = [
        httpx.get(x, verify=False) for x in thumbnail_presigned_urls.urls.values()
    ]

    assert all(x.status_code == 200 for x in data_url_responses)
    assert all(x.status_code == 200 for x in thumbnail_url_responses)

    assert all(x.content is not None for x in data_url_responses)
    assert all(x.content is not None for x in thumbnail_url_responses)


# TODO: add:
# - test upload_media
# - test delete_media (MISSING file in io)
# - test copy_media (MISSING file in io)
# - test move_media (MISSING file in io)
