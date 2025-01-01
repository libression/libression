import pytest
import os
import io
import uuid
from libression.entities.io import FileStreams, FileStream, FileKeyMapping
from libression.io_handler.in_memory import InMemoryIOHandler
from libression.io_handler.webdav import WebDAVIOHandler


TEST_DATA = b"Hello Test!"

WEBDAV_USER = os.environ.get("WEBDAV_USER", "libression_user")
WEBDAV_PASSWORD = os.environ.get("WEBDAV_PASSWORD", "libression_password")
NGINX_SECURE_LINK_KEY = os.environ.get("NGINX_SECURE_LINK_KEY", "libression_secret_key")


@pytest.fixture
def file_key():
    return f"{uuid.uuid4()}.txt"


@pytest.fixture
def folder_name():
    return str(uuid.uuid4())


@pytest.fixture
def test_file_stream():
    return FileStream(
        file_stream=io.BytesIO(TEST_DATA),
        file_byte_size=len(TEST_DATA),
    )


def docker_webdav_handler():
    # TODO Only works if docker compose is running ... configure this...
    return WebDAVIOHandler(
        base_url="https://localhost:8443",  # Updated port
        username=WEBDAV_USER,
        password=WEBDAV_PASSWORD,
        secret_key=NGINX_SECURE_LINK_KEY,
        url_path="dummy_photos",
        presigned_url_path="readonly_dummy_photos",
        verify_ssl=False,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler", [InMemoryIOHandler(), docker_webdav_handler()])
async def test_upload_and_list(io_handler, file_key, test_file_stream):
    try:
        # Test upload
        await io_handler.upload(FileStreams(file_streams={file_key: test_file_stream}))

        # Verify upload
        objects = await io_handler.list_objects()
        found_files = [x for x in objects if x.filename == file_key]
        assert len(found_files) == 1
        assert found_files[0].size == len(TEST_DATA)
    finally:
        # Cleanup: delete test file
        await io_handler.delete([file_key], raise_on_error=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler", [InMemoryIOHandler(), docker_webdav_handler()])
async def test_nested_upload(io_handler, file_key, folder_name, test_file_stream):
    nested_key = f"{folder_name}/{file_key}"
    try:
        await io_handler.upload(
            FileStreams(file_streams={nested_key: test_file_stream})
        )

        objects = await io_handler.list_objects(folder_name)
        found_files = [x for x in objects if x.absolute_path == nested_key]
        assert len(found_files) == 1
        assert found_files[0].size == len(TEST_DATA)
    finally:
        # Cleanup: delete test file
        await io_handler.delete([nested_key], raise_on_error=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler", [InMemoryIOHandler(), docker_webdav_handler()])
async def test_get_readonly_urls(io_handler, file_key, test_file_stream):
    # Upload file first
    await io_handler.upload(FileStreams(file_streams={file_key: test_file_stream}))

    # Test URL generation
    urls = io_handler.get_readonly_urls([file_key], expires_in_seconds=3600)
    assert file_key in urls.urls
    assert "://" in urls.urls[file_key]  # protocol is present


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler", [InMemoryIOHandler(), docker_webdav_handler()])
async def test_delete(io_handler, file_key, test_file_stream):
    # Upload file first
    await io_handler.upload(FileStreams(file_streams={file_key: test_file_stream}))

    # Test delete
    await io_handler.delete([file_key])
    objects = await io_handler.list_objects()
    assert len([x for x in objects if x.filename == file_key]) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler", [InMemoryIOHandler(), docker_webdav_handler()])
async def test_delete_missing_file(io_handler):
    # Should not raise when raise_on_error is False
    await io_handler.delete(["non_existent.txt"], raise_on_error=False)

    # Should raise when raise_on_error is True
    with pytest.raises(Exception):
        await io_handler.delete(["non_existent.txt"], raise_on_error=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler", [InMemoryIOHandler(), docker_webdav_handler()])
async def test_copy(io_handler, file_key, folder_name, test_file_stream):
    nested_key = f"{folder_name}/{file_key}"
    try:
        # Upload initial file
        await io_handler.upload(FileStreams(file_streams={file_key: test_file_stream}))

        # Test copy
        await io_handler.copy(
            [FileKeyMapping(source_key=file_key, destination_key=nested_key)],
            delete_source=False,
        )

        # Verify both files exist
        objects = await io_handler.list_objects(subfolder_contents=True)
        source_files = [x for x in objects if x.absolute_path == file_key]
        dest_files = [x for x in objects if x.absolute_path == nested_key]

        assert len(source_files) == 1, "Source file should exist"
        assert len(dest_files) == 1, "Destination file should exist"
        assert source_files[0].size == len(TEST_DATA)
        assert dest_files[0].size == len(TEST_DATA)
    finally:
        # Cleanup: delete both source and destination files
        await io_handler.delete([file_key, nested_key], raise_on_error=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler", [InMemoryIOHandler(), docker_webdav_handler()])
async def test_move(io_handler, file_key, test_file_stream):
    new_key = f"moved_{file_key}"
    try:
        # Upload initial file
        await io_handler.upload(FileStreams(file_streams={file_key: test_file_stream}))

        # Test move
        await io_handler.copy(
            [FileKeyMapping(source_key=file_key, destination_key=new_key)],
            delete_source=True,
        )

        objects = await io_handler.list_objects()
        assert len([x for x in objects if x.absolute_path == file_key]) == 0
        assert len([x for x in objects if x.absolute_path == new_key]) == 1
    finally:
        # Cleanup
        await io_handler.delete([new_key], raise_on_error=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler", [InMemoryIOHandler(), docker_webdav_handler()])
async def test_copy_with_overwrite(io_handler, file_key, folder_name, test_file_stream):
    nested_key = f"{folder_name}/{file_key}"
    try:
        # Upload initial file
        await io_handler.upload(FileStreams(file_streams={file_key: test_file_stream}))

        # First copy should succeed
        await io_handler.copy(
            [FileKeyMapping(source_key=file_key, destination_key=nested_key)],
            delete_source=False,
            overwrite_existing=False,
        )

        # Second copy with overwrite=False should fail
        with pytest.raises(Exception):  # or more specific exception
            await io_handler.copy(
                [FileKeyMapping(source_key=file_key, destination_key=nested_key)],
                delete_source=False,
                overwrite_existing=False,
            )

        # Second copy with overwrite=True should succeed
        await io_handler.copy(
            [FileKeyMapping(source_key=file_key, destination_key=nested_key)],
            delete_source=False,
            overwrite_existing=True,
        )

    finally:
        # Cleanup
        await io_handler.delete([file_key, nested_key], raise_on_error=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler", [InMemoryIOHandler(), docker_webdav_handler()])
async def test_move_with_overwrite(io_handler, file_key, test_file_stream):
    new_key = f"moved_{file_key}"
    try:
        # Upload initial files
        await io_handler.upload(
            FileStreams(
                file_streams={
                    file_key: test_file_stream,
                    new_key: test_file_stream,  # Create destination file
                }
            )
        )

        # Move with overwrite=False should fail
        with pytest.raises(Exception):
            await io_handler.copy(
                [FileKeyMapping(source_key=file_key, destination_key=new_key)],
                delete_source=True,
                overwrite_existing=False,
            )

        # Move with overwrite=True should succeed
        await io_handler.copy(
            [FileKeyMapping(source_key=file_key, destination_key=new_key)],
            delete_source=True,
            overwrite_existing=True,
        )

        objects = await io_handler.list_objects()
        assert len([x for x in objects if x.absolute_path == file_key]) == 0
        assert len([x for x in objects if x.absolute_path == new_key]) == 1

    finally:
        # Cleanup
        await io_handler.delete([new_key], raise_on_error=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler", [InMemoryIOHandler(), docker_webdav_handler()])
async def test_list_objects_with_nested_paths(
    io_handler, file_key, folder_name, test_file_stream
):
    root_key = file_key
    nested_key = f"{folder_name}/{file_key}"
    try:
        # Upload both files
        await io_handler.upload(
            FileStreams(
                file_streams={root_key: test_file_stream, nested_key: test_file_stream}
            )
        )

        # Test listing all objects
        all_objects = await io_handler.list_objects(subfolder_contents=True)
        root_files = [x for x in all_objects if x.absolute_path == root_key]
        nested_files = [x for x in all_objects if x.absolute_path == nested_key]

        assert (
            len(root_files) == 1
        ), f"Root file not found in: {[x.absolute_path for x in all_objects]}"
        assert (
            len(nested_files) == 1
        ), f"Nested file not found in: {[x.absolute_path for x in all_objects]}"

        # Test listing folder specifically
        folder_objects = await io_handler.list_objects(
            folder_name, subfolder_contents=True
        )
        folder_files = [x for x in folder_objects if x.absolute_path == nested_key]
        assert (
            len(folder_files) == 1
        ), f"Nested file not found in folder: {[x.absolute_path for x in folder_objects]}"

    finally:
        await io_handler.delete([root_key, nested_key], raise_on_error=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler", [InMemoryIOHandler(), docker_webdav_handler()])
async def test_list_objects_single_vs_recursive(
    io_handler, file_key, folder_name, test_file_stream
):
    root_key = file_key
    nested_key = f"{folder_name}/{file_key}"
    nested_subfolder_key = f"{folder_name}/subfolder/{file_key}"

    try:
        # Upload files in different directory levels
        await io_handler.upload(
            FileStreams(
                file_streams={
                    root_key: test_file_stream,
                    nested_key: test_file_stream,
                    nested_subfolder_key: test_file_stream,
                }
            )
        )

        # Test single directory listing (non-recursive)
        root_objects = await io_handler.list_objects(subfolder_contents=False)
        assert (
            len([x for x in root_objects if x.absolute_path == root_key]) == 1
        ), "Root file should be listed"
        assert (
            len([x for x in root_objects if x.is_dir and x.filename == folder_name])
            == 1
        ), "Folder should be listed"
        assert (
            len([x for x in root_objects if x.absolute_path == nested_key]) == 0
        ), "Nested file should not be listed"

        folder_objects = await io_handler.list_objects(
            folder_name, subfolder_contents=False
        )
        assert (
            len([x for x in folder_objects if x.absolute_path == nested_key]) == 1
        ), "Nested file should be listed"
        assert (
            len([x for x in folder_objects if x.is_dir and x.filename == "subfolder"])
            == 1
        ), "Subfolder should be listed"
        assert (
            len([x for x in folder_objects if x.absolute_path == nested_subfolder_key])
            == 0
        ), "Deeply nested file should not be listed"

        # Test recursive listing
        all_objects = await io_handler.list_objects(subfolder_contents=True)
        assert (
            len([x for x in all_objects if x.absolute_path == root_key]) == 1
        ), "Root file should be listed"
        assert (
            len([x for x in all_objects if x.absolute_path == nested_key]) == 1
        ), "Nested file should be listed"
        assert (
            len([x for x in all_objects if x.absolute_path == nested_subfolder_key])
            == 1
        ), "Deeply nested file should be listed"

        folder_objects_recursive = await io_handler.list_objects(
            folder_name, subfolder_contents=True
        )
        assert (
            len([x for x in folder_objects_recursive if x.absolute_path == nested_key])
            == 1
        ), "Nested file should be listed"
        assert (
            len(
                [
                    x
                    for x in folder_objects_recursive
                    if x.absolute_path == nested_subfolder_key
                ]
            )
            == 1
        ), "Deeply nested file should be listed"
        assert (
            len([x for x in folder_objects_recursive if x.absolute_path == root_key])
            == 0
        ), "Root file should not be listed"

    finally:
        await io_handler.delete(
            [root_key, nested_key, nested_subfolder_key], raise_on_error=False
        )
