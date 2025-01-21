import asyncio
import io
import logging
import time
import uuid

import httpx

import libression.entities.io
import libression.io_handler.webdav

BASE_URL = "https://localhost"
URL_PATH = "libression_photos"
PRESIGNED_URL_PATH = "readonly_libression_photos"
USERNAME = "chilledgeek"
PASSWORD = "chilledgeek"
SECRET_KEY = "chilledgeek_secret_key"
CHUNK_BYTE_SIZE = 1024 * 1024 * 5  # 5MB

# Set test vars
TEST_DATA = b"Hello WebDAV!"
FILE_KEY = f"{uuid.uuid4()}.txt"
FOLDER_NAME = str(uuid.uuid4())


# Configure logging at the module level
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


async def manual_test_webdav():
    # Initialize handler (adjust these credentials for your WebDAV server)
    handler = libression.io_handler.webdav.WebDAVIOHandler(
        base_url=BASE_URL,
        username=USERNAME,
        password=PASSWORD,
        secret_key=SECRET_KEY,
        url_path=URL_PATH,
        presigned_url_path=PRESIGNED_URL_PATH,
        verify_ssl=False,
    )

    # Test upload
    test_file_stream1 = libression.entities.io.FileStreamInfo(
        file_stream=io.BytesIO(TEST_DATA),
    )
    test_file_stream2 = libression.entities.io.FileStreamInfo(
        file_stream=io.BytesIO(TEST_DATA),
    )
    test_file_streams = libression.entities.io.FileStreamInfos(
        file_streams={
            FILE_KEY: test_file_stream1,
            f"{FOLDER_NAME}/{FILE_KEY}": test_file_stream2,
        }
    )
    await handler.upload(test_file_streams, chunk_byte_size=CHUNK_BYTE_SIZE)

    # Test list directory
    objects = await handler.list_objects()

    found_root_files = [x for x in objects if x.filename == FILE_KEY]

    assert len(found_root_files) == 1
    assert not found_root_files[0].is_dir
    assert found_root_files[0].absolute_path == FILE_KEY
    assert found_root_files[0].size > 0

    found_nested_folders = [x for x in objects if x.filename == FOLDER_NAME]
    assert len(found_nested_folders) == 1
    assert found_nested_folders[0].is_dir
    assert found_nested_folders[0].absolute_path == FOLDER_NAME

    # Test list nested directory
    objects = await handler.list_objects(FOLDER_NAME)
    found_nested_files = [x for x in objects if x.filename == FILE_KEY]

    # Add debug logging
    logger.debug(f"Found nested files: {found_nested_files}")
    if found_nested_files:
        logger.debug(f"File size: {found_nested_files[0].size}")
        logger.debug(f"Original test data size: {len(TEST_DATA)}")

    assert len(found_nested_files) == 1
    assert not found_nested_files[0].is_dir
    assert found_nested_files[0].filename == FILE_KEY
    assert found_nested_files[0].absolute_path == f"{FOLDER_NAME}/{FILE_KEY}"

    # Change this assertion to be more specific
    assert found_nested_files[0].size == len(
        TEST_DATA
    ), f"Expected size {len(TEST_DATA)}, got {found_nested_files[0].size}"

    # Test get_url
    presigned_urls = handler.get_readonly_urls(
        [FILE_KEY, f"{FOLDER_NAME}/{FILE_KEY}"],
        expires_in_seconds=3600,
    )
    root_response = httpx.get(
        f"{presigned_urls.base_url}/{presigned_urls.paths[FILE_KEY]}", verify=False
    )
    nested_response = httpx.get(
        f"{presigned_urls.base_url}/{presigned_urls.paths[f"{FOLDER_NAME}/{FILE_KEY}"]}",
        verify=False,
    )

    assert root_response.content == TEST_DATA
    assert nested_response.content == TEST_DATA

    # Test get_url timeout
    timedout_presigned_urls = handler.get_readonly_urls(
        [FILE_KEY, f"{FOLDER_NAME}/{FILE_KEY}"],
        expires_in_seconds=0,
    )
    time.sleep(1.1)
    timedout_root_response = httpx.get(
        f"{timedout_presigned_urls.base_url}/{timedout_presigned_urls.paths[FILE_KEY]}",
        verify=False,
    )
    timedout_nested_response = httpx.get(
        f"{timedout_presigned_urls.base_url}/{timedout_presigned_urls.paths[f"{FOLDER_NAME}/{FILE_KEY}"]}",
        verify=False,
    )

    assert timedout_root_response.status_code == 410
    assert timedout_nested_response.status_code == 410

    # Test delete
    await handler.delete([f"{FOLDER_NAME}/{FILE_KEY}"])
    objects = await handler.list_objects()
    assert (
        len([x for x in objects if x.absolute_path == f"{FOLDER_NAME}/{FILE_KEY}"]) == 0
    )

    # Test move
    await handler.copy(
        [
            libression.entities.io.FileKeyMapping(
                source_key=FILE_KEY,
                destination_key=f"{FOLDER_NAME}/{FILE_KEY}",
            )
        ],
        delete_source=True,
    )
    objects = await handler.list_objects(FOLDER_NAME)
    assert (
        len([x for x in objects if x.absolute_path == f"{FOLDER_NAME}/{FILE_KEY}"]) == 1
    )
    assert len([x for x in objects if x.absolute_path == f"{FILE_KEY}"]) == 0

    # Test copy
    await handler.copy(
        [
            libression.entities.io.FileKeyMapping(
                source_key=f"{FOLDER_NAME}/{FILE_KEY}",
                destination_key=FILE_KEY,
            )
        ],
        delete_source=False,
    )
    objects = await handler.list_objects()
    assert len([x for x in objects if x.absolute_path == FILE_KEY]) == 1

    objects = await handler.list_objects(FOLDER_NAME)
    assert (
        len([x for x in objects if x.absolute_path == f"{FOLDER_NAME}/{FILE_KEY}"]) == 1
    )

    # Teardown
    await handler.delete([f"{FOLDER_NAME}/{FILE_KEY}", FILE_KEY])
    objects = await handler.list_objects()
    assert (
        len([x for x in objects if x.absolute_path == f"{FOLDER_NAME}/{FILE_KEY}"]) == 0
    )
    assert len([x for x in objects if x.absolute_path == f"{FILE_KEY}"]) == 0

    # Test get_url (deleted/not found)
    deleted_presigned_urls = handler.get_readonly_urls(
        [FILE_KEY, f"{FOLDER_NAME}/{FILE_KEY}"],
        expires_in_seconds=3600,
    )
    deleted_root_response = httpx.get(
        f"{deleted_presigned_urls.base_url}/{deleted_presigned_urls.paths[FILE_KEY]}",
        verify=False,
    )
    deleted_nested_response = httpx.get(
        f"{deleted_presigned_urls.base_url}/{deleted_presigned_urls.paths[f"{FOLDER_NAME}/{FILE_KEY}"]}",
        verify=False,
    )

    assert deleted_root_response.status_code == 404
    assert deleted_nested_response.status_code == 404

    print("Manual test passed")


if __name__ == "__main__":
    asyncio.run(manual_test_webdav())
