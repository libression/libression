import io
import uuid
import requests
from libression.entities.io import FileStreams, FileKeyMapping
from libression.io_handler.webdav import WebDAVIOHandler

BASE_URL = "https://localhost"
URL_PATH = "dummy_photos"
PRESIGNED_URL_PATH = "secure"
USERNAME = "chilledgeek"
PASSWORD = "chilledgeek"
SECRET_KEY = "chilledgeek_secret_key"


import logging

# Configure logging at the module level
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def manual_test_webdav():
    # Initialize handler (adjust these credentials for your WebDAV server)
    handler = WebDAVIOHandler(
        base_url=BASE_URL,
        username=USERNAME,
        password=PASSWORD,
        secret_key=SECRET_KEY,
        verify_ssl=False,  # Webdav should allow local only
        url_path=URL_PATH,
        presigned_url_path=PRESIGNED_URL_PATH,
    )

    # Test vars
    TEST_DATA = b"Hello WebDAV!"
    FILE_KEY = f"{uuid.uuid4()}.txt"
    FOLDER_NAME = str(uuid.uuid4())

    # Test upload
    test_file_streams = FileStreams(
        file_streams={
            FILE_KEY: io.BytesIO(TEST_DATA),
            f"{FOLDER_NAME}/{FILE_KEY}": io.BytesIO(TEST_DATA),
        }
    )
    handler.upload(test_file_streams)

    # Test list directory
    objects = handler.list_objects()

    found_root_files = [x for x in objects if x.filename == FILE_KEY]

    assert len(found_root_files) == 1
    assert found_root_files[0].is_dir == False
    assert found_root_files[0].absolute_path == FILE_KEY
    assert found_root_files[0].size > 0

    found_nested_folders = [x for x in objects if x.filename == FOLDER_NAME]
    assert len(found_nested_folders) == 1
    assert found_nested_folders[0].is_dir == True
    assert found_nested_folders[0].absolute_path == FOLDER_NAME

    # Test list nested directory
    objects = handler.list_objects(FOLDER_NAME)
    found_nested_files = [x for x in objects if x.filename == FILE_KEY]
    assert len(found_nested_files) == 1
    assert found_nested_files[0].is_dir == False
    assert found_nested_files[0].filename == FILE_KEY
    assert found_nested_files[0].absolute_path == f"{FOLDER_NAME}/{FILE_KEY}"
    assert found_nested_files[0].size > 0

    # Test download
    download_streams = handler.get([FILE_KEY, f"{FOLDER_NAME}/{FILE_KEY}"])
    assert download_streams.file_streams[FILE_KEY].read() == TEST_DATA
    assert download_streams.file_streams[f"{FOLDER_NAME}/{FILE_KEY}"].read() == TEST_DATA

    # Test get_url
    presigned_urls = handler.get_urls([FILE_KEY, f"{FOLDER_NAME}/{FILE_KEY}"])
    assert requests.get(presigned_urls.urls[FILE_KEY], verify=False).content == TEST_DATA
    assert requests.get(presigned_urls.urls[f"{FOLDER_NAME}/{FILE_KEY}"], verify=False).content == TEST_DATA

    # Test delete
    handler.delete([f"{FOLDER_NAME}/{FILE_KEY}"])
    objects = handler.list_objects()
    assert len([x for x in objects if x.absolute_path == f"{FOLDER_NAME}/{FILE_KEY}"]) == 0

    # Test move
    handler.move(
        [
            FileKeyMapping(
                source_key=FILE_KEY,
                destination_key=f"{FOLDER_NAME}/{FILE_KEY}",
            )
        ]
    )
    objects = handler.list_objects(FOLDER_NAME)
    assert len([x for x in objects if x.absolute_path == f"{FOLDER_NAME}/{FILE_KEY}"]) == 1
    assert len([x for x in objects if x.absolute_path == f"FILE_KEY"]) == 0

    # Test copy
    handler.copy(
        [
            FileKeyMapping(
                source_key=f"{FOLDER_NAME}/{FILE_KEY}",
                destination_key=FILE_KEY,
            )
        ]
    )
    objects = handler.list_objects()
    assert len([x for x in objects if x.absolute_path == FILE_KEY]) == 1

    objects = handler.list_objects(FOLDER_NAME)
    assert len([x for x in objects if x.absolute_path == f"{FOLDER_NAME}/{FILE_KEY}"]) == 1


    # Teardown
    handler.delete([f"{FOLDER_NAME}/{FILE_KEY}", FILE_KEY])
    objects = handler.list_objects()
    assert len([x for x in objects if x.absolute_path == f"{FOLDER_NAME}/{FILE_KEY}"]) == 0
    assert len([x for x in objects if x.absolute_path == f"FILE_KEY"]) == 0

    print("Manual test passed")

if __name__ == "__main__":
    manual_test_webdav()
