import io
import uuid

from libression.entities.io import FileStreams
from libression.io_handler.webdav import WebDAVIOHandler

BASE_URL = "https://localhost/photos_to_print_copy/"
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
    )

    # Test vars
    TEST_DATA = b"Hello WebDAV!"
    FILE_KEY = f"{uuid.uuid4()}.txt"
    FOLDER_NAME = str(uuid.uuid4())

    # Test upload
    file_streams = FileStreams(
        file_streams={
            FILE_KEY: io.BytesIO(TEST_DATA),
            f"{FOLDER_NAME}/{FILE_KEY}": io.BytesIO(TEST_DATA),
        }
    )
    handler.upload(file_streams)

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

    ##############################################
    # TODO CONTINUE TESTing...rest is a bit broken...
    ##############################################

    print("\n3. Testing bytestream (download)...")
    stream = handler.bytestream(test_key)
    content = stream.read()
    print(f"✓ Downloaded content: {content}")
    assert content == test_data, "Content mismatch!"

    print("\n4. Testing copy...")
    copy_key = "test-file-copy.txt"
    handler.copy(test_key, copy_key)
    print("✓ Copy successful")

    print("\n5. Testing move...")
    move_key = "test-file-moved.txt"
    handler.move(copy_key, move_key)
    print("✓ Move successful")

    print("\n6. Testing delete...")
    handler.delete(test_key)
    handler.delete(move_key)
    print("✓ Delete successful")

    print("\nAll tests passed! 🎉")


if __name__ == "__main__":
    manual_test_webdav()
