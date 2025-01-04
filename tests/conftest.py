import os
import pytest
import libression.db.client
import libression.entities.io
import libression.config
import libression.io_handler.webdav
import uuid


@pytest.fixture
def dummy_file_key():
    return f"{uuid.uuid4()}.txt"


@pytest.fixture
def dummy_folder_name():
    return str(uuid.uuid4())


###########################################################################
# IO handler fixtures
###########################################################################


@pytest.fixture
def docker_webdav_io_handler():
    # TODO Only works if docker compose is running ... configure this...
    # Async to keep same interface as in_memory_io_handler
    handler = libression.io_handler.webdav.WebDAVIOHandler(
        base_url="https://localhost:8443",  # Updated port
        url_path="dummy_photos",
        presigned_url_path="readonly_dummy_photos",
        verify_ssl=False,
    )  # Default credentials are set in the WebDAVIOHandler class
    yield handler


###########################################################################
# Data fixtures
###########################################################################


@pytest.fixture
def minimal_image(request) -> bytes:
    """
    Fixture for loading test images in different formats.
    Usage:
        @pytest.mark.parametrize("dark_square_image", ["jpeg"], indirect=True)
        def test_jpeg_only(dark_square_image):
            ...
    """
    file_format = request.param  # raise if not provided
    filepath = os.path.join(
        os.path.dirname(__file__),
        "fixtures",
        f"minimal.{file_format}",
    )
    with open(filepath, "rb") as f:
        content = f.read()
    return content


###########################################################################
# DB fixtures
###########################################################################


@pytest.fixture(scope="function")
def db_client(tmp_path):
    """Create a temporary database for testing."""
    db_path = tmp_path / "test.db"
    client = libression.db.client.DBClient(db_path)

    return client
