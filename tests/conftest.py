import os
import pytest
import libression.db.client


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


@pytest.fixture
def db_client(tmp_path):
    """Create a temporary database for testing."""
    db_path = tmp_path / "test.db"
    client = libression.db.client.DBClient(db_path)
    yield client
    # Clean up
    if db_path.exists():
        db_path.unlink()
