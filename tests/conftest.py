import os
import pytest


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
