import os
import pytest
import libression.db.client
import libression.entities.io
import libression.config
import libression.io_handler.webdav
import uuid
import threading
import pathlib
import time
from aiohttp import web
import asyncio
import mimetypes


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
        base_url=libression.config.WEBDAV_BASE_URL,  # Updated port
        url_path=libression.config.WEBDAV_URL_PATH,
        presigned_url_path=libression.config.WEBDAV_PRESIGNED_URL_PATH,
        verify_ssl=False,
    )  # Default credentials are set in the WebDAVIOHandler class
    yield handler


###########################################################################
# Data fixtures
###########################################################################


@pytest.fixture
def media_fixture_by_filename(request) -> bytes:
    """
    Fixture for loading test images in different formats.
    Usage:
        @pytest.mark.parametrize("dark_square_image", ["jpeg"], indirect=True)
        def test_jpeg_only(dark_square_image):
            ...
    """
    file_name = request.param  # raise if not provided
    filepath = os.path.join(
        os.path.dirname(__file__),
        "fixtures",
        file_name,
    )
    with open(filepath, "rb") as f:
        content = f.read()
    return content


@pytest.fixture
def short_mp4_video() -> bytes:
    filepath = os.path.join(
        os.path.dirname(__file__),
        "fixtures",
        "short.mp4",
    )
    with open(filepath, "rb") as f:
        content = f.read()
    return content


class _TestHttpFileServer:
    def __init__(self, port=8123):
        self.port = port
        self.directory = pathlib.Path(__file__).parent / "fixtures"
        self._server = None
        self._runner = None
        self._thread = None
        self._loop = None

    async def handle_file(self, request):
        filename = request.match_info["filename"]
        filepath = self.directory / filename

        if not filepath.exists():
            return web.Response(status=404)

        # Get file size and mime type
        file_size = filepath.stat().st_size
        content_type = mimetypes.guess_type(filepath)[0] or "application/octet-stream"

        # Handle range requests
        range_header = request.headers.get("Range")
        start = 0
        end = file_size - 1

        if range_header:
            try:
                range_match = range_header.replace("bytes=", "").split("-")
                start = int(range_match[0]) if range_match[0] else 0
                end = int(range_match[1]) if range_match[1] else file_size - 1
            except (ValueError, IndexError):
                pass

        # Prepare response headers
        headers = {
            "Content-Type": content_type,
            "Content-Length": str(end - start + 1),
            "Accept-Ranges": "bytes",
            "Content-Range": f"bytes {start}-{end}/{file_size}"
            if range_header
            else None,
        }
        headers = {k: v for k, v in headers.items() if v is not None}

        response = web.StreamResponse(
            status=206 if range_header else 200,
            reason="Partial Content" if range_header else "OK",
            headers=headers,
        )

        await response.prepare(request)

        # Stream file in chunks
        chunk_size = 64 * 1024  # 64KB chunks
        with open(filepath, "rb") as f:
            f.seek(start)
            bytes_remaining = end - start + 1
            while bytes_remaining > 0:
                chunk = f.read(min(chunk_size, bytes_remaining))
                if not chunk:
                    break
                await response.write(chunk)
                bytes_remaining -= len(chunk)

        return response

    async def start_server(self):
        app = web.Application()
        app.router.add_get("/{filename}", self.handle_file)

        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "localhost", self.port)
        await site.start()

    def _run_server(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self.start_server())
        self._loop.run_forever()

    def __enter__(self):
        self._thread = threading.Thread(target=self._run_server)
        self._thread.daemon = True
        self._thread.start()
        # Give the server a moment to start
        time.sleep(1)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread:
            self._thread.join(timeout=1)

    def get_url(self, filename: str) -> str:
        return f"http://localhost:{self.port}/{filename}"


@pytest.fixture(scope="session")
def mock_http_file_server():
    with _TestHttpFileServer() as server:
        yield server


###########################################################################
# DB fixtures
###########################################################################


@pytest.fixture(scope="function")
def db_client(tmp_path):
    """Create a temporary database for testing."""
    db_path = tmp_path / "test.db"
    client = libression.db.client.DBClient(db_path)

    return client
