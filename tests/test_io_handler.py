import pytest
import io
import uuid
from libression.entities.io import FileStreams, FileStream, FileKeyMapping
from libression.io_handler.in_memory import InMemoryIOHandler

TEST_DATA = b"Hello Test!"


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


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler", [InMemoryIOHandler()])
async def test_upload_and_list(io_handler, file_key, test_file_stream):
    # Test upload
    await io_handler.upload(
        FileStreams(file_streams={file_key: test_file_stream}), chunk_byte_size=1024
    )

    # Verify upload
    objects = await io_handler.list_objects()
    found_files = [x for x in objects if x.filename == file_key]
    assert len(found_files) == 1
    assert found_files[0].size == len(TEST_DATA)


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler", [InMemoryIOHandler()])
async def test_nested_upload(io_handler, file_key, folder_name, test_file_stream):
    nested_key = f"{folder_name}/{file_key}"
    await io_handler.upload(
        FileStreams(file_streams={nested_key: test_file_stream}), chunk_byte_size=1024
    )

    objects = await io_handler.list_objects(folder_name)
    found_files = [x for x in objects if x.absolute_path == nested_key]
    assert len(found_files) == 1
    assert found_files[0].size == len(TEST_DATA)


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler", [InMemoryIOHandler()])
async def test_get_readonly_urls(io_handler, file_key, test_file_stream):
    # Upload file first
    await io_handler.upload(
        FileStreams(file_streams={file_key: test_file_stream}), chunk_byte_size=1024
    )

    # Test URL generation
    urls = io_handler.get_readonly_urls([file_key], expires_in_seconds=3600)
    assert file_key in urls.urls
    assert urls.urls[file_key].startswith("memory://")
    assert "expires=3600" in urls.urls[file_key]


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler", [InMemoryIOHandler()])
async def test_delete(io_handler, file_key, test_file_stream):
    # Upload file first
    await io_handler.upload(
        FileStreams(file_streams={file_key: test_file_stream}), chunk_byte_size=1024
    )

    # Test delete
    await io_handler.delete([file_key])
    objects = await io_handler.list_objects()
    assert len([x for x in objects if x.filename == file_key]) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler", [InMemoryIOHandler()])
async def test_delete_missing_file(io_handler):
    # Should not raise when raise_on_error is False
    await io_handler.delete(["non_existent.txt"], raise_on_error=False)

    # Should raise when raise_on_error is True
    with pytest.raises(Exception):
        await io_handler.delete(["non_existent.txt"], raise_on_error=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler", [InMemoryIOHandler()])
async def test_copy(io_handler, file_key, folder_name, test_file_stream):
    # Upload initial file
    await io_handler.upload(
        FileStreams(file_streams={file_key: test_file_stream}), chunk_byte_size=1024
    )

    # Test copy
    nested_key = f"{folder_name}/{file_key}"
    await io_handler.copy(
        [FileKeyMapping(source_key=file_key, destination_key=nested_key)],
        delete_source=False,
        chunk_byte_size=1024,
    )

    # Verify both files exist
    objects = await io_handler.list_objects()
    source_files = [x for x in objects if x.absolute_path == file_key]
    dest_files = [x for x in objects if x.absolute_path == nested_key]

    assert len(source_files) == 1, "Source file should exist"
    assert len(dest_files) == 1, "Destination file should exist"
    assert source_files[0].size == len(TEST_DATA)
    assert dest_files[0].size == len(TEST_DATA)


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler", [InMemoryIOHandler()])
async def test_move(io_handler, file_key, test_file_stream):
    # Upload initial file
    await io_handler.upload(
        FileStreams(file_streams={file_key: test_file_stream}), chunk_byte_size=1024
    )

    # Test move (copy with delete_source=True)
    new_key = f"moved_{file_key}"
    await io_handler.copy(
        [FileKeyMapping(source_key=file_key, destination_key=new_key)],
        delete_source=True,
        chunk_byte_size=1024,
    )

    objects = await io_handler.list_objects()
    assert len([x for x in objects if x.absolute_path == file_key]) == 0
    assert len([x for x in objects if x.absolute_path == new_key]) == 1
