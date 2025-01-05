import pytest
import fastapi
import fastapi.testclient
import unittest.mock
import libression.router.media_router
import libression.media_vault


@pytest.fixture
def app():
    app = fastapi.FastAPI()
    app.include_router(libression.router.media_router.router)
    return app


@pytest.fixture
def mock_media_vault():
    vault = unittest.mock.Mock(spec=libression.media_vault.MediaVault)
    # Make async methods actually async
    vault.upload_media = unittest.mock.AsyncMock()
    vault.get_files_info = unittest.mock.AsyncMock()
    vault.copy = unittest.mock.AsyncMock()
    vault.delete = unittest.mock.AsyncMock()
    # Non-async methods stay as regular mocks
    vault.get_thumbnail_presigned_urls = unittest.mock.Mock()
    vault.get_data_presigned_urls = unittest.mock.Mock()
    return vault


@pytest.fixture
def client(app, mock_media_vault):
    app.state.media_vault = mock_media_vault
    return fastapi.testclient.TestClient(app)


############################################################
# --- Tests ---
############################################################


def test_upload_media(client, mock_media_vault):
    # Setup
    test_file_content = "SGVsbG8gV29ybGQ="  # base64 encoded "Hello World"
    expected_response = [
        libression.router.media_router.FileEntry(
            file_key="test/path/file1.jpg",
            file_entity_uuid="blablabla",
            thumbnail_key="test/path/file1_thumb.jpg",
            thumbnail_mime_type="image/jpeg",
            thumbnail_checksum="abc123",
            thumbnail_phash="def456",
            mime_type="image/jpeg",
            tags=[],
        )
    ]
    mock_media_vault.upload_media.return_value = expected_response

    # Execute
    response = client.post(
        "/libression/v1/upload",
        json={
            "files": [{"filename": "test.jpg", "file_source": test_file_content}],
            "target_dir": "test/path",
        },
    )

    # Assert
    assert response.status_code == 200
    assert response.json() == [expected_response[0].model_dump()]
    mock_media_vault.upload_media.assert_called_once()
    call_args = mock_media_vault.upload_media.call_args[1]
    assert len(call_args["upload_entries"]) == 1
    assert call_args["upload_entries"][0].filename == "test.jpg"
    assert call_args["target_dir_key"] == "test/path"


def test_get_files_info(client, mock_media_vault):
    # Setup
    expected_response = [
        libression.router.media_router.FileEntry(
            file_key="test/file1.jpg",
            file_entity_uuid="blablabla",
            thumbnail_key="test/file1_thumb.jpg",
            thumbnail_mime_type="image/jpeg",
            thumbnail_checksum="abc123",
            thumbnail_phash="def456",
            mime_type="image/jpeg",
            tags=[],
        )
    ]
    mock_media_vault.get_files_info.return_value = expected_response

    # Execute
    response = client.post(
        "/libression/v1/files_info",
        json={"file_keys": ["test/file1.jpg"], "force_refresh": False},
    )

    # Assert
    assert response.status_code == 200
    assert response.json() == [expected_response[0].model_dump()]


def test_get_thumbnail_urls(client, mock_media_vault):
    # Setup
    expected_response = {"urls": {"test/thumb1.jpg": "http://example.com/thumb1.jpg"}}
    mock_media_vault.get_thumbnail_presigned_urls.return_value = expected_response

    # Execute
    response = client.post(
        "/libression/v1/thumbnails_urls", json={"file_keys": ["test/thumb1.jpg"]}
    )

    # Assert
    assert response.status_code == 200
    assert response.json() == expected_response


def test_get_file_urls(client, mock_media_vault):
    # Setup
    expected_response = {"urls": {"test/file1.jpg": "http://example.com/file1.jpg"}}
    mock_media_vault.get_data_presigned_urls.return_value = expected_response

    # Execute
    response = client.post(
        "/libression/v1/files_urls", json={"file_keys": ["test/file1.jpg"]}
    )

    # Assert
    assert response.status_code == 200
    assert response.json() == expected_response


def test_copy_files(client, mock_media_vault):
    # Setup
    file_mappings = [
        libression.router.media_router.FileKeyMapping(
            source_key="source/file1.jpg", destination_key="dest/file1.jpg"
        )
    ]
    expected_response = [
        {"file_key": "source/file1.jpg", "success": True, "error_message": None}
    ]
    mock_media_vault.copy.return_value = expected_response

    # Execute
    response = client.post(
        "/libression/v1/copy",
        json={
            "file_mappings": [m.model_dump() for m in file_mappings],
            "delete_source": False,
        },
    )

    # Assert
    assert response.status_code == 200
    assert response.json() == expected_response


def test_delete_files(client, mock_media_vault):
    # Setup
    file_entries = [
        libression.router.media_router.FileEntry(
            file_key="test/file1.jpg",
            file_entity_uuid="123",
            thumbnail_key="test/file1_thumb.jpg",
            thumbnail_mime_type="image/jpeg",
            thumbnail_checksum="abc123",
            thumbnail_phash="def456",
            mime_type="image/jpeg",
            tags=[],
        )
    ]
    expected_response = [
        {"file_key": "test/file1.jpg", "success": True, "error_message": None}
    ]
    mock_media_vault.delete.return_value = expected_response

    # Execute
    response = client.post(
        "/libression/v1/delete",
        json={"file_entries": [entry.model_dump() for entry in file_entries]},
    )

    # Assert
    assert response.status_code == 200
    assert response.json() == expected_response
