import pytest
import fastapi
import fastapi.testclient
import httpx
import unittest.mock
import libression.router.media_router
import libression.media_vault
import base64
import uuid
from unittest.mock import Mock


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
def mock_client(app, mock_media_vault):
    app.state.media_vault = mock_media_vault
    return fastapi.testclient.TestClient(app)


############################################################
# --- Tests ---
############################################################


def test_upload_media(mock_client, mock_media_vault):
    # Setup
    test_file_content = "SGVsbG8gV29ybGQ="  # base64 encoded "Hello World"
    expected_response = [
        libression.entities.db.DBFileEntry(
            file_key="test/path/file1.jpg",
            file_entity_uuid="blablabla",
            thumbnail_key="test/path/file1_thumb.jpg",
            thumbnail_mime_type="image/jpeg",
            thumbnail_checksum="abc123",
            thumbnail_phash="def456",
            mime_type="image/jpeg",
            tags=[],
            action_type=libression.entities.db.DBFileAction.CREATE,
        )
    ]

    # Configure mock_media_vault with necessary attributes for logging
    mock_io_handler = Mock()
    mock_io_handler.base_url = "https://webdav:443"
    mock_io_handler.url_path = "libression_photos"
    mock_media_vault.data_io_handler = mock_io_handler
    mock_media_vault.upload_media.return_value = expected_response

    # Execute
    response = mock_client.post(
        "/libression/v1/upload",
        json={
            "files": [{"filename": "test.jpg", "file_source": test_file_content}],
            "target_dir": "test/path",
        },
    )

    # Assert
    assert response.status_code == 200
    assert response.json() == {
        "files": [
            {
                "file_key": "test/path/file1.jpg",
                "file_entity_uuid": "blablabla",
                "thumbnail_key": "test/path/file1_thumb.jpg",
                "thumbnail_mime_type": "image/jpeg",
                "thumbnail_checksum": "abc123",
                "thumbnail_phash": "def456",
                "mime_type": "image/jpeg",
                "tags": [],
            }
        ]
    }

    mock_media_vault.upload_media.assert_called_once()
    call_args = mock_media_vault.upload_media.call_args[1]
    assert len(call_args["upload_entries"]) == 1
    assert call_args["upload_entries"][0].filename == "test.jpg"
    assert call_args["target_dir_key"] == "test/path"


def test_get_files_info(mock_client, mock_media_vault):
    # Setup
    expected_response = [
        libression.entities.db.DBFileEntry(
            file_key="test/file1.jpg",
            file_entity_uuid="blablabla",
            thumbnail_key="test/file1_thumb.jpg",
            thumbnail_mime_type="image/jpeg",
            thumbnail_checksum="abc123",
            thumbnail_phash="def456",
            mime_type="image/jpeg",
            tags=[],
            action_type=libression.entities.db.DBFileAction.CREATE,
        )
    ]
    mock_media_vault.get_files_info.return_value = expected_response

    # Execute
    response = mock_client.post(
        "/libression/v1/files_info",
        json={"file_keys": ["test/file1.jpg"], "force_refresh": False},
    )

    # Assert
    assert response.status_code == 200
    response_file = response.json()["files"][0]
    assert response_file["file_key"] == expected_response[0].file_key
    assert response_file["file_entity_uuid"] == expected_response[0].file_entity_uuid
    assert response_file["thumbnail_key"] == expected_response[0].thumbnail_key
    assert (
        response_file["thumbnail_mime_type"] == expected_response[0].thumbnail_mime_type
    )
    assert (
        response_file["thumbnail_checksum"] == expected_response[0].thumbnail_checksum
    )
    assert response_file["thumbnail_phash"] == expected_response[0].thumbnail_phash
    assert response_file["mime_type"] == expected_response[0].mime_type
    assert response_file["tags"] == expected_response[0].tags
    assert "action_type" not in response_file.keys()


def test_get_thumbnail_urls(mock_client, mock_media_vault):
    # Setup
    expected_response = {
        "base_url": "http://example.com",
        "paths": {"test/thumb1.jpg": "thumb1.jpg"},
    }
    mock_media_vault.get_thumbnail_presigned_urls.return_value = expected_response

    # Execute
    response = mock_client.post(
        "/libression/v1/thumbnails_urls", json={"file_keys": ["test/thumb1.jpg"]}
    )

    # Assert
    assert response.status_code == 200
    assert response.json() == expected_response


def test_get_file_urls(mock_client, mock_media_vault):
    # Setup
    expected_response = {
        "base_url": "http://example.com",
        "paths": {"test/file1.jpg": "file1.jpg"},
    }
    mock_media_vault.get_data_presigned_urls.return_value = expected_response

    # Execute
    response = mock_client.post(
        "/libression/v1/files_urls", json={"file_keys": ["test/file1.jpg"]}
    )

    # Assert
    assert response.status_code == 200
    assert response.json() == expected_response


def test_copy_files(mock_client, mock_media_vault):
    # Setup
    file_mappings = [
        libression.entities.io.FileKeyMapping(
            source_key="source/file1.jpg", destination_key="dest/file1.jpg"
        )
    ]
    expected_response = [
        {"file_key": "source/file1.jpg", "success": True, "error": None}
    ]
    mock_media_vault.copy.return_value = expected_response

    # Execute
    response = mock_client.post(
        "/libression/v1/copy",
        json={
            "file_mappings": [m.model_dump() for m in file_mappings],
            "delete_source": False,
        },
    )

    # Assert
    assert response.status_code == 200
    assert response.json() == expected_response


def test_delete_files(mock_client, mock_media_vault):
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
    expected_response = [{"file_key": "test/file1.jpg", "success": True, "error": None}]
    mock_media_vault.delete.return_value = expected_response

    # Execute
    response = mock_client.post(
        "/libression/v1/delete",
        json={"file_entries": [entry.model_dump() for entry in file_entries]},
    )

    # Assert
    assert response.status_code == 200
    assert response.json() == expected_response


############################################################
# Integration Tests
############################################################


@pytest.mark.parametrize(
    "minimal_image,",
    [
        "png",
    ],
    indirect=["minimal_image"],
)
def test_upload_media_integration_docker(minimal_image):
    base_libression_url = "http://localhost:8000"
    local_webdav_url = (
        "https://localhost:8443"  # nginx self-signed cert (local address)
    )
    docker_webdav_url = "https://webdav:443"  # nginx self-signed cert (docker address)
    base_64_image = base64.b64encode(minimal_image).decode("utf-8")
    test_dir = f"test/path/{uuid.uuid4()}"
    test_tag = str(uuid.uuid4())

    # Upload a file
    upload_response = httpx.post(
        f"{base_libression_url}/libression/v1/upload",
        json={
            "files": [{"filename": "test.jpg", "file_source": base_64_image}],
            "target_dir": test_dir,
        },
    )
    assert upload_response.status_code == 200
    original_file_entry = libression.router.media_router.FileEntry.model_validate(
        upload_response.json()["files"][0]
    )

    # Copy file
    copied_file_key = f"{test_dir}/{test_dir}/copy.jpg"
    copy_response = httpx.post(
        f"{base_libression_url}/libression/v1/copy",
        json={
            "file_mappings": [
                {
                    "source_key": original_file_entry.file_key,
                    "destination_key": copied_file_key,
                }
            ],
            "delete_source": False,
        },
    )
    assert copy_response.status_code == 200

    # Move file
    moved_file_key = f"{test_dir}/{test_dir}/moved.jpg"
    move_response = httpx.post(
        f"{base_libression_url}/libression/v1/copy",
        json={
            "file_mappings": [
                {
                    "source_key": original_file_entry.file_key,
                    "destination_key": moved_file_key,
                }
            ],
            "delete_source": True,
        },
    )
    assert move_response.status_code == 200

    # Check file info is correct
    get_files_info_response = httpx.post(
        f"{base_libression_url}/libression/v1/files_info",
        json={"file_keys": [copied_file_key, moved_file_key]},
    )

    assert get_files_info_response.status_code == 200

    files_info = get_files_info_response.json()["files"]
    assert len(files_info) == 2

    # Get file urls (bulk)
    file_urls_response = httpx.post(
        f"{base_libression_url}/libression/v1/files_urls",
        json={"file_keys": [copied_file_key, moved_file_key]},
    ).json()

    assert len(file_urls_response["paths"]) == 2

    corrected_base_url = file_urls_response["base_url"].replace(
        docker_webdav_url, local_webdav_url
    )

    # Check fetching info + source files
    file_entries = dict()

    for entry in files_info:
        # parses correctly
        file_entry = libression.router.media_router.FileEntry.model_validate(entry)
        file_entries[file_entry.file_key] = file_entry

        assert file_entry.file_key in [copied_file_key, moved_file_key]
        assert file_entry.file_entity_uuid is not None

        # Check file is accessible
        found_path = file_urls_response["paths"][file_entry.file_key]
        adjusted_file_url = f"{corrected_base_url}/{found_path}"
        file_get_response = httpx.get(adjusted_file_url, verify=False)
        assert file_get_response.status_code == 200
        assert file_get_response.content == minimal_image

        # Check thumbnail is accessible
        thumbnail_urls_response = httpx.post(
            f"{base_libression_url}/libression/v1/thumbnails_urls",
            json={"file_keys": [file_entry.thumbnail_key]},
        ).json()

        assert len(thumbnail_urls_response["paths"]) == 1

        corrected_thumbnail_base_url = thumbnail_urls_response["base_url"].replace(
            docker_webdav_url, local_webdav_url
        )

        adjusted_thumbnail_url = f"{corrected_thumbnail_base_url}/{thumbnail_urls_response['paths'][file_entry.thumbnail_key]}"
        thumbnail_get_response = httpx.get(adjusted_thumbnail_url, verify=False)
        assert thumbnail_get_response.status_code == 200
        assert len(thumbnail_get_response.content) > 0

    # Update tags
    update_tags_response = httpx.post(
        f"{base_libression_url}/libression/v1/update_tags",
        json={
            "tag_entries": [
                {
                    "file_entity_uuid": file_entries[copied_file_key].file_entity_uuid,
                    "tags": [test_tag, f"{test_tag}_2", f"{test_tag}_3"],
                }
            ],
        },
    )
    assert update_tags_response.status_code == 200

    search_by_tags_positive_response = httpx.post(
        f"{base_libression_url}/libression/v1/search_by_tags",
        json={
            "include_tag_groups": [[test_tag], ["asdfadf"]],
            "exclude_tags": [],
        },
    )
    assert search_by_tags_positive_response.status_code == 200
    assert len(search_by_tags_positive_response.json()["files"]) == 1

    search_by_tags_negative_response = httpx.post(
        f"{base_libression_url}/libression/v1/search_by_tags",
        json={
            "include_tag_groups": [[test_tag], [f"{test_tag}_2"]],
            "exclude_tags": [f"{test_tag}_3"],  # Key to return no files
        },
    )
    assert search_by_tags_negative_response.status_code == 200
    assert len(search_by_tags_negative_response.json()["files"]) == 0

    # Check dir contents
    show_dir_contents_response = httpx.post(
        f"{base_libression_url}/libression/v1/show_dir_contents",
        json={"dir_key": test_dir, "subfolder_contents": True},
    )
    assert show_dir_contents_response.status_code == 200
    show_dir_obj = (
        libression.router.media_router.ShowDirContentsResponse.model_validate(
            show_dir_contents_response.json()
        )
    )
    assert len([x for x in show_dir_obj.dir_contents if not x.is_dir]) > 0

    # Check delete
    delete_response = httpx.post(
        f"{base_libression_url}/libression/v1/delete",
        json={"file_entries": [x.model_dump() for x in file_entries.values()]},
    )
    assert delete_response.status_code == 200

    # Check files are deleted + teardown
    for file_key in [original_file_entry.file_key, copied_file_key, moved_file_key]:
        bad_get_files_info_response = httpx.post(
            f"{base_libression_url}/libression/v1/files_info",
            json={"file_keys": [file_key]},
        )
        assert (
            bad_get_files_info_response.status_code == 500
        )  # Not good code, but not designed to be called like this

        bad_file_urls_response = httpx.post(
            f"{base_libression_url}/libression/v1/files_urls",
            json={"file_keys": [file_key]},
        ).json()

        corrected_base_url = bad_file_urls_response["base_url"].replace(
            docker_webdav_url, local_webdav_url
        )
        adjusted_bad_file_url = (
            f"{corrected_base_url}/{bad_file_urls_response['paths'][file_key]}"
        )
        bad_file_get_response = httpx.get(adjusted_bad_file_url, verify=False)
        assert bad_file_get_response.status_code == 404

    # Check dir contents
    show_empty_dir_contents_response = httpx.post(
        f"{base_libression_url}/libression/v1/show_dir_contents",
        json={"dir_key": test_dir, "subfolder_contents": True},
    )

    assert show_empty_dir_contents_response.status_code == 200
    show_empty_dir_obj = (
        libression.router.media_router.ShowDirContentsResponse.model_validate(
            show_empty_dir_contents_response.json()
        )
    )
    assert len([x for x in show_empty_dir_obj.dir_contents if not x.is_dir]) == 0

    print("passed")
