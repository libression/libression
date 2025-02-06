import pytest
import httpx
import io
import uuid
from libression.entities.io import FileStreamInfos, FileStreamInfo, FileKeyMapping

TEST_DATA = b"Hello Test!"


@pytest.fixture
def test_file_stream():
    return FileStreamInfo(
        file_stream=io.BytesIO(TEST_DATA),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler_fixture_name", ["docker_webdav_io_handler"])
async def test_upload_and_list(
    io_handler_fixture_name,
    dummy_file_key,
    test_file_stream,
    request: pytest.FixtureRequest,
):
    io_handler = request.getfixturevalue(io_handler_fixture_name)

    try:
        # Test upload
        await io_handler.upload(
            FileStreamInfos(file_streams={dummy_file_key: test_file_stream})
        )

        # Verify upload
        objects = await io_handler.list_objects()
        found_files = [x for x in objects if x.filename == dummy_file_key]
        assert len(found_files) == 1
        assert found_files[0].size == len(TEST_DATA)
    finally:
        # Cleanup: delete test file
        await io_handler.delete([dummy_file_key])


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler_fixture_name", ["docker_webdav_io_handler"])
async def test_nested_upload(
    io_handler_fixture_name,
    dummy_file_key,
    dummy_folder_name,
    test_file_stream,
    request: pytest.FixtureRequest,
):
    io_handler = request.getfixturevalue(io_handler_fixture_name)
    nested_key = f"{dummy_folder_name}/{dummy_file_key}"
    try:
        await io_handler.upload(
            FileStreamInfos(file_streams={nested_key: test_file_stream})
        )

        objects = await io_handler.list_objects(dummy_folder_name)
        found_files = [x for x in objects if x.absolute_path == nested_key]
        assert len(found_files) == 1
        assert found_files[0].size == len(TEST_DATA)
    finally:
        # Cleanup: delete test file
        await io_handler.delete([nested_key])


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler_fixture_name", ["docker_webdav_io_handler"])
async def test_get_readonly_urls(
    io_handler_fixture_name,
    dummy_file_key,
    test_file_stream,
    request: pytest.FixtureRequest,
):
    io_handler = request.getfixturevalue(io_handler_fixture_name)
    # Upload file first
    await io_handler.upload(
        FileStreamInfos(file_streams={dummy_file_key: test_file_stream})
    )

    # Test URL generation
    response = io_handler.get_readonly_urls([dummy_file_key], expires_in_seconds=3600)
    assert dummy_file_key in response.paths
    assert "://" in response.base_url  # protocol is present


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler_fixture_name", ["docker_webdav_io_handler"])
async def test_delete(
    io_handler_fixture_name,
    dummy_file_key,
    test_file_stream,
    request: pytest.FixtureRequest,
):
    io_handler = request.getfixturevalue(io_handler_fixture_name)
    # Upload file first
    await io_handler.upload(
        FileStreamInfos(file_streams={dummy_file_key: test_file_stream})
    )

    # Test delete
    await io_handler.delete([dummy_file_key])
    objects = await io_handler.list_objects()
    assert len([x for x in objects if x.filename == dummy_file_key]) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler_fixture_name", ["docker_webdav_io_handler"])
async def test_delete_missing_file(
    io_handler_fixture_name,
    request: pytest.FixtureRequest,
):
    io_handler = request.getfixturevalue(io_handler_fixture_name)
    await io_handler.delete(["non_existent.txt"])  # should not raise


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler_fixture_name", ["docker_webdav_io_handler"])
async def test_copy(
    io_handler_fixture_name,
    dummy_file_key,
    dummy_folder_name,
    test_file_stream,
    request: pytest.FixtureRequest,
):
    io_handler = request.getfixturevalue(io_handler_fixture_name)
    nested_key = f"{dummy_folder_name}/{dummy_file_key}"
    try:
        # Upload initial file
        await io_handler.upload(
            FileStreamInfos(file_streams={dummy_file_key: test_file_stream})
        )

        # Test copy
        await io_handler.copy(
            [FileKeyMapping(source_key=dummy_file_key, destination_key=nested_key)],
            delete_source=False,
        )

        # Verify both files exist
        objects = await io_handler.list_objects(subfolder_contents=True)
        source_files = [x for x in objects if x.absolute_path == dummy_file_key]
        dest_files = [x for x in objects if x.absolute_path == nested_key]

        assert len(source_files) == 1, "Source file should exist"
        assert len(dest_files) == 1, "Destination file should exist"
        assert source_files[0].size == len(TEST_DATA)
        assert dest_files[0].size == len(TEST_DATA)
    finally:
        # Cleanup: delete both source and destination files
        await io_handler.delete([dummy_file_key, nested_key])


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler_fixture_name", ["docker_webdav_io_handler"])
async def test_move(
    io_handler_fixture_name,
    dummy_file_key,
    test_file_stream,
    request: pytest.FixtureRequest,
):
    io_handler = request.getfixturevalue(io_handler_fixture_name)
    new_key = f"moved_{dummy_file_key}"
    try:
        # Upload initial file
        await io_handler.upload(
            FileStreamInfos(file_streams={dummy_file_key: test_file_stream})
        )

        # Test move
        await io_handler.copy(
            [FileKeyMapping(source_key=dummy_file_key, destination_key=new_key)],
            delete_source=True,
        )

        objects = await io_handler.list_objects()
        assert len([x for x in objects if x.absolute_path == dummy_file_key]) == 0
        assert len([x for x in objects if x.absolute_path == new_key]) == 1
    finally:
        # Cleanup
        await io_handler.delete([new_key])


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler_fixture_name", ["docker_webdav_io_handler"])
async def test_copy_with_overwrite(
    io_handler_fixture_name,
    dummy_file_key,
    dummy_folder_name,
    test_file_stream,
    request: pytest.FixtureRequest,
):
    io_handler = request.getfixturevalue(io_handler_fixture_name)
    nested_key = f"{dummy_folder_name}/{dummy_file_key}"
    try:
        # Upload initial file
        await io_handler.upload(
            FileStreamInfos(file_streams={dummy_file_key: test_file_stream})
        )

        # First copy should succeed
        success_responses = await io_handler.copy(
            [FileKeyMapping(source_key=dummy_file_key, destination_key=nested_key)],
            delete_source=False,
            overwrite_existing=False,
        )
        assert success_responses[0].success

        # Second copy with overwrite=False should fail
        success_responses = await io_handler.copy(
            [FileKeyMapping(source_key=dummy_file_key, destination_key=nested_key)],
            delete_source=False,
            overwrite_existing=False,
        )
        assert not success_responses[0].success

        # Second copy with overwrite=True should succeed
        success_2nd_responses = await io_handler.copy(
            [FileKeyMapping(source_key=dummy_file_key, destination_key=nested_key)],
            delete_source=False,
            overwrite_existing=True,
        )
        assert success_2nd_responses[0].success

    finally:
        # Cleanup
        await io_handler.delete([dummy_file_key, nested_key])


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler_fixture_name", ["docker_webdav_io_handler"])
async def test_move_with_overwrite(
    io_handler_fixture_name,
    dummy_file_key,
    test_file_stream,
    request: pytest.FixtureRequest,
):
    io_handler = request.getfixturevalue(io_handler_fixture_name)
    new_key = f"moved_{dummy_file_key}"
    try:
        # Upload initial files
        await io_handler.upload(
            FileStreamInfos(
                file_streams={
                    dummy_file_key: test_file_stream,
                    new_key: test_file_stream,  # Create destination file
                }
            )
        )

        # Move with overwrite=False should fail
        failed_responses = await io_handler.copy(
            [FileKeyMapping(source_key=dummy_file_key, destination_key=new_key)],
            delete_source=True,
            overwrite_existing=False,
        )
        assert not failed_responses[0].success

        # Move with overwrite=True should succeed
        success_responses = await io_handler.copy(
            [FileKeyMapping(source_key=dummy_file_key, destination_key=new_key)],
            delete_source=True,
            overwrite_existing=True,
        )
        assert success_responses[0].success

        objects = await io_handler.list_objects()
        assert len([x for x in objects if x.absolute_path == dummy_file_key]) == 0
        assert len([x for x in objects if x.absolute_path == new_key]) == 1

    finally:
        # Cleanup
        await io_handler.delete([new_key])


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler_fixture_name", ["docker_webdav_io_handler"])
async def test_list_objects_with_nested_paths(
    io_handler_fixture_name,
    dummy_file_key,
    dummy_folder_name,
    test_file_stream,
    request: pytest.FixtureRequest,
):
    io_handler = request.getfixturevalue(io_handler_fixture_name)
    root_key = dummy_file_key
    nested_key = f"{dummy_folder_name}/{dummy_file_key}"
    try:
        # Upload both files
        await io_handler.upload(
            FileStreamInfos(
                file_streams={root_key: test_file_stream, nested_key: test_file_stream}
            )
        )

        # Test listing all objects
        all_objects = await io_handler.list_objects(subfolder_contents=True)
        root_files = [x for x in all_objects if x.absolute_path == root_key]
        nested_files = [x for x in all_objects if x.absolute_path == nested_key]

        assert (
            len(root_files) == 1
        ), f"Root file not found in: {[x.absolute_path for x in all_objects]}"
        assert (
            len(nested_files) == 1
        ), f"Nested file not found in: {[x.absolute_path for x in all_objects]}"

        # Test listing folder specifically
        folder_objects = await io_handler.list_objects(
            dummy_folder_name, subfolder_contents=True
        )
        folder_files = [x for x in folder_objects if x.absolute_path == nested_key]
        assert (
            len(folder_files) == 1
        ), f"Nested file not found in folder: {[x.absolute_path for x in folder_objects]}"

    finally:
        await io_handler.delete([root_key, nested_key])


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler_fixture_name", ["docker_webdav_io_handler"])
async def test_list_objects_single_vs_recursive(
    io_handler_fixture_name,
    dummy_file_key,
    dummy_folder_name,
    test_file_stream,
    request: pytest.FixtureRequest,
):
    io_handler = request.getfixturevalue(io_handler_fixture_name)
    root_key = dummy_file_key
    nested_key = f"{dummy_folder_name}/{dummy_file_key}"
    nested_subfolder_key = f"{dummy_folder_name}/subfolder/{dummy_file_key}"

    try:
        # Upload files in different directory levels
        await io_handler.upload(
            FileStreamInfos(
                file_streams={
                    root_key: test_file_stream,
                    nested_key: test_file_stream,
                    nested_subfolder_key: test_file_stream,
                }
            )
        )

        # Test single directory listing (non-recursive)
        root_objects = await io_handler.list_objects(subfolder_contents=False)
        assert (
            len([x for x in root_objects if x.absolute_path == root_key]) == 1
        ), "Root file should be listed"
        assert (
            len(
                [
                    x
                    for x in root_objects
                    if x.is_dir and x.filename == dummy_folder_name
                ]
            )
            == 1
        ), "Folder should be listed"
        assert (
            len([x for x in root_objects if x.absolute_path == nested_key]) == 0
        ), "Nested file should not be listed"

        folder_objects = await io_handler.list_objects(
            dummy_folder_name, subfolder_contents=False
        )
        assert (
            len([x for x in folder_objects if x.absolute_path == nested_key]) == 1
        ), "Nested file should be listed"
        assert (
            len([x for x in folder_objects if x.is_dir and x.filename == "subfolder"])
            == 1
        ), "Subfolder should be listed"
        assert (
            len([x for x in folder_objects if x.absolute_path == nested_subfolder_key])
            == 0
        ), "Deeply nested file should not be listed"

        # Test recursive listing
        all_objects = await io_handler.list_objects(subfolder_contents=True)
        print("\nAll objects:")
        for obj in all_objects:
            print(f"Path: {obj.absolute_path}, Is Dir: {obj.is_dir}")

        assert (
            len([x for x in all_objects if x.absolute_path == root_key]) == 1
        ), "Root file should be listed"
        assert (
            len([x for x in all_objects if x.absolute_path == nested_key]) == 1
        ), "Nested file should be listed"
        assert (
            len([x for x in all_objects if x.absolute_path == nested_subfolder_key])
            == 1
        ), "Deeply nested file should be listed"

        folder_objects_recursive = await io_handler.list_objects(
            dummy_folder_name, subfolder_contents=True
        )
        assert (
            len([x for x in folder_objects_recursive if x.absolute_path == nested_key])
            == 1
        ), "Nested file should be listed"
        assert (
            len(
                [
                    x
                    for x in folder_objects_recursive
                    if x.absolute_path == nested_subfolder_key
                ]
            )
            == 1
        ), "Deeply nested file should be listed"
        assert (
            len([x for x in folder_objects_recursive if x.absolute_path == root_key])
            == 0
        ), "Root file should not be listed"

    finally:
        await io_handler.delete(
            [root_key, nested_key, nested_subfolder_key],
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("io_handler_fixture_name", ["docker_webdav_io_handler"])
async def test_list_objects_max_depth(
    io_handler_fixture_name,
    dummy_file_key,
    test_file_stream,
    request: pytest.FixtureRequest,
):
    """Test that max_depth parameter correctly limits directory traversal"""

    io_handler = request.getfixturevalue(io_handler_fixture_name)

    # Create a deeply nested structure:
    # /level1/
    #   - file1.txt
    #   /level2/
    #     - file2.txt
    #     /level3/
    #       - file3.txt
    #       /level4/
    #         - file4.txt

    nested_files = {
        f"level1/{dummy_file_key}": test_file_stream,
        f"level1/level2/{dummy_file_key}": test_file_stream,
        f"level1/level2/level3/{dummy_file_key}": test_file_stream,
        f"level1/level2/level3/level4/{dummy_file_key}": test_file_stream,
    }

    try:
        # Upload all files
        await io_handler.upload(FileStreamInfos(file_streams=nested_files))

        # Test with max_depth=2
        objects_depth2 = await io_handler.list_objects(
            dirpath="level1", subfolder_contents=True, max_depth=2
        )

        # Should find files up to level2 but not deeper
        assert any(
            obj.absolute_path == f"level1/{dummy_file_key}" for obj in objects_depth2
        ), "Should find level 1 file"
        assert any(
            obj.absolute_path == f"level1/level2/{dummy_file_key}"
            for obj in objects_depth2
        ), "Should find level 2 file"
        assert not any(
            obj.absolute_path == f"level1/level2/level3/{dummy_file_key}"
            for obj in objects_depth2
        ), "Should not find level 3 file"
        assert not any(
            obj.absolute_path == f"level1/level2/level3/level4/{dummy_file_key}"
            for obj in objects_depth2
        ), "Should not find level 4 file"

        # Test with max_depth=3
        objects_depth3 = await io_handler.list_objects(
            dirpath="level1", subfolder_contents=True, max_depth=3
        )

        # Should find files up to level3 but not level4
        assert any(
            obj.absolute_path == f"level1/{dummy_file_key}" for obj in objects_depth3
        ), "Should find level 1 file"
        assert any(
            obj.absolute_path == f"level1/level2/{dummy_file_key}"
            for obj in objects_depth3
        ), "Should find level 2 file"
        assert any(
            obj.absolute_path == f"level1/level2/level3/{dummy_file_key}"
            for obj in objects_depth3
        ), "Should find level 3 file"
        assert not any(
            obj.absolute_path == f"level1/level2/level3/level4/{dummy_file_key}"
            for obj in objects_depth3
        ), "Should not find level 4 file"

    finally:
        # Clean up all files and directories
        await io_handler.delete(list(nested_files.keys()))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "io_handler_fixture_name,filename",
    [
        ("docker_webdav_io_handler", f"single_space {uuid.uuid4()}"),
        (
            "docker_webdav_io_handler",
            f"single_space folder/single_space {uuid.uuid4()}",
        ),
        ("docker_webdav_io_handler", f"single_space%20encoded{uuid.uuid4()}"),
        (
            "docker_webdav_io_handler",
            f"single_space%20encodedfolder/single_space%20encoded{uuid.uuid4()}",
        ),
        ("docker_webdav_io_handler", f"double_space  {uuid.uuid4()}"),
        (
            "docker_webdav_io_handler",
            f"double_space  folder/double_space  {uuid.uuid4()}",
        ),
        ("docker_webdav_io_handler", f"double_space%20encoded{uuid.uuid4()}"),
        (
            "docker_webdav_io_handler",
            f"double_space%20encodedfolder/double_space%20encoded{uuid.uuid4()}",
        ),
        (
            "docker_webdav_io_handler",
            f"messy spaces%20  fold%2520er/mixed%20dou ble  space%2520{uuid.uuid4()}",
        ),
    ],
)
async def test_list_objects_filenames_with_spaces(
    io_handler_fixture_name,
    test_file_stream,
    request: pytest.FixtureRequest,
    filename: str,
):
    """Test that max_depth parameter correctly limits directory traversal"""

    io_handler = request.getfixturevalue(io_handler_fixture_name)
    files_with_spaces = {
        filename: test_file_stream,
    }

    try:
        # Upload all files
        await io_handler.upload(FileStreamInfos(file_streams=files_with_spaces))

        objects_root = await io_handler.list_objects(
            dirpath="", subfolder_contents=True, max_depth=3
        )

        found_file = [x for x in objects_root if x.absolute_path == filename]
        assert len(found_file) == 1, "Should find file"

        # test url paths work:
        presigned_urls = io_handler.get_readonly_urls(
            [filename],
            expires_in_seconds=3600,
        )

        presigned_url_path = presigned_urls.paths[filename]
        presigned_url_contents = httpx.get(
            f"{io_handler.presigned_base_url_with_path}/{presigned_url_path}",
            verify=False,
        )

        assert presigned_url_contents.status_code == 200, "Should find file with spaces"

    finally:
        # Clean up all files and directories
        await io_handler.delete(list(files_with_spaces.keys()))
