import os
from typing import Any
from botocore.response import StreamingBody

from libression import organiser, s3_old
from libression.entities import PageParamsRequest


def test_get_content(
    s3_resource: Any,
    test_data_bucket_name: str,
) -> None:

    # Arrange
    mock_file_contents = b"\x00\x00\x01\x01"

    s3.put(
        "rubbish_test_key",
        mock_file_contents,
        test_data_bucket_name,
    )

    # Act
    output = organiser.get_content("rubbish_test_key", test_data_bucket_name)

    # Assert
    assert output._raw_stream.data == mock_file_contents


def test_load_cache(
    s3_resource: Any,
    test_cache_bucket_name: str,
) -> None:

    # Arrange
    mock_file_contents = b"\x00\x00\x01\x01"

    s3.put(
        organiser._cache_key("rubbish_test_key"),
        mock_file_contents,
        test_cache_bucket_name,
    )

    # Act
    output = organiser.load_cache("rubbish_test_key", test_cache_bucket_name)

    # Assert
    assert isinstance(output, StreamingBody)
    assert output._raw_stream.data == mock_file_contents


def test_update_cache(
    s3_resource: Any,
    black_png: bytes,
    test_data_bucket_name: str,
    test_cache_bucket_name: str,
):

    # Arrange
    s3.put(
        "to_cache.png",
        black_png,
        test_data_bucket_name,
    )

    # Act
    output = organiser.update_caches(
        list_of_keys=["to_cache.png"],
        overwrite=False,
        data_bucket=test_data_bucket_name,
        cache_bucket=test_cache_bucket_name,
    )

    # Assert
    assert output.get("to_cache.png") is not None
    assert len(s3.list_objects(test_cache_bucket_name)) == 1


def test_move(
    s3_resource: Any,
    black_png: bytes,
    test_data_bucket_name: str,
    test_cache_bucket_name: str,
):

    # Arrange
    original_key = os.path.join(
        "original folder", "fixture.png"
    )

    s3.put(
        original_key,
        black_png,
        test_data_bucket_name,
    )

    organiser.update_caches(
        [original_key],
        data_bucket=test_data_bucket_name,
        cache_bucket=test_cache_bucket_name,
    )

    # Act
    organiser.move(
        file_keys=[original_key],
        target_dir="another folder",
        data_bucket=test_data_bucket_name,
        cache_bucket=test_cache_bucket_name,
    )

    # Assert
    target_path = os.path.join(
        "another folder", "fixture.png"
    )

    assert s3.list_objects(
        test_data_bucket_name,
    ) == [target_path]

    assert s3.list_objects(
        test_cache_bucket_name,
    ) == [organiser._cache_key(target_path)]

    assert s3.get_body(
        target_path,
        test_data_bucket_name,
    )._raw_stream.data == black_png


def test_copy(
    s3_resource: Any,
    black_png: bytes,
    test_data_bucket_name: str,
    test_cache_bucket_name: str,
):

    # Arrange
    original_key = os.path.join(
        "original folder", "fixture.png"
    )

    s3.put(
        original_key,
        black_png,
        test_data_bucket_name,
    )

    organiser.update_caches(
        [original_key],
        data_bucket=test_data_bucket_name,
        cache_bucket=test_cache_bucket_name,
    )

    # Act
    organiser.copy(
        file_keys=[original_key],
        target_dir="another folder",
        data_bucket=test_data_bucket_name,
        cache_bucket=test_cache_bucket_name,
    )

    # Assert
    target_path = os.path.join(
        "another folder", "fixture.png"
    )

    in_data = [target_path, original_key]
    in_cache = [
        organiser._cache_key(target_path),
        organiser._cache_key(original_key),
    ]

    assert sorted(s3.list_objects(test_data_bucket_name)) == in_data
    assert sorted(s3.list_objects(test_cache_bucket_name)) == in_cache

    assert s3.get_body(
        target_path,
        test_data_bucket_name,
    )._raw_stream.data == black_png


def test_fetch_page_params(
    test_data_bucket_name: str,
    black_png: bytes,
    s3_resource: Any,
):
    # Arrange
    fixture = PageParamsRequest(
        cur_dir="",
        show_subdirs=True,
        show_hidden_content=True,
    )

    file_key = os.path.join("folder", "fixture.png")

    s3.put(
        file_key,
        black_png,
        test_data_bucket_name,
    )

    # Act
    output = organiser.fetch_page_params(
        fixture,
        test_data_bucket_name,
    )

    assert output.file_keys == [file_key]
    assert output.inner_dirs == ["folder"]
    assert output.par_dir == ""
