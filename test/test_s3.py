from typing import Any

import pytest

from libression import s3

TEST_BUCKET = "unit-testing-buckets"
TEST_BUCKET_2 = "unit-testing-buckets-2"


@pytest.fixture()
def s3_resource() -> None:
    # setup
    s3.create_bucket(bucket_name=TEST_BUCKET)

    yield

    # teardown
    existing_buckets = [x["Name"] for x in s3.get_client().list_buckets()["Buckets"]]
    for bucket in [TEST_BUCKET, TEST_BUCKET_2]:
        if bucket in existing_buckets:
            items = s3.list_objects(bucket, get_all=True)
            if items:
                s3.delete(items, bucket)

            s3.delete_bucket(bucket)


def test_create_list_delete_buckets(s3_resource: Any) -> None:
    # Act
    s3.create_bucket(TEST_BUCKET_2)

    # Assert
    buckets = [x["Name"] for x in s3.get_client().list_buckets()["Buckets"]]
    assert TEST_BUCKET_2 in buckets


def test_create_get_list_delete_object(s3_resource: Any) -> None:
    # Arrange
    mock_file_contents = b"\x00\x00\x00\x00\x00\x00\x00\x00\x01\x01\x01\x01\x01\x01"

    # Act
    s3.put(
        "rubbish_test_key",
        mock_file_contents,
        TEST_BUCKET,
    )

    list_output = s3.list_objects(TEST_BUCKET)
    retrieved_output = s3.get_body("rubbish_test_key", TEST_BUCKET)

    # Assert
    assert "rubbish_test_key" in list_output
    assert retrieved_output.read() == mock_file_contents


def test_list_object_max_keys(s3_resource: Any) -> None:
    # Arrange (and act)
    mock_file_contents = b"\x00\x00\x00\x00\x00\x00\x00\x00\x01\x01\x01\x01\x01\x01"
    keys = [f"rubbish_max_keys_{n}" for n in range(5)]

    for key in keys:
        s3.put(key, mock_file_contents, TEST_BUCKET)

    # Act and Assert
    assert len(s3.list_objects(TEST_BUCKET)) == 5
    assert len(s3.list_objects(TEST_BUCKET, max_keys=2)) == 2


def test_list_object_with_truncated_keys(s3_resource: Any) -> None:
    # Arrange (and act)
    mock_file_contents = b"\x00\x00\x00\x00\x00\x00\x00\x00\x01\x01\x01\x01\x01\x01"
    keys = [f"rubbish_max_keys_{n}" for n in range(100)]

    for key in keys:
        s3.put(key, mock_file_contents, TEST_BUCKET)

    # Act
    output_get_all = s3.list_objects(
        TEST_BUCKET,
        max_keys=10,  # force truncation
        get_all=True,
    )

    output_not_get_all = s3.list_objects(
        TEST_BUCKET,
        max_keys=10,  # force truncation
        get_all=False,
    )

    # Assert
    assert len(output_get_all) == 100
    assert len(output_not_get_all) == 10
