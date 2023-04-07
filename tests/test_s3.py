import pytest
from typing import Any
from libression import s3


def test_put_get_body(
    s3_resource: Any,
    test_data_bucket_name: str,
) -> None:

    # Arrange
    mock_file_contents = b"\x00\x01"

    # Act
    s3.put(
        "rubbish_test_key",
        mock_file_contents,
        test_data_bucket_name,
    )
    retrieved_output = s3.get_body("rubbish_test_key", test_data_bucket_name)

    # Assert
    assert s3.list_objects(test_data_bucket_name) == ["rubbish_test_key"]
    assert retrieved_output.read() == mock_file_contents


def test_delete(
    s3_resource: Any,
    test_data_bucket_name: str,
) -> None:

    # Arrange
    s3.put(
        "to_del",
        b"\x00\x01",
        test_data_bucket_name,
    )

    # Act
    s3.delete(["to_del"], test_data_bucket_name)

    # Assert
    assert s3.list_objects(test_data_bucket_name) == []


@pytest.mark.parametrize(
    "total_files,max_keys,get_all,expected_count",
    [
        (100, 1000, True, 100),
        (100, 2, True, 100),  # max_keys limited but get_all works
        (100, 2, False, 2),  # max_keys limited, no get_all
    ],
)
def test_list_objects(
    total_files: int,
    max_keys: int,
    get_all: bool,
    expected_count: int,
    s3_resource: Any,
    test_data_bucket_name: str,
) -> None:

    # Arrange
    keys = [f"rubbish_max_keys_{n}" for n in range(total_files)]

    for key in keys:
        s3.put(key, b"\x00\x01", test_data_bucket_name)

    # Act
    output = s3.list_objects(
        test_data_bucket_name,
        max_keys=max_keys,
        get_all=get_all,
    )

    # Assert
    assert len(output) == expected_count


def test_list_objects_prefix_filter(
    s3_resource: Any,
    test_data_bucket_name: str,
) -> None:

    # Arrange
    keys = [
        f"rubbish_max_keys_{n}" for n in range(5)
    ] + [
        f"subfolder/rubbish_max_keys_{n}" for n in range(5)
    ]

    for key in keys:
        s3.put(key, b"\x00\x01", test_data_bucket_name)

    # Act
    subfolder_output = s3.list_objects(
        test_data_bucket_name,
        prefix_filter="subfolder"
    )

    rootfolder_output = s3.list_objects(
        test_data_bucket_name,
    )

    # Assert
    assert len(subfolder_output) == 5
    assert len(rootfolder_output) == 10
