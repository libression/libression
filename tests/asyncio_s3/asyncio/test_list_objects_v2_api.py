import datetime
from typing import AsyncGenerator

import botocore.exceptions
import pytest
import pytest_asyncio
from mypy_boto3_s3 import S3ServiceResource

from mercury.aws.s3.asyncio import S3Client

S3_KEY_TO_DATA: dict[str, bytes] = {
    "sample.jpg": b"photo",
    "doc.pdf": b"doc",
    "data/2023/sample.jpg": b"abcdefgh",
    "data/2023/other.txt": b"text",
    "data/2023/08/data.txt": b"data",
    "files/file": b"file",
}


@pytest_asyncio.fixture(scope="function")
async def client_to_full_bucket(
    connected_s3_client: S3Client, sync_s3_boto_resource: S3ServiceResource
) -> AsyncGenerator[S3Client, None]:
    """
    Populates the bucket with predetermined data, then clears it.
    """
    bucket_name = connected_s3_client.bucket_name

    bucket = sync_s3_boto_resource.Bucket(bucket_name)
    for key, data in S3_KEY_TO_DATA.items():

        # Validate the key doesn't already exist, as expected
        try:
            sync_s3_boto_resource.Object(bucket_name, key).load()
        except botocore.exceptions.ClientError:
            pass
        else:
            assert False, f"Object {key} already exists in bucket. This may invalidate test assumptions"

        bucket.put_object(Key=key, Body=data)

    yield connected_s3_client

    # Empty the bucket
    for key, data in S3_KEY_TO_DATA.items():
        sync_s3_boto_resource.Object(bucket_name, key).delete()


@pytest.mark.asyncio
async def test_list_with_default_args(client_to_full_bucket: S3Client) -> None:
    """
    Not changing page limits or filtering. Expected to list the entire (small) bucket
    """

    # Act
    resp = await client_to_full_bucket.list_objects_v2()

    # Assert
    assert len(resp.contents) == len(S3_KEY_TO_DATA)
    assert set(obj.key for obj in resp.contents) == set(S3_KEY_TO_DATA)
    assert len(resp.common_prefixes) == 0


@pytest.mark.asyncio
async def test_object_metadata(client_to_full_bucket: S3Client) -> None:
    # Act
    resp = await client_to_full_bucket.list_objects_v2()

    assert len(resp.contents) == len(S3_KEY_TO_DATA)
    for c in resp.contents:
        assert c.key in S3_KEY_TO_DATA
        assert c.size == len(S3_KEY_TO_DATA[c.key])

        # We can't predict these values, but the members are expected to be populated
        assert isinstance(c.last_modified, datetime.datetime)
        assert c.etag


@pytest.mark.asyncio
async def test_list_empty_bucket(connected_s3_client: S3Client) -> None:
    resp = await connected_s3_client.list_objects_v2()

    assert len(resp.contents) == 0


@pytest.mark.asyncio
async def test_pagination_has_full_content(client_to_full_bucket: S3Client) -> None:
    """
    Gets all keys in 2 batches. Asserts that using the continuation token, there is no intersection between paginated
    results, and that the union of all pages contain the full list of keys.
    """
    first_page_length = 2
    first_page = await client_to_full_bucket.list_objects_v2(max_keys=first_page_length)

    assert len(first_page.contents) == first_page_length
    assert first_page.next_continuation_token

    second_page_length = len(S3_KEY_TO_DATA) - first_page_length
    second_page = await client_to_full_bucket.list_objects_v2(
        max_keys=second_page_length, continuation_token=first_page.next_continuation_token
    )

    assert len(second_page.contents) == second_page_length
    assert second_page.next_continuation_token is None

    first_page_keys = {o.key for o in first_page.contents}
    second_page_keys = {o.key for o in second_page.contents}
    assert first_page_keys & second_page_keys == set()
    assert set(first_page_keys | second_page_keys) == set(S3_KEY_TO_DATA)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prefix, expected_num_contents",
    [
        ("data", 3),
        ("d", 4),  # includes "doc.pdf" as well
        ("data/2023/", 3),
        ("", len(S3_KEY_TO_DATA)),
    ],
)
async def test_list_with_prefix(client_to_full_bucket: S3Client, prefix: str, expected_num_contents: int) -> None:
    resp = await client_to_full_bucket.list_objects_v2(prefix=prefix)

    assert len(resp.contents) == expected_num_contents


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prefix, delimiter, expected_contents, expected_common_prefixes",
    [
        ("", "/", {"sample.jpg", "doc.pdf"}, {"data/", "files/"}),
        ("data", "/", set(), {"data/"}),  # "data" is a "directory", so no individual keys between "data*" and "*/"
        ("d", "/", {"doc.pdf"}, {"data/"}),
        ("data", "2023", set(), {"data/2023"}),  # No individual keys between these as well
        ("data", "23", set(), {"data/2023"}),  # Same as above
        ("", "", set(S3_KEY_TO_DATA), set()),
        ("", "non-existing-delimiter", set(S3_KEY_TO_DATA), set()),
        ("non-existing-prefix", "", set(), set()),
        ("non-existing-prefix", "/", set(), set()),
    ],
)
async def test_list_keys_with_prefix_and_delimiter(
    client_to_full_bucket: S3Client,
    prefix: str,
    delimiter: str,
    expected_contents: set[str],
    expected_common_prefixes: set[str],
) -> None:
    resp = await client_to_full_bucket.list_objects_v2(prefix=prefix, delimiter=delimiter)

    assert {c.key for c in resp.contents} == expected_contents
    assert {p.prefix for p in resp.common_prefixes} == expected_common_prefixes


@pytest.mark.asyncio
async def test_contents_and_prefixes_add_up_to_max_count(client_to_full_bucket: S3Client) -> None:
    max_keys = 2
    resp = await client_to_full_bucket.list_objects_v2(prefix="", delimiter="/", max_keys=max_keys)

    assert len(resp.contents) + len(resp.common_prefixes) == max_keys
