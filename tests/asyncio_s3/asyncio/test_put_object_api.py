import base64
import hashlib
from typing import Optional

import pytest
from mypy_boto3_s3 import S3ServiceResource

from mercury.aws.s3.asyncio.exceptions import S3ClientError
from mercury.aws.s3.asyncio.interfaces import S3Client


@pytest.mark.asyncio
async def test_put(connected_s3_client: S3Client, sync_s3_boto_resource: S3ServiceResource) -> None:
    object_name = "some_data"
    data = b"i am data"
    resp = await connected_s3_client.put_object(object_name, body=data)

    assert resp.etag is not None

    # Getting object from s3 using raw sync client (it's rough and generally yikes-inducing)
    sync_s3_obj = sync_s3_boto_resource.Object(bucket_name=connected_s3_client.bucket_name, key=object_name)
    data_from_s3 = sync_s3_obj.get()["Body"].read()
    assert data == data_from_s3


@pytest.mark.asyncio
async def test_error_if_putting_empty_object(connected_s3_client: S3Client) -> None:
    with pytest.raises(ValueError):
        await connected_s3_client.put_object(key="", body=b"12345")


@pytest.mark.asyncio
async def test_can_overwrite(connected_s3_client: S3Client) -> None:
    # Arrange
    obj_name = "duplicated"
    put_resp = await connected_s3_client.put_object(key=obj_name, body=b"data")
    assert put_resp.etag

    # Act
    put_duplicate_resp = await connected_s3_client.put_object(key=obj_name, body=b"other data")

    # Assert
    assert put_duplicate_resp.etag
    assert put_resp.etag != put_duplicate_resp.etag


@pytest.mark.asyncio
async def test_etag_is_same_for_duplicate(connected_s3_client: S3Client) -> None:
    obj_name = "object"
    data: bytes = b"blob"
    put_resp = await connected_s3_client.put_object(key=obj_name, body=data)
    assert put_resp.etag

    # Act
    put_duplicate_resp = await connected_s3_client.put_object(key=obj_name, body=data)

    # Assert
    assert put_duplicate_resp.etag
    assert put_resp.etag == put_duplicate_resp.etag


@pytest.mark.asyncio
async def test_put_with_correct_md5_succeeds(connected_s3_client: S3Client) -> None:
    obj_name = "obj"
    data: bytes = b"thembytes"

    md5_bytes = hashlib.md5(data).digest()
    checksum: str = base64.b64encode(md5_bytes).decode()

    put_resp = await connected_s3_client.put_object(key=obj_name, body=data, content_md5=checksum)

    assert put_resp.etag


@pytest.mark.asyncio
async def test_put_with_empty_md5_succeeds(connected_s3_client: S3Client) -> None:
    obj_name = "obj"
    data: bytes = b"thembytes"

    put_resp = await connected_s3_client.put_object(key=obj_name, body=data, content_md5="")

    assert put_resp.etag


@pytest.mark.asyncio
async def test_error_if_put_with_incorrect_md5(connected_s3_client: S3Client) -> None:
    data: bytes = b"dabytes"

    # Note - checking with a real checksum (format and all), but wrong value, and not just an invalid MD5 string
    wrong_checksum = base64.b64encode(b"wrong_md5").decode()

    with pytest.raises(S3ClientError):
        await connected_s3_client.put_object(key="something", body=data, content_md5=wrong_checksum)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "declared_content_type, expected_content_type",
    [
        (None, "binary/octet-stream"),
        ("application/pdf", "application/pdf"),
        ("application/json", "application/json"),
        ("image/png", "image/png"),
        ("image/jpeg", "image/jpeg"),
        ("image/tiff", "image/tiff"),
        ("text/plain", "text/plain"),
    ],
)
async def test_put_with_content_type(
    connected_s3_client: S3Client,
    declared_content_type: Optional[str],
    expected_content_type: str,
) -> None:
    obj_name = "obj"
    data: bytes = b"thembytes"

    put_resp = await connected_s3_client.put_object(key=obj_name, body=data, content_type=declared_content_type)

    assert put_resp.etag
    retrieved = await connected_s3_client.get_object(obj_name)
    assert getattr(retrieved.body, "content_type") == expected_content_type
