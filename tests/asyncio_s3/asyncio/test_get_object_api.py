import pytest
from mypy_boto3_s3 import S3ServiceResource

from mercury.aws.s3.asyncio.exceptions import S3ClientError
from mercury.aws.s3.asyncio.interfaces import S3Client


@pytest.mark.asyncio
async def test_get_existing_object(connected_s3_client: S3Client, sync_s3_boto_resource: S3ServiceResource) -> None:
    # Arrange
    # Put object via sync raw interface (code not under test)
    sync_bucket = sync_s3_boto_resource.Bucket(connected_s3_client.bucket_name)
    object_name = "somename"
    body = b"blablablaaaaaa"
    aws_resp = sync_bucket.put_object(Key=object_name, Body=body)
    assert aws_resp.e_tag

    # Act
    resp = await connected_s3_client.get_object(object_name)

    # Assert
    assert await resp.body.read() == body
    assert resp.etag == aws_resp.e_tag
    assert resp.content_length == len(body)


@pytest.mark.asyncio
async def test_get_non_existing_object(connected_s3_client: S3Client) -> None:
    with pytest.raises(S3ClientError):
        await connected_s3_client.get_object("blaaa")


@pytest.mark.asyncio
async def test_get_invalid_name(connected_s3_client: S3Client) -> None:
    with pytest.raises(ValueError):
        await connected_s3_client.get_object("")
