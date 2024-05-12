import pytest
from mypy_boto3_s3 import S3ServiceResource

from mercury.aws.s3.asyncio.interfaces import S3Client


@pytest.mark.asyncio
@pytest.mark.parametrize("expiration_seconds", [60, 300])
async def test_generate_presigned_url(
    expiration_seconds: int, connected_s3_client: S3Client, sync_s3_boto_resource: S3ServiceResource
) -> None:
    # Arrange
    # Put object via sync raw interface (code not under test)
    sync_bucket = sync_s3_boto_resource.Bucket(connected_s3_client.bucket_name)
    key = "test_name"
    body = b"test_content"
    aws_resp = sync_bucket.put_object(Key=key, Body=body)
    assert aws_resp.e_tag

    # Act
    presigned_url = await connected_s3_client.generate_presigned_url(key, expiration_seconds)

    # Assert
    assert isinstance(presigned_url, str)
    assert key in presigned_url
    assert f"Expires={expiration_seconds}" in presigned_url


@pytest.mark.asyncio
async def test_get_empty_key(connected_s3_client: S3Client) -> None:
    with pytest.raises(ValueError):
        await connected_s3_client.generate_presigned_url("")
