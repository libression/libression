import pytest
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3ServiceResource

from mercury.aws.s3.asyncio import S3Client


@pytest.mark.asyncio
async def test_delete_object(connected_s3_client: S3Client, sync_s3_boto_resource: S3ServiceResource) -> None:
    bucket = sync_s3_boto_resource.Bucket(connected_s3_client.bucket_name)
    name = "will_get_deleted"
    data = b"datadata"
    sync_resp = bucket.put_object(Key=name, Body=data)
    assert sync_resp.e_tag

    # Act
    await connected_s3_client.delete_object(name)

    # Assert
    s3_obj = sync_s3_boto_resource.Object(bucket_name=connected_s3_client.bucket_name, key=name)
    with pytest.raises(ClientError) as e:
        s3_obj.get()
    assert e.typename == "NoSuchKey"


@pytest.mark.asyncio
async def test_delete_already_deleted_object(
    connected_s3_client: S3Client, sync_s3_boto_resource: S3ServiceResource
) -> None:
    bucket = sync_s3_boto_resource.Bucket(connected_s3_client.bucket_name)
    name = "will_get_deleted_twice"
    data = b"dataaaa"
    sync_resp = bucket.put_object(Key=name, Body=data)
    assert sync_resp.e_tag

    await connected_s3_client.delete_object(name)

    # Act - Delete again. Implicit assertion - no error is raised
    await connected_s3_client.delete_object(name)


@pytest.mark.asyncio
async def test_delete_non_existent_object(connected_s3_client: S3Client) -> None:
    # Implicit assertion that no error is raised
    await connected_s3_client.delete_object("does_not_exist")


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_object_key", ["", "#$", "Å"])
async def test_delete_object_with_invalid_key_name(connected_s3_client: S3Client, invalid_object_key: str) -> None:
    # Implicit assertion that no error is raised
    await connected_s3_client.delete_object("does_not_exist")
