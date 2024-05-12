import pytest

from mercury.aws.s3.asyncio import create_s3_client
from tests.aws.s3.asyncio.conftest import S3Env


@pytest.mark.asyncio
async def test_create_s3_client(env_params: S3Env, autoclearing_bucket_name: str) -> None:
    """
    This mimics regular recommended creation and usage for the S3 client using a factory method.
    """
    s3_client = create_s3_client(
        bucket_name=autoclearing_bucket_name,
        region=env_params.region,
        endpoint_url=env_params.endpoint_url,
    )

    # Just make sure we can connect and disconnect without error
    name = "some_name"
    data = b"some_data"
    async with s3_client:
        put_resp = await s3_client.put_object(name, body=data)
        assert put_resp.etag

        get_resp = await s3_client.get_object(name)
        assert await get_resp.body.read() == data
