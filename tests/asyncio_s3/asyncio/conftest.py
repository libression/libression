import os
import time
from typing import AsyncGenerator, Generator, NamedTuple, cast

import boto3
import pytest
import pytest_asyncio
from mypy_boto3_s3.service_resource import Bucket, S3ServiceResource

from mercury.aws.common.asyncio import DEFAULT_SESSION_FACTORY
from mercury.aws.s3.asyncio import AiobotocoreS3ClientFactory
from mercury.aws.s3.asyncio.base_client import BaseS3Client
from mercury.aws.s3.asyncio.interfaces import S3Client


class S3Env(NamedTuple):
    region: str
    endpoint_url: str


def _empty_and_delete_bucket(bucket: Bucket) -> None:
    # We can't delete non-empty buckets so we first delete all objects
    for obj in bucket.objects.all():
        obj.delete()
    bucket.delete()
    time.sleep(1)  # Give the S3 backend some time to process the command, to avoid state lingering between tests


@pytest.fixture(scope="session")
def env_params() -> S3Env:
    return S3Env(
        region=os.getenv("AWS_DEFAULT_REGION", "us-east-2"),
        endpoint_url=os.getenv("TEST_AWS_EDGE_ENDPOINT_URL", "http://localhost:4566/"),
    )


@pytest_asyncio.fixture(scope="function")
def autoclearing_bucket_name(env_params: S3Env) -> Generator[str, None, None]:
    bucket_name = f"test-bucket-{time.time()}"

    # We cannot use the same code being tested to set up SNS, so we're using boto3 (synchronous) and not aiobotocore
    session = boto3.session.Session(region_name=env_params.region)
    s3_resource = session.resource("s3", endpoint_url=env_params.endpoint_url)
    bucket = s3_resource.create_bucket(
        Bucket=bucket_name,
    )
    yield bucket_name

    _empty_and_delete_bucket(bucket)


@pytest_asyncio.fixture(scope="function")
def sync_s3_boto_resource(env_params: S3Env) -> S3ServiceResource:
    """
    A raw boto sync S3 client, for tests that need to interact with S3 (e.g., set up) without using the async client
    being tested
    """
    session = boto3.session.Session(region_name=env_params.region)
    return session.resource("s3", endpoint_url=env_params.endpoint_url)


@pytest_asyncio.fixture(scope="function")
async def connected_s3_client(autoclearing_bucket_name: str, env_params: S3Env) -> AsyncGenerator[S3Client, None]:
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    client_factory = AiobotocoreS3ClientFactory(
        session_factory=DEFAULT_SESSION_FACTORY,
        endpoint_url=env_params.endpoint_url,
        region=env_params.region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    client = BaseS3Client(
        client_factory=client_factory,
        bucket_name=autoclearing_bucket_name,
    )

    async with client as connected_client:
        yield cast(S3Client, connected_client)
