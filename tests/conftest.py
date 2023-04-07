import os
from typing import Any
import pytest

from libression import s3

TEST_DATA_BUCKET = "test-data-bucket"
TEST_CACHE_BUCKET = "test-cache-bucket"


@pytest.fixture()
def test_data_bucket_name() -> str:
    return TEST_DATA_BUCKET


@pytest.fixture()
def test_cache_bucket_name() -> str:
    return TEST_CACHE_BUCKET


@pytest.fixture()
def black_png() -> bytes:
    filepath = os.path.join(
        os.path.dirname(__file__),
        "fixtures",
        "black.png",
    )
    with open(filepath, "rb") as f:
        content = f.read()
    return content


@pytest.fixture()
def s3_resource() -> Any:

    # setup
    s3.create_bucket(bucket_name=TEST_DATA_BUCKET)
    s3.create_bucket(bucket_name=TEST_CACHE_BUCKET)

    yield

    # teardown
    existing_buckets = [
        x["Name"] for x in s3._get_client().list_buckets()["Buckets"]
    ]
    for bucket in [TEST_DATA_BUCKET, TEST_CACHE_BUCKET]:
        if bucket in existing_buckets:
            items = s3.list_objects(bucket, get_all=True)
            if items:
                s3.delete(items, bucket)

        s3.delete_bucket(bucket)
