import io
from typing import Optional
import logging
import boto3
import botocore.response
from boto3.resources.base import ServiceResource

from libression.config import S3_ACCESS_KEY_ID, S3_SECRET, S3_ENDPOINT_URL, AWS_REGION

logger = logging.getLogger(__name__)

"""
https://boto3.amazonaws.com/v1/documentation/api/1.14.31/guide/resources.html

Note
Low-level clients are thread safe. When using a low-level client,
it is recommended to instantiate your client then pass that client object
to each of your threads.
"""


def get_client(
        aws_access_key_id: str = S3_ACCESS_KEY_ID,
        aws_secret_access_key: str = S3_SECRET,
        endpoint_url: str = S3_ENDPOINT_URL,
):
    return boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url,
    )


def create_bucket(bucket_name: str, s3_client: Optional[ServiceResource] = None) -> None:
    s3_client = s3_client or get_client()

    if bucket_name not in [bucket["Name"] for bucket in s3_client.list_buckets()["Buckets"]]:
        location = {'LocationConstraint': AWS_REGION}
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration=location,
            ACL='public-read-write',
        )


def delete_bucket(bucket_name: str, s3_client: Optional[ServiceResource] = None) -> None:
    s3_client = s3_client or get_client()
    s3_client.delete_bucket(Bucket=bucket_name)


def put(key: str, body: bytes, bucket_name: str, s3_client: Optional[ServiceResource] = None) -> None:
    s3_client = s3_client or get_client()
    bytes_io = io.BytesIO(body)
    s3_client.put_object(
        Body=bytes_io,
        Bucket=bucket_name,
        Key=key,
        ACL='public-read-write',
    )


def get(key: str, bucket_name: str, s3_client: Optional[ServiceResource] = None) -> dict:
    s3_client = s3_client or get_client()
    return s3_client.get_object(Bucket=bucket_name, Key=key)


def get_body(
        key: str,
        bucket_name: str,
        s3_client: Optional[ServiceResource] = None
) -> botocore.response.StreamingBody:

    s3_client = s3_client or get_client()
    output = s3_client.get_object(Bucket=bucket_name, Key=key)
    if "Body" in output:
        return output["Body"]
    raise FileNotFoundError


def delete(
        keys: list[str],
        bucket_name: str,
        s3_client: Optional[ServiceResource] = None,
) -> None:
    s3_client = s3_client or get_client()
    s3_client.delete_objects(
        Bucket=bucket_name,
        Delete={
            "Objects": [{"Key": key} for key in keys],
            "Quiet": True,
        },
    )


def _kwargs_without_none(**kwargs) -> dict:
    # boto3 doesn't like None kwargs...filter them
    return {k: v for k, v in kwargs.items() if v is not None}


def list_objects(
        bucket: str,
        max_keys: int = 1000,
        prefix_filter: Optional[str] = None,
        get_all: bool = False,
        s3_client: Optional[ServiceResource] = None,
) -> list[str]:
    s3_client = s3_client or get_client()
    response = s3_client.list_objects(
        Bucket=bucket,
        **_kwargs_without_none(MaxKeys=max_keys, Prefix=prefix_filter)
    )

    contents = response.get("Contents")

    if contents is None:
        logger.info(f"list_objects in s3 bucket {bucket} returned no matched contents")
        return []
    else:
        output = [x["Key"] for x in contents]

    if response.get("IsTruncated") and get_all:
        extra_data = _get_truncated_contents(
            bucket,
            response.get("NextMarker"),
            max_keys,
            prefix_filter,
        )

        output.extend(extra_data)

    return output


def _get_truncated_contents(
        bucket: str,
        next_key: str,
        max_keys: int,
        prefix_filter: Optional[str] = None,
        s3_client: Optional[ServiceResource] = None,
) -> list[str]:
    s3_client = s3_client or get_client()
    output = []
    truncated_flag = True
    while truncated_flag:
        new_contents = s3_client.list_objects(
            Bucket=bucket,
            Marker=next_key,
            **_kwargs_without_none(MaxKeys=max_keys, Prefix=prefix_filter)
        )
        extra_data = [x["Key"] for x in new_contents.get("Contents")]
        output.extend(extra_data)
        next_key = new_contents.get("NextMarker")
        truncated_flag = new_contents.get("IsTruncated")

    return output
