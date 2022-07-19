import io
import os
from typing import Optional, List, Tuple
import logging
import boto3
import botocore.response
from boto3.resources.base import ServiceResource

S3_BUCKET = "test_photos"
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID", "minioadmin")
S3_SECRET = os.getenv("S3_SECRET", "miniopassword")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://127.0.0.1:9000")
AWS_REGION = "us-east-2"

logger = logging.getLogger(__name__)


class S3:
    def __init__(
            self,
            aws_access_key_id: Optional[str] = None,
            aws_secret_access_key: Optional[str] = None,
            s3_endpoint_url: Optional[str] = None,
    ):
        self.aws_access_key_id = aws_access_key_id if aws_access_key_id is not None else S3_ACCESS_KEY_ID
        self.aws_secret_access_key = aws_secret_access_key if aws_secret_access_key is not None else S3_SECRET
        self.endpoint_url = s3_endpoint_url if s3_endpoint_url is not None else S3_ENDPOINT_URL
        self._inner_s3_client: Optional[ServiceResource] = None

    @property
    def _s3_client(self):
        if self._inner_s3_client is None:
            logger.info("s3_client not yet initiated...initiating now")
            self._inner_s3_client = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                endpoint_url=self.endpoint_url,
            )
        return self._inner_s3_client

    def list_buckets(self) -> List[str]:
        raw_buckets = self._s3_client.list_buckets()
        return [bucket["Name"] for bucket in raw_buckets["Buckets"]]

    def create_bucket(self, bucket_name: str) -> None:
        if bucket_name not in self.list_buckets():
            location = {'LocationConstraint': AWS_REGION}
            self._s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration=location,
                ACL='public-read-write',
            )

    def delete_bucket(self, bucket_name: str) -> None:
        self._s3_client.delete_bucket(
            Bucket=bucket_name,
        )

    def put_object(self,
                   key: str,
                   body: bytes,
                   bucket_name: str) -> None:
        # TODO need feedback?
        file_io = io.BytesIO(body)

        self._s3_client.put_object(
            Body=file_io,
            Bucket=bucket_name,
            Key=key,
            ACL='public-read-write',
        )

    def get_object(self, key: str, bucket_name: str) -> dict:
        output = self._s3_client.get_object(
            Bucket=bucket_name,
            Key=key,
        )
        return output

    def get_object_body(self, key: str, bucket_name: str) -> botocore.response.StreamingBody:
        output = self._s3_client.get_object(Bucket=bucket_name, Key=key)
        if "Body" in output:
            return output["Body"]
        raise FileNotFoundError

    def delete_objects(
            self,
            keys: List[str],
            bucket_name: str
    ) -> None:
        self._s3_client.delete_objects(
            Bucket=bucket_name,
            Delete={
                "Objects": [{"Key": key} for key in keys],
                "Quiet": True,
            },
        )

    def list_objects(
            self,
            bucket: str,
            max_keys: int = 1000,
            prefix_filter: Optional[str] = None,
            get_all: bool = False,
    ) -> List[str]:

        response = self._s3_client.list_objects(
            Bucket=bucket,
            **self._kwargs_without_none(MaxKeys=max_keys, Prefix=prefix_filter)
        )

        contents = response.get("Contents")

        if contents is None:
            logger.info(f"list_objects in s3 bucket {bucket} returned no matched contents")
            return []
        else:
            output = [x["Key"] for x in contents]

        if response.get("IsTruncated") and get_all:
            extra_data = self._get_truncated_contents(
                bucket,
                response.get("NextMarker"),
                max_keys,
                prefix_filter,
            )

            output.extend(extra_data)

        return output

    def get_subdirs_and_content(
            self,
            bucket: str,
            get_subdir_content: bool,
            rel_dir_no_leading_slash: str = "",
    ) -> Tuple[List[str], List[str]]:
        """
        e.g. Given these exist in the "data" bucket:
            folder0/subfolder0/subsubfolder0/file0.png
            folder1/subfolder1/subsubfolder1/file1.png
            folder1/subfolder1/subsubfolder2/file2.png
            folder1/subfolder1/file3.png
            file4.png
        When specified a partial path "folder1/subfolder1",
        this function should return the subdirectories/files of subfolder1, i.e.
        (
            ["folder1/subfolder1/subsubfolder1","folder1/subfolder1/subsubfolder2"],
            ["folder1/subfolder1/file3.png", "folder1/subfolder1/file1", "folder1/subfolder2/file2"],
        )

        When no partial path specified (i.e. root), should return:
        (
            ["folder0", "folder1"],
            ["file4.png", "folder1/subfolder1/file3.png", "folder1/subfolder1/file1", "folder1/subfolder2/file2"],
        )
        """
        object_keys = self.list_objects(bucket,
                                        prefix_filter=rel_dir_no_leading_slash,
                                        get_all=True)
        subdirs = []
        files = []

        for object_key in object_keys:
            object_key_tokens = object_key[len(rel_dir_no_leading_slash):].split("/")
            prefix = ""
            if rel_dir_no_leading_slash:
                object_key_tokens = object_key_tokens[1:]  # first token is empty str [""]...remove...
                prefix = f"{rel_dir_no_leading_slash}/"

            subdir = f"{prefix}{object_key_tokens[0]}"
            if (len(object_key_tokens) > 1) and (subdir not in subdirs):  # this is a subdir in dir of interest, add it...
                subdirs.append(subdir)
            elif len(object_key_tokens) == 1:
                files.append(object_key)
            else:
                logger.error(
                    "get_subdirs_and_content called with errors\n"
                    f"when calling with root dir\n"
                    f"and processing object key {object_key}"
                )

        if get_subdir_content:
            files = object_keys

        return sorted(subdirs), sorted(files)

    @staticmethod
    def _kwargs_without_none(**kwargs) -> dict:
        # boto3 doesn't like None kwargs...filter them
        return {k: v for k, v in kwargs.items() if v is not None}

    def _get_truncated_contents(
            self,
            bucket: str,
            next_key: str,
            max_keys: int,
            prefix_filter: str,
    ) -> List[str]:
        output = []
        truncated_flag = True
        while truncated_flag:
            new_contents = self._s3_client.list_objects(
                Bucket=bucket,
                Marker=next_key,
                **self._kwargs_without_none(MaxKeys=max_keys, Prefix=prefix_filter)
            )
            extra_data = [x["Key"] for x in new_contents.get("Contents")]
            output.extend(extra_data)
            next_key = new_contents.get("NextMarker")
            truncated_flag = new_contents.get("IsTruncated")

        return output
