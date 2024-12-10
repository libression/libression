import io
import boto3
import logging

import libression.entities.io

logger = logging.getLogger(__name__)


class S3IOHandler(libression.entities.io.IOHandler):
    def __init__(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        endpoint_url: str,
    ):
        self.client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            endpoint_url=endpoint_url,
        )

    def upload(self, key: str, body: bytes, bucket_name: str) -> None:
        self.client.put_object(
            Body=body,
            Bucket=bucket_name,
            Key=key,
            ACL='public-read-write',
        )

    def list_objects(self, bucket_name: str) -> list[str]:
        response = self.client.list_objects(Bucket=bucket_name)
        contents = response.get("Contents")

        if contents is None:
            logger.info(f"list_objects in s3 bucket {bucket_name} returned no matched contents")
            return []

        output = [x["Key"] for x in contents]

        if response.get("IsTruncated"):
            output.extend(self._get_truncated_contents(
                bucket_name,
                response.get("NextMarker"),
            ))

        return output

    def move(self, source_key: str, destination_key: str, bucket_name: str) -> None:
        # S3 doesn't have a direct move operation, so we copy then delete
        self.copy(source_key, destination_key, bucket_name)
        self.delete(source_key, bucket_name)

    def copy(self, source_key: str, destination_key: str, bucket_name: str) -> None:
        copy_source = {
            'Bucket': bucket_name,
            'Key': source_key
        }
        self.client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=destination_key,
            ACL='public-read-write'
        )

    def delete(self, key: str, bucket_name: str) -> None:
        self.client.delete_objects(
            Bucket=bucket_name,
            Delete={
                "Objects": [{"Key": key}],
                "Quiet": True,
            },
        )

    def bytestream(self, key: str, bucket_name: str) -> io.IOBase:
        output = self.client.get_object(Bucket=bucket_name, Key=key)
        if "Body" in output:
            return output["Body"]
        raise FileNotFoundError(f"Object {key} not found in bucket {bucket_name}")

    def _get_truncated_contents(
        self,
        bucket: str,
        next_key: str,
    ) -> list[str]:
        output = []
        truncated_flag = True
        while truncated_flag:
            new_contents = self.client.list_objects(
                Bucket=bucket,
                Marker=next_key,
            )
            extra_data = [x["Key"] for x in new_contents.get("Contents", [])]
            output.extend(extra_data)
            next_key = new_contents.get("NextMarker")
            truncated_flag = new_contents.get("IsTruncated")

        return output
