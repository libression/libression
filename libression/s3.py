import threading
import urllib.parse
from typing import IO, Any, Dict, List, Optional, Tuple, Union

import boto3.session
from botocore.response import StreamingBody
from mypy_boto3_s3.client import S3Client
from mypy_boto3_s3.type_defs import CommonPrefixTypeDef, ObjectTypeDef

# why 3? Long enough to account for service degradation, short enough to be a reasonable timeout
CLIENT_LOCK_TIMEOUT_SECONDS = 3


class S3Connector:
    def __init__(
        self,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: Optional[str] = None,
    ):
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name

        self._client_lock = threading.RLock()
        self._s3_client: Optional[S3Client] = None

    def _connect_s3_client(self) -> S3Client:
        # lock the client creation, because this is not thread safe
        # THIS MUST BE RELEASED. DO NOT EARLY RETURN FROM THIS FUNCTION
        locked = self._client_lock.acquire(timeout=CLIENT_LOCK_TIMEOUT_SECONDS)
        if not locked:
            if self._s3_client is not None:
                # this is SAFE because we didn't acquire the lock, otherwise we CAN NOT
                # early return without RELEASING the lock
                return self._s3_client
            raise RuntimeError(f"Failed to acquire client creation lock (timeout {CLIENT_LOCK_TIMEOUT_SECONDS}s)")

        try:
            if self._s3_client is None:
                # we create own instace of session object because otherwise boto3 uses
                # a global default -- the problem with this is that it means that even when
                # we have separate instances of this `S3Connector` object, they're using a global
                # shared state; but the separate instances have their own locks so the lock
                # won't effectively guard access to the global shared state.
                session = boto3.session.Session(
                    region_name=self.region_name,
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                )
                self._s3_client = session.client(
                    "s3",
                    endpoint_url=self.endpoint_url,
                )
        finally:
            # no except here, because we want to let these errors
            # bubble up, we just need to clean up the lock
            self._client_lock.release()

        return self._s3_client

    def put(
        self,
        content: Union[bytes, IO[bytes], StreamingBody],
        item_key: str,
        bucket_name: str,
        content_type: Optional[str] = None,
    ) -> None:
        """
        Upload content to s3

        Parameters:
            `content` Union[bytes, IO[bytes], StreamingBody]: the file content to upload
            `item_key` str: file path in s3
            `bucket_name` (optional str): bucket name in s3
        """

        client = self._connect_s3_client()

        non_default_args: dict[str, Any] = {}
        if content_type:
            non_default_args["ContentType"] = content_type

        client.put_object(
            Body=content,
            Key=item_key,
            Bucket=bucket_name,
            ACL="bucket-owner-full-control",
            **non_default_args,
        )


    def _list(
        self,
        key_prefix: str,
        *,
        max_items: int,
        bucket_name: Optional[str] = None,
        delimiter: str = "",
    ) -> Tuple[List[ObjectTypeDef], List[CommonPrefixTypeDef]]:

        bucket = self._get_bucket(bucket_name)
        client = self._connect_s3_client()

        next_flag = True
        contents = []
        prefixes = []
        # We use a dict here to do kwargs unpacking to the call because defaulting ContinuationToken
        # to an empty string isn't accepted by the API
        token: Dict[str, str] = {}

        while next_flag:
            response = client.list_objects_v2(
                Bucket=bucket,
                Prefix=key_prefix,
                Delimiter=delimiter,
                MaxKeys=max_items,
                **token,  # type: ignore[arg-type]
            )
            next_flag = response["IsTruncated"]
            token["ContinuationToken"] = response.get("NextContinuationToken", "")  # it won't be there at the end
            contents.extend(response.get("Contents", []))
            prefixes.extend(response.get("CommonPrefixes", []))

        return contents, prefixes

    def _list_with_delimiter(
        self, key_prefix: str, delimiter: str, bucket_name: Optional[str] = None
    ) -> List[CommonPrefixTypeDef]:
        return self._list(key_prefix, bucket_name=bucket_name, delimiter=delimiter)[1]

    def _list_without_delimiter(self, key_prefix: str, bucket_name: Optional[str] = None) -> List[ObjectTypeDef]:
        return self._list(key_prefix, bucket_name=bucket_name)[0]

    def list_prefixes(self, delimiter: str, prefix: str = "", bucket_name: Optional[str] = None) -> List[str]:
        """
        Given a prefix delimiter, and optionally a prefix, list all the prefixes matching up to delimiter
        """
        if delimiter == "":
            raise ValueError("Empty string delimiter will not give the expected behaviour")
        return [o["Prefix"] for o in self._list_with_delimiter(prefix, delimiter, bucket_name)]

    def list_objects(self, key_prefix: str, bucket_name: Optional[str] = None) -> List[str]:
        """
        Get the list of objects in a prefix.
        Parameters:
            'key_prefix': Limits the response to keys that begin with the specified prefix.
            `bucket_name` (optional str): S3 bucket name
        """
        return [o["Key"] for o in self._list_without_delimiter(key_prefix, bucket_name)]

    def list_objects_with_size(self, key_prefix: str, bucket_name: Optional[str] = None) -> Dict[str, int]:
        """
        Get the list of objects in a prefix.
        Parameters:
            'key_prefix': Limits the response to keys that begin with the specified prefix.
            `bucket_name` (optional str): S3 bucket name
        """
        return {o["Key"]: o["Size"] for o in self._list_without_delimiter(key_prefix, bucket_name)}

    def list_uri_objects_with_size(self, uri: str) -> Dict[str, int]:
        bucket, path = self.parse_uri(uri)
        return self.list_objects_with_size(path, bucket)

    def object_size(self, key: str, bucket_name: Optional[str] = None) -> int:
        """
        Get the size of the requested object in bytes.
        Throws ValueError if the key is not found
        """
        contents = self._list_without_delimiter(key, bucket_name)

        if len(contents) == 0:
            raise ValueError(f"Key '{key}' not found!")

        [content] = contents

        if content["Key"] != key:
            raise ValueError(f"Returned information for a key other than '{key}'")

        return content["Size"]

    def get(self, item_key: str, bucket_name: Optional[str] = None) -> bytes:
        """
        Get content from s3

        Parameters:
            `item_key` str: file path
            `bucket_name` (optional str): bucket name

        Returns:
            bytes: file bytes
        """
        bucket = self._get_bucket(bucket_name)
        client = self._connect_s3_client()
        response = client.get_object(Key=item_key, Bucket=bucket)
        body = response["Body"].read()
        return body

    def parse_uri(self, uri: str) -> Tuple[str, str]:
        """
        I have a uri like s3://bucket/path/to/key
        and I want to get the bucket and path
        """
        # ParseResult object is immutable
        parsed = urllib.parse.urlparse(uri)._asdict()
        bucket = parsed["netloc"]
        if not parsed["scheme"].startswith("s3"):
            raise ValueError("Must be an s3 URI scheme")
        parsed["scheme"] = ""
        parsed["netloc"] = ""
        # path needs lstrip after reconstruction because in a uri there will not be a leading slash
        # but this will generate a uri with a leading slash
        path = urllib.parse.ParseResult(**parsed).geturl().lstrip("/")
        return bucket, path

    def get_uri(self, uri: str) -> bytes:
        """
        I have a uri like s3://bucket/path/to/key
        and I want to get the object bytes
        """
        bucket, path = self.parse_uri(uri)
        return self.get(path, bucket)

    def delete(self, item_key: str, bucket_name: Optional[str] = None) -> None:
        """
        Delete file content from s3

        Parameters:
            `item_key` str: file path
            `bucket_name` (optional str): bucket name
        """
        bucket = self._get_bucket(bucket_name)
        client = self._connect_s3_client()

        client.delete_object(Bucket=bucket, Key=item_key)

    def copy(
        self, file_path: str, destination_path: str, destination_bucket: str, source_bucket: Optional[str] = None
    ) -> None:
        """
        Get content from s3, the assumption is that buckets are within the same S3

        Parameters:
            `file_path` str: origin file path
            `destination_path` str: destination file path
            `destination_bucket` str: destination bucket name
            `source_bucket` (optional str): source bucket name if applicable, if not self.default_bucket_name
                will be used
        """
        source_bucket_name = self._get_bucket(source_bucket)

        content = self.get(file_path, bucket_name=source_bucket_name)
        self.put(content, destination_path, bucket_name=destination_bucket)

    def get_presigned_url(self, key: str, bucket_name: Optional[str] = None, expires_in: int = 900) -> Optional[str]:
        """
        Get presigned url

        Parameters:
            `key` str: S3 key
            `bucket_name` (optional str): S3 bucket name
            `expires_in` int: expiration time in seconds
        """
        bucket = self._get_bucket(bucket_name)
        client = self._connect_s3_client()

        return client.generate_presigned_url(
            "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=expires_in
        )