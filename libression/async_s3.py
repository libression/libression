import asyncio
import datetime
import urllib.parse
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass
from types import TracebackType
from typing import Type, Protocol, Optional, Any, NewType, Sequence, TypeVar
from botocore.exceptions import BotoCoreError, ClientError
from aiobotocore.session import get_session
from types_aiobotocore_s3.client import S3Client as AiobotocoreS3Client
from typing_extensions import Self


TClient = TypeVar("TClient", covariant=True)
ContinuationToken = NewType("ContinuationToken", str)
_BotoExceptions = (ClientError, BotoCoreError)


class BodyReader(Protocol):
    async def read(self, n_bytes: Optional[int] = None) -> bytes:
        ...


@dataclass
class GetObjectResponse:
    body: BodyReader
    etag: str
    last_modified: datetime.datetime
    content_length: int


@dataclass(frozen=True)
class PutObjectResponse:
    etag: str


@dataclass(frozen=True)
class ObjectMetadata:
    key: str
    size: int
    last_modified: datetime.datetime
    etag: str


@dataclass(frozen=True)
class PrefixData:
    prefix: str


@dataclass(frozen=True)
class ListObjectsV2Response:
    contents: Sequence[ObjectMetadata]
    common_prefixes: Sequence[PrefixData]
    next_continuation_token: Optional[ContinuationToken]


async def create_client(
    region: str | None = None,
    endpoint_url: str | None = None,
    aws_secret_access_key: str | None = None,
    aws_access_key_id: str | None = None,
) -> AbstractAsyncContextManager[TClient]:
    session = get_session()
    return session.create_client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=region,
        aws_secret_access_key=aws_secret_access_key,
        aws_access_key_id=aws_access_key_id,
    )


def parser_s3_uri(uri: str) -> tuple[str, str]:
    """
    given uri s3://bucket/path/to/key
    returns bucket and path
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


class ManagedS3Client:
    def __init__(self):
        """
        Manages client ownership. Each referencer controls enter/exit or connect/close.
        i.e. connects to the resource when first referencer enters/connects it,
        and disconnects when the last referencer closes/exits.

        Use with an async context manager wrapper
        """
        self._aexit_stack = AsyncExitStack()
        self._client: AiobotocoreS3Client | None = None
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        async with self._lock:  # avoid potential invalid state
            client_ctxmgr: AbstractAsyncContextManager[AiobotocoreS3Client] = await create_client()
            self._client = await self._aexit_stack.enter_async_context(client_ctxmgr)

    async def close(self) -> None:
        if self._client is None:
            raise Exception("Already disconnected. Are connect() and close() calls mismatched?")

        async with self._lock:  # avoid potential invalid state
            await self._aexit_stack.aclose()
            self._client = None

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(
            self,
            exc_type: Type[BaseException] | None,
            exc: BaseException | None,
            traceback: TracebackType | None,
    ) -> bool | None:
        await self.close()
        return False  # Do not suppress exceptions

    async def get_client(self) -> AiobotocoreS3Client:
        if self._client is None:
            raise Exception('Inner client not connected. Use connect() or "async with"')
        return self._client

    async def _get(self, key: str, bucket_name: str) -> GetObjectResponse:
        if not key:
            raise ValueError("Object key cannot be empty")

        client = await self.get_client()
        try:
            aws_resp = await client.get_object(Bucket=bucket_name, Key=key)
        except _BotoExceptions as e:
            raise Exception from e

        return GetObjectResponse(
            body=aws_resp["Body"],
            etag=aws_resp["ETag"],
            last_modified=aws_resp["LastModified"],
            content_length=aws_resp["ContentLength"],
        )

    async def get_uri_bytes(self, uri: str) -> bytes:
        """
        uri like s3://bucket/path/to/key
        and I want to get the object bytes
        """
        bucket, path = parser_s3_uri(uri)
        object_response = await self._get(path, bucket)
        return await object_response.body.read()

    async def put(
            self,
            body: bytes,
            target_s3_uri: str,
            content_md5: Optional[str] = None,
            content_type: Optional[str] = None,
    ) -> PutObjectResponse:

        non_default_args: dict[str, Any] = {}
        if content_md5:
            non_default_args["ContentMD5"] = content_md5
        if content_type:
            non_default_args["ContentType"] = content_type

        client = await self.get_client()
        bucket_name, key = parser_s3_uri(target_s3_uri)
        try:
            aws_resp = await client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=body,
                **non_default_args,
            )
        except _BotoExceptions as e:
            raise Exception from e

        return PutObjectResponse(etag=aws_resp["ETag"])

    async def copy(
        self,
        source_s3_uri: str,
        destination_s3_uri: str,
    ) -> None:
        content = await self.get_uri_bytes(source_s3_uri)
        await self.put(content, destination_s3_uri)

    async def delete(self, s3_uri: str) -> None:
        client = await self.get_client()
        bucket_name, key = parser_s3_uri(s3_uri)
        try:
            # Note, the response contains a status code of 204 whether an object was deleted or not, so there
            # is no benefit in returning it. If something really went wrong, an exception will be raised.
            await client.delete_object(
                Bucket=bucket_name,
                Key=key,
            )
        except _BotoExceptions as e:
            raise Exception from e

    async def list_objects(
            self,
            *,
            max_keys: int,
            bucket_name: str,
            prefix: Optional[str] = None,
            delimiter: Optional[str] = None,
            continuation_token: Optional[ContinuationToken] = None,
    ) -> ListObjectsV2Response:
        non_default_args: dict[str, Any] = {}
        if continuation_token:
            non_default_args["ContinuationToken"] = continuation_token
        if prefix:
            non_default_args["Prefix"] = prefix
        if delimiter:
            non_default_args["Delimiter"] = delimiter

        client = await self.get_client()
        try:
            aws_resp = await client.list_objects_v2(
                Bucket=bucket_name,
                MaxKeys=max_keys,
                **non_default_args,
            )
        except _BotoExceptions as e:
            raise Exception from e

        next_token: Optional[ContinuationToken] = None
        if aws_resp["IsTruncated"]:
            next_token = ContinuationToken(aws_resp["NextContinuationToken"])

        objects: list[ObjectMetadata] = []
        for aws_obj in aws_resp.get("Contents", {}):
            objects.append(
                ObjectMetadata(
                    key=aws_obj["Key"],
                    size=aws_obj["Size"],
                    last_modified=aws_obj["LastModified"],
                    etag=aws_obj["ETag"],
                )
            )

        prefixes = [PrefixData(prefix=p["Prefix"]) for p in aws_resp.get("CommonPrefixes", [])]

        return ListObjectsV2Response(next_continuation_token=next_token, contents=objects, common_prefixes=prefixes)


'''

    def _list(
        self,
        key_prefix: str,
        *,
        bucket_name: Optional[str] = None,
        delimiter: str = "",
        max_items: int = AWS_LIST_OBJECTS_MAX,
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

    def _list_without_delimiter(self, key_prefix: str, bucket_name: Optional[str] = None) -> List[ObjectTypeDef]:
        return self._list(key_prefix, bucket_name=bucket_name)[0]

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
'''