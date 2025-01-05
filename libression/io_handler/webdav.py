import asyncio
import base64
import datetime
import enum
import hashlib
import logging
import os
import typing

import bs4
import httpx

import libression.entities.base
import libression.entities.io
import libression.config

logger = logging.getLogger(__name__)


class WebDAVServerType(enum.Enum):
    """Known WebDAV server types with sharing capabilities"""

    UNKNOWN = enum.auto()
    NEXTCLOUD = enum.auto()
    OWNCLOUD = enum.auto()
    APACHE = enum.auto()
    NGINX = enum.auto()


def _parse_nginx_ls_size(size_text: str) -> int:
    """Convert Nginx size string to bytes (plain numbers only)"""
    if not size_text or size_text == "-":
        return 0

    return int(size_text)  # Size is always in bytes


class WebDAVIOHandler(libression.entities.io.IOHandler):
    def __init__(
        self,
        base_url: str,
        url_path: str,
        presigned_url_path: str,
        username: str = libression.config.WEBDAV_USER,
        password: str = libression.config.WEBDAV_PASSWORD,
        secret_key: str = libression.config.NGINX_SECURE_LINK_KEY,
        verify_ssl: bool = True,
    ):
        """
        Given webdav setting:
        server {
            listen 443 ssl;
            server_name localhost
            location /dummy/photos/ {...}  # authenticated access (for file operations)
            location /secure/read_only/ {...}  # read-only access (for presigned URLs)
            ...
        }

        access to dummy/photos via https://localhost/dummy/photos/...
        access to secure/read_only via https://localhost/secure/read_only/...

        Args:
            base_url: e.g. "https://localhost" (no slash at the end)
            url_path: e.g. "dummy/photos" (no leading/ending slash)
            presigned_url_path: e.g. "secure/read_only" (no leading/ending slash)
            username: The username to use for webdav authentication
            password: The password to use for webdav authentication
            secret_key: The secret key to use for presigned URLs (nginx secure link key)
            httpx_client: The httpx client to use for requests
        """

        self.base_url = base_url.rstrip("/")
        self.url_path = url_path.rstrip("/").lstrip("/")
        self.presigned_url_path = presigned_url_path.rstrip("/").lstrip("/")
        self.auth = (username, password)
        self.secret_key = secret_key
        self.verify_ssl = verify_ssl

        if not self.url_path:
            raise ValueError("url_path must be not be empty string")
        if not self.presigned_url_path:
            raise ValueError("presigned_url_path must be not be empty string")

    @property
    def presigned_base_url_with_path(self) -> str:
        """
        Returns the base URL with the presigned URL path (no trailing slash)
        """
        if self.presigned_url_path:
            return f"{self.base_url}/{self.presigned_url_path}"
        else:
            return self.base_url

    @property
    def base_url_with_path(self) -> str:
        """
        Returns the base URL with the URL path (no trailing slash)
        """
        if self.url_path:
            return f"{self.base_url}/{self.url_path}"
        else:
            return self.base_url

    def _create_httpx_client(
        self,
        verify_ssl: bool | None = None,
        follow_redirects: bool | None = None,
    ) -> httpx.AsyncClient:
        if verify_ssl is None:
            verify_ssl = self.verify_ssl
        if follow_redirects is None:
            follow_redirects = True

        return httpx.AsyncClient(
            verify=verify_ssl,
            follow_redirects=follow_redirects,
        )

    async def _upload_single(
        self,
        file_key: str,
        file_stream: libression.entities.io.FileStreamInfo,
        opened_client: httpx.AsyncClient,
        chunk_byte_size: int = libression.config.DEFAULT_CHUNK_BYTE_SIZE,
    ) -> libression.entities.base.FileActionResponse:
        """
        Upload a single stream (in chunks)
        Let caller manage (open/close) the client context
        Not meant to be used directly (thus private)
        """

        if chunk_byte_size <= 0:
            raise ValueError("chunk_byte_size must be positive")
        if opened_client.is_closed:
            raise ValueError("httpx client is closed")

        put_headers = {}
        if file_stream.mime_type is not None:
            put_headers["Content-Type"] = file_stream.mime_type.value  # get the string

        # Ensure directory exists before upload
        directory = os.path.dirname(file_key)
        if directory:
            await self._ensure_directory_exists(directory, opened_client)

        async def file_sender():  # func in func annoyingly but need to reference file_stream
            while True:
                chunk = file_stream.file_stream.read(
                    chunk_byte_size
                )  # Read only chunk_byte_size bytes
                if not chunk:  # EOF
                    break
                yield chunk  # Send just this chunk
                # Memory is freed after each chunk is sent

        # httpx will consume the generator one chunk at a time
        response = await opened_client.put(
            f"{self.base_url_with_path}/{file_key}",
            auth=self.auth,
            content=file_sender(),  # Generator is consumed lazily
            headers=put_headers,
        )

        try:
            success = True
            error: str | None = None
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            success = False
            error = str(e)

        return libression.entities.base.FileActionResponse(
            file_key=file_key,
            success=success,
            error=error,
        )

    async def upload(
        self,
        file_streams: libression.entities.io.FileStreamInfos,
        chunk_byte_size: int = libression.config.DEFAULT_CHUNK_BYTE_SIZE,
    ) -> list[libression.entities.base.FileActionResponse]:
        """
        Upload multiple streams (in chunks)
        """
        async with self._create_httpx_client() as opened_client:
            upload_tasks = [
                self._upload_single(file_key, stream, opened_client, chunk_byte_size)
                for file_key, stream in file_streams.file_streams.items()
            ]

            # Execute all uploads concurrently
            return await asyncio.gather(*upload_tasks)

    def get_readonly_urls(
        self,
        file_keys: typing.Sequence[str],
        expires_in_seconds: int,
    ) -> libression.entities.io.GetUrlsResponse:
        """
        Generate a presigned URL that's valid for expires_in seconds
        Returns presigned URL that can be accessed without authentication
        """

        get_readonly_urls_response = dict()

        for file_key in file_keys:
            get_readonly_urls_response[file_key] = self._presigned_url(
                expires_in_seconds, file_key
            )

        return libression.entities.io.GetUrlsResponse(urls=get_readonly_urls_response)

    async def _delete_single(
        self,
        file_key: str,
        opened_client: httpx.AsyncClient,
    ) -> libression.entities.base.FileActionResponse:
        """
        Let caller manage (open/close) the client context
        Not meant to be used directly (thus private)
        """

        if opened_client.is_closed:
            raise ValueError("httpx client is closed")

        response = await opened_client.delete(
            f"{self.base_url_with_path}/{file_key}",
            auth=self.auth,
        )
        try:
            success = True
            error: str | None = None
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            success = False
            error = str(e)
            logger.error(f"Failed to delete file {file_key}: {error}")

        return libression.entities.base.FileActionResponse(
            file_key=file_key,
            success=success,
            error=error,
        )

    async def delete(
        self,
        file_keys: typing.Sequence[str],
    ) -> list[libression.entities.base.FileActionResponse]:
        unique_file_keys = list(set(file_keys))

        async with self._create_httpx_client() as opened_client:
            delete_tasks = [
                self._delete_single(key, opened_client) for key in unique_file_keys
            ]
            return await asyncio.gather(*delete_tasks)

    async def _list_single_directory(
        self,
        dirpath: str,
        opened_client: httpx.AsyncClient,
    ) -> list[libression.entities.io.ListDirectoryObject]:
        """
        List contents of a single directory (non-recursive)
        Let caller manage (open/close) the client context
        Not meant to be used directly (thus private)
        """

        # Ensure directory path has trailing slash for WebDAV
        dirpath = dirpath.rstrip("/")
        url = (
            f"{self.base_url_with_path}/{dirpath}/"
            if dirpath
            else f"{self.base_url_with_path}/"
        )

        response = await opened_client.get(
            url,
            auth=self.auth,
            headers={"Accept": "text/html"},
        )
        response.raise_for_status()

        return self._parse_directory_listing(response.text, dirpath)

    async def _list_recursive(
        self,
        dirpath: str,
        opened_client: httpx.AsyncClient,
        max_depth: int,
        current_depth: int = 0,
    ) -> list[libression.entities.io.ListDirectoryObject]:
        if current_depth >= max_depth:
            return []

        results = []

        # Get initial directory listing
        current_level = await self._list_single_directory(dirpath, opened_client)
        results.extend(current_level)

        # Recursively list subdirectories
        for item in current_level:
            if item.is_dir:
                # Get full path for subdirectory
                subdir_path = item.absolute_path
                # Recursively list contents
                subdir_contents = await self._list_recursive(
                    subdir_path, opened_client, max_depth, current_depth + 1
                )
                results.extend(subdir_contents)

        return results

    async def list_objects(
        self,
        dirpath: str = "",
        subfolder_contents: bool = False,
        max_depth: int = 5,
    ) -> list[libression.entities.io.ListDirectoryObject]:
        """List directory contents using GET request and parsing Nginx's autoindex

        Args:
            dirpath: The directory path to list
            subfolder_contents: If True, only show immediate contents (like ls)
                              If False, show all nested contents recursively
        """
        async with self._create_httpx_client() as opened_client:
            if subfolder_contents:
                return await self._list_recursive(
                    dirpath, opened_client, max_depth=max_depth
                )
            else:
                return await self._list_single_directory(dirpath, opened_client)

    def _parse_directory_listing(
        self, html: str, dirpath: str
    ) -> list[libression.entities.io.ListDirectoryObject]:
        """Parse Nginx autoindex HTML output"""
        soup = bs4.BeautifulSoup(html, "html.parser")
        files: list[libression.entities.io.ListDirectoryObject] = []

        pre = soup.find("pre")
        if not pre:
            return files

        # Create a mapping of text -> href for all links
        links = {a.text.strip(): a["href"] for a in pre.find_all("a")}

        # Split the text and filter out empty lines and parent directory
        lines = [
            line
            for line in pre.text.split("\n")
            if line.strip() and not line.startswith("../")
        ]

        date_pattern = "%d-%b-%Y %H:%M"

        for line in lines:
            parts = line.strip().split()
            raw_name = parts[0]
            if raw_name not in links:
                continue

            is_dir = raw_name.endswith("/")
            filename = raw_name[:-1] if is_dir else raw_name

            absolute_path = (f"{dirpath}/{filename}" if dirpath else filename).strip(
                "/"
            )

            modified = datetime.datetime.strptime(
                f"{parts[-3]} {parts[-2]}", date_pattern
            )

            size = 0 if is_dir else _parse_nginx_ls_size(parts[-1])

            files.append(
                libression.entities.io.ListDirectoryObject(
                    filename=filename,
                    absolute_path=absolute_path,
                    size=size,
                    modified=modified,
                    is_dir=is_dir,
                )
            )

        return files

    def _presigned_url(
        self,
        expires_in_seconds: int,
        file_key: str,
    ) -> str:
        current_time = int(datetime.datetime.now().timestamp())
        expires = current_time + expires_in_seconds

        cleaned_file_key = file_key.lstrip("/")
        uri = f"/{self.presigned_url_path}/{cleaned_file_key}"

        string_to_hash = f"{expires}{uri} {self.secret_key}"
        md5_hash = base64.urlsafe_b64encode(
            hashlib.md5(string_to_hash.encode()).digest()
        ).decode()

        # TODO: check if macos needs this (old code that works...but not sure if its secured link)
        # string_to_hash = f"{expires}/{self.presigned_url_path}/{cleaned_file_key} {self.secret_key}"
        # md5_hash = hashlib.md5(string_to_hash.encode()).hexdigest()

        # Build the secure URL
        secure_url = (
            f"{self.presigned_base_url_with_path}/{cleaned_file_key}"
            f"?md5={md5_hash}&expires={expires}"
        )

        return secure_url

    async def _ensure_directory_exists(self, url_dir_path: str, client) -> None:
        """Create directory and all parent directories if they don't exist."""
        parts = url_dir_path.split("/")
        current_path = str(self.base_url_with_path)  # Start with base path

        for part in parts:
            if not part:
                continue
            current_path = f"{current_path}/{part}"

            # Use full URL dir path (TRAILING SLASH is important) for the request
            response = await client.request("MKCOL", f"{current_path}/", auth=self.auth)

            # 405/409 means directory already exists, which is fine
            if response.status_code not in (201, 405, 409):
                print(f"MKCOL failed with body: {response.text}")
                response.raise_for_status()

    async def _copy_single(
        self,
        file_key_mapping: libression.entities.io.FileKeyMapping,
        opened_client: httpx.AsyncClient,
        delete_source: bool,
        overwrite_existing: bool,
    ) -> libression.entities.base.FileActionResponse:
        """Use WebDAV COPY/MOVE for efficient file operations"""
        method = "MOVE" if delete_source else "COPY"

        destination_dir = os.path.dirname(file_key_mapping.destination_key)
        if destination_dir:
            await self._ensure_directory_exists(destination_dir, opened_client)

        # Construct source and destination URLs
        source_url = f"{self.base_url_with_path}/{file_key_mapping.source_key}"
        destination_url = (
            f"{self.base_url_with_path}/{file_key_mapping.destination_key}"
        )

        # WebDAV requires Destination header with absolute URL
        headers = {
            "Destination": destination_url,
            "Overwrite": "T" if overwrite_existing else "F",
        }

        response = await opened_client.request(
            method, source_url, auth=self.auth, headers=headers
        )

        try:
            success = True
            error: str | None = None
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            success = False
            error = str(e)

        return libression.entities.base.FileActionResponse(
            file_key=file_key_mapping.source_key,
            success=success,
            error=error,
        )

    async def copy(
        self,
        file_key_mappings: typing.Sequence[libression.entities.io.FileKeyMapping],
        delete_source: bool,  # False: copy, True: paste
        overwrite_existing: bool = True,
    ) -> list[libression.entities.base.FileActionResponse]:
        libression.entities.io.FileKeyMapping.validate_mappings(file_key_mappings)

        async with self._create_httpx_client() as opened_client:
            copy_tasks = [
                self._copy_single(
                    mapping,
                    opened_client,
                    delete_source,
                    overwrite_existing,
                )
                for mapping in file_key_mappings
            ]
            return await asyncio.gather(*copy_tasks)
