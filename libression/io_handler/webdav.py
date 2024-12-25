import enum
import io
import logging
import typing
import hashlib
import datetime
import libression.entities.io
import base64
import bs4
import httpx
import asyncio

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
    if not size_text or size_text == '-':
        return 0
    
    return int(size_text)  # Size is always in bytes


class WebDAVIOHandler(libression.entities.io.IOHandler):
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        secret_key: str,
        url_path: str,
        presigned_url_path: str,
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
            username: The username to use for authentication
            password: The password to use for authentication
            secret_key: The secret key to use for presigned URLs
            httpx_client: The httpx client to use for requests  
        """

        self.base_url = base_url.rstrip('/')
        self.url_path = url_path.rstrip('/').lstrip('/')
        self.presigned_url_path = presigned_url_path.rstrip('/').lstrip('/')
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
        file_stream: typing.IO[bytes],
        chunk_byte_size: int,  # 1024 * 1024 is 1MB chunks
        opened_client: httpx.AsyncClient,
    ) -> None:
        """
        Upload a single stream (in chunks)
        Let caller manage (open/close) the client context
        Not meant to be used directly (thus private)
        """

        if chunk_byte_size <= 0:
            raise ValueError("chunk_byte_size must be positive")
        if opened_client.is_closed:
            raise ValueError("httpx client is closed")

        async def file_sender():  # func in func annoyingly but need to reference file_stream
            while True:
                chunk = file_stream.read(chunk_byte_size)  # Read only chunk_byte_size bytes
                if not chunk:  # EOF
                    break
                yield chunk  # Send just this chunk
                # Memory is freed after each chunk is sent

        # httpx will consume the generator one chunk at a time
        response = await opened_client.put(
            f"{self.base_url_with_path}/{file_key}",
            auth=self.auth,
            content=file_sender()  # Generator is consumed lazily
        )
        
        response.raise_for_status()        

    async def upload(
        self,
        file_streams: libression.entities.io.FileStreams,
        chunk_byte_size: int,  # 1024 * 1024 is 1MB chunks
    ) -> None:
        """
        Upload multiple streams (in chunks)
        """
        async with self._create_httpx_client() as opened_client:
            upload_tasks = [
                self._upload_single(
                    file_key,
                    stream.file_stream,
                    chunk_byte_size,
                    opened_client
                )
                for file_key, stream in file_streams.file_streams.items()
            ]

            # Execute all uploads concurrently
            await asyncio.gather(*upload_tasks)

    def get_readonly_urls(
        self,
        file_keys: typing.Iterable[str],
        expires_in_seconds: int,
    ) -> libression.entities.io.GetUrlsResponse:
        """
        Generate a presigned URL that's valid for expires_in seconds
        Returns presigned URL that can be accessed without authentication
        """

        get_readonly_urls_response = dict()

        for file_key in file_keys:
            get_readonly_urls_response[file_key] = self._presigned_url(
                expires_in_seconds,
                file_key
            )

        return libression.entities.io.GetUrlsResponse(
            urls=get_readonly_urls_response
        )

    async def _delete_single(
        self,
        file_key: str,
        opened_client: httpx.AsyncClient,
        raise_on_error: bool,
    ) -> None:
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
        if raise_on_error:
            response.raise_for_status()

    async def delete(
        self,
        file_keys: typing.Iterable[str],
        raise_on_error: bool = True,
    ) -> None:
        unique_file_keys = list(set(file_keys))

        async with self._create_httpx_client() as opened_client:
            delete_tasks = [
                self._delete_single(key, opened_client, raise_on_error)
                for key in unique_file_keys
            ]
            await asyncio.gather(*delete_tasks)

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

        url = f"{self.base_url_with_path}/{dirpath.lstrip('/')}"
        
        response = await opened_client.get(
            url,
            auth=self.auth,
            headers={'Accept': 'text/html'},
        )
        response.raise_for_status()
            
        return self._parse_directory_listing(response.text, dirpath)

    async def _list_recursive(
        self,
        dirpath: str,
        opened_client: httpx.AsyncClient,
        max_folder_crawl: int = 100,
    ) -> list[libression.entities.io.ListDirectoryObject]:
        results = []
        to_visit = [dirpath]
        folder_crawl_count = 0
        
        while to_visit and folder_crawl_count <= max_folder_crawl:
            current_dir = to_visit.pop(0)
            dir_contents = await self._list_single_directory(current_dir, opened_client)
            results.extend(dir_contents)
            
            for item in dir_contents:
                if item.is_dir:
                    to_visit.append(item.absolute_path)
            
            folder_crawl_count += 1

        if len(to_visit) > 0:
            logger.warning(f"Max folder crawl count {max_folder_crawl} reached. {len(to_visit)} folders not explored.")

        return results

    async def list_objects(
        self,
        dirpath: str = "",
        subfolder_contents: bool = False
    ) -> list[libression.entities.io.ListDirectoryObject]:
        """List directory contents using GET request and parsing Nginx's autoindex
        
        Args:
            dirpath: The directory path to list
            subfolder_contents: If True, only show immediate contents (like ls)
                              If False, show all nested contents recursively
        """
        async with self._create_httpx_client() as opened_client:

            if subfolder_contents:
                return await self._list_recursive(dirpath, opened_client)
            else:
                return await self._list_single_directory(dirpath, opened_client)

    def _parse_directory_listing(self, html: str, dirpath: str) -> list[libression.entities.io.ListDirectoryObject]:
        """Parse Nginx autoindex HTML output"""
        soup = bs4.BeautifulSoup(html, 'html.parser')
        files = []
        
        pre = soup.find('pre')
        if not pre:
            return files
        
        # Create a mapping of text -> href for all links
        links = {a.text.strip(): a['href'] for a in pre.find_all('a')}
        
        # Split the text and filter out empty lines and parent directory
        lines = [line for line in pre.text.split('\n') 
                if line.strip() and not line.startswith('../')]
        
        date_pattern = '%d-%b-%Y %H:%M'
        
        for line in lines:
            parts = line.strip().split()
            raw_name = parts[0]
            if raw_name not in links:
                continue
            
            is_dir = raw_name.endswith('/')
            filename = raw_name[:-1] if is_dir else raw_name
            
            href = links[raw_name]
            absolute_path = (f"{dirpath.strip('/')}/{href.strip('/')}" 
                            if dirpath else href.strip('/'))
            
            modified = datetime.datetime.strptime(
                f"{parts[-3]} {parts[-2]}", 
                date_pattern
            )
            
            size = 0 if is_dir else _parse_nginx_ls_size(parts[-1])
            
            files.append(
                libression.entities.io.ListDirectoryObject(
                    filename=filename,
                    absolute_path=absolute_path,
                    size=size,
                    modified=modified,
                    is_dir=is_dir
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

        cleaned_file_key = file_key.lstrip('/')
        uri = f"/{self.presigned_url_path}/{cleaned_file_key}"

        string_to_hash = f"{expires}{uri} {self.secret_key}"
        md5_hash = base64.urlsafe_b64encode(hashlib.md5(string_to_hash.encode()).digest()).decode()

        # TODO: check if macos needs this (old code that works...but not sure if its secured link)
        # string_to_hash = f"{expires}/{self.presigned_url_path}/{cleaned_file_key} {self.secret_key}"
        # md5_hash = hashlib.md5(string_to_hash.encode()).hexdigest()

        # Build the secure URL
        secure_url = (
            f"{self.presigned_base_url_with_path}/{cleaned_file_key}"
            f"?md5={md5_hash}&expires={expires}"
        )

        return secure_url

    async def _copy_single(
        self,
        source_key: str,
        destination_key: str,
        opened_client: httpx.AsyncClient,
        delete_source: bool,
        allow_missing: bool,
        chunk_byte_size: int,  # No longer needed but kept for compatibility
    ) -> None:
        """Use WebDAV COPY/MOVE for efficient file operations"""
        method = "MOVE" if delete_source else "COPY"
        
        # Construct source and destination URLs
        source_url = f"{self.base_url_with_path}/{source_key}"
        destination_url = f"{self.base_url_with_path}/{destination_key}"
        
        # WebDAV requires Destination header with absolute URL
        headers = {
            "Destination": destination_url
        }
        
        response = await opened_client.request(
            method,
            source_url,
            auth=self.auth,
            headers=headers
        )
        
        if response.status_code == 404:
            if not allow_missing:
                raise ValueError(f"File {source_key} not found")
            return None
        
        response.raise_for_status()


    async def copy(
        self,
        file_key_mappings: typing.Iterable[libression.entities.io.FileKeyMapping],
        delete_source: bool,  # False: copy, True: paste
        chunk_byte_size: int,
        allow_missing: bool = False,
    ) -> None:
        async with self._create_httpx_client() as opened_client:
            copy_tasks = [
                self._copy_single(
                    mapping.source_key,
                    mapping.destination_key,
                    opened_client,
                    delete_source,
                    allow_missing,
                    chunk_byte_size
                )
                for mapping in file_key_mappings
            ]
            await asyncio.gather(*copy_tasks)
