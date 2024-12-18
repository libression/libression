import enum
import io
import logging
import time
import typing
import hashlib
import datetime
import requests
import libression.entities.io
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class WebDAVServerType(enum.Enum):
    """Known WebDAV server types with sharing capabilities"""
    UNKNOWN = enum.auto()
    NEXTCLOUD = enum.auto()
    OWNCLOUD = enum.auto()
    APACHE = enum.auto()
    NGINX = enum.auto()


def _validate_paths(object_keys: typing.Iterable[str]) -> None:
    if not isinstance(object_keys, typing.Iterable):
        raise ValueError("Object keys must be a sequence")
    if isinstance(object_keys, str):
        raise ValueError("Object keys must not be a string (use a list or tuple?)")
    for key in object_keys:
        if key.startswith('/'):
            raise ValueError("Object key must not start with a slash")
    return None

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
        verify_ssl: bool = True,
        url_path: str = "",
        presigned_url_path: str = "",
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
            verify_ssl: Whether to verify SSL certificates
        """

        self.base_url = base_url.rstrip('/')
        self.url_path = url_path.rstrip('/').lstrip('/')
        self.presigned_url_path = presigned_url_path.rstrip('/').lstrip('/')
        self.auth = (username, password)
        self.verify_ssl = verify_ssl
        self.secret_key = secret_key

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

    def get(self, file_keys: typing.Iterable[str]) -> libression.entities.io.FileStreams:
        _validate_paths(file_keys)

        streams = dict()
        for file_key in file_keys:
            response = requests.get(
                f"{self.base_url_with_path}/{file_key}",
                auth=self.auth,
                verify=self.verify_ssl,
                stream=True
            )
            response.raise_for_status()
            streams[file_key] = io.BytesIO(response.content)

        return libression.entities.io.FileStreams(file_streams=streams)

    def upload(self, file_streams: libression.entities.io.FileStreams) -> None:
        _validate_paths(file_streams.file_streams.keys())

        for file_key, stream in file_streams.file_streams.items():
            stream.seek(0)
            content = stream.read()
            
            response = requests.put(
                f"{self.base_url_with_path}/{file_key}",
                data=content,
                auth=self.auth,
                verify=self.verify_ssl
            )
            response.raise_for_status()

        return None

    def delete(self, file_keys: typing.Iterable[str]) -> None:
        _validate_paths(file_keys)
        for file_key in file_keys:
            response = requests.delete(
                f"{self.base_url_with_path}/{file_key}",
                auth=self.auth,
                verify=self.verify_ssl
            )
            response.raise_for_status()

    def list_objects(self, dirpath: str = "", subfolder_contents: bool = False) -> list[libression.entities.io.ListDirectoryObject]:
        """List directory contents using GET request and parsing Nginx's autoindex
        
        Args:
            dirpath: The directory path to list
            subfolder_contents: If True, only show immediate contents (like ls)
                              If False, show all nested contents recursively
        """
        _validate_paths([dirpath])
        
        if subfolder_contents:
            # Recursive listing
            return self._list_recursive(dirpath)
        else:
            # Single directory listing (like ls)
            return self._list_single_directory(dirpath)

    def _list_single_directory(self, dirpath: str) -> list[libression.entities.io.ListDirectoryObject]:
        """List contents of a single directory (non-recursive)"""
        url = f"{self.base_url_with_path}/{dirpath.lstrip('/')}"
        logger.debug(f"GET request to: {url}")
        
        response = requests.get(
            url,
            auth=self.auth,
            verify=self.verify_ssl,
            headers={'Accept': 'text/html'}
        )
        response.raise_for_status()
        
        return self._parse_directory_listing(response.text, dirpath)

    def _list_recursive(
        self,
        dirpath: str,
        max_folder_crawl: int = 100
    ) -> list[libression.entities.io.ListDirectoryObject]:
        """List directory contents recursively"""
        results = []
        to_visit = [dirpath]
        folder_crawl_count = 0
        
        while to_visit and folder_crawl_count <= max_folder_crawl:
            current_dir = to_visit.pop(0)
            dir_contents = self._list_single_directory(current_dir)
            results.extend(dir_contents)
            
            # Add subdirectories to visit
            for item in dir_contents:
                if item.is_dir:
                    to_visit.append(item.absolute_path)
            
            folder_crawl_count += 1

        if len(to_visit) > 0:
            logger.warning(f"Max folder crawl count {max_folder_crawl} reached. {len(to_visit)} folders not explored.")

        return results

    def _parse_directory_listing(self, html: str, dirpath: str) -> list[libression.entities.io.ListDirectoryObject]:
        """Parse Nginx autoindex HTML output"""
        soup = BeautifulSoup(html, 'html.parser')
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

    def _presigned_url(self, expires_in_seconds: int, file_key: str) -> str:
        expires = int(time.time()) + expires_in_seconds

        cleaned_file_key = file_key.lstrip('/')

        # Create the md5 hash that Nginx expects
        if self.presigned_url_path:
            string_to_hash = f"{expires}/{self.presigned_url_path}/{cleaned_file_key} {self.secret_key}"
        else:
            string_to_hash = f"{expires}/{cleaned_file_key} {self.secret_key}"

        md5_hash = hashlib.md5(string_to_hash.encode()).hexdigest()
        
        # Build the secure URL
        secure_url = (
            f"{self.presigned_base_url_with_path}/{cleaned_file_key}"
            f"?md5={md5_hash}&expires={expires}"
        )
        
        return secure_url


    def get_urls(
        self,
        file_keys: typing.Iterable[str],
        expires_in_seconds: int = 60 * 60 * 7
    ) -> libression.entities.io.GetUrlsResponse:
        """
        Generate a presigned URL that's valid for expires_in seconds
        Returns presigned URL that can be accessed without authentication
        """
        _validate_paths(file_keys)

        get_urls_response = dict()

        for file_key in file_keys:
            get_urls_response[file_key] = self._presigned_url(
                expires_in_seconds,
                file_key
            )
        
        return libression.entities.io.GetUrlsResponse(
            urls=get_urls_response
        )
