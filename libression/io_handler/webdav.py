import enum
import io
import logging
import time
import typing
import hashlib
import datetime
import requests
import xml.etree.ElementTree as ET
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


def _validate_paths(object_keys: typing.Sequence[str]) -> None:
    for key in object_keys:
        if key.startswith('/'):
            raise ValueError("Object key must not start with a slash")
    return None

def _parse_nginx_ls_size(size_text: str) -> int:
    """Convert Nginx size format to bytes"""
    if size_text == '-':
        return 0
    
    size = float(size_text[:-1])
    unit = size_text[-1].upper()
    
    multipliers = {
        'K': 1024,
        'M': 1024 * 1024,
        'G': 1024 * 1024 * 1024
    }
    
    return int(size * multipliers.get(unit, 1))


class WebDAVIOHandler:
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        secret_key: str,
        verify_ssl: bool = True,
    ):
        if not base_url.endswith('/'):
            raise ValueError("Base URL must end with a slash")

        self.base_url = base_url
        self.auth = (username, password)
        self.verify_ssl = verify_ssl
        self.secret_key = secret_key

    def get(self, file_keys: typing.Sequence[str]) -> libression.entities.io.FileStreams:
        _validate_paths(file_keys)

        streams = dict()
        for file_key in file_keys:
            response = requests.get(
                f"{self.base_url}/{file_key}",
                auth=self.auth,
                verify=self.verify_ssl,
                stream=True
            )
            response.raise_for_status()
            streams[file_key] = io.BytesIO(response.content)

        return libression.entities.io.FileStreams(file_streams=streams)

    def upload(self, file_streams: libression.entities.io.FileStreams) -> None:
        _validate_paths(file_streams.file_streams.keys())

        for filepath, stream in file_streams.file_streams.items():
            stream.seek(0)
            content = stream.read()
            
            response = requests.put(
                f"{self.base_url}{filepath.lstrip('/')}",
                data=content,
                auth=self.auth,
                verify=self.verify_ssl
            )
            response.raise_for_status()

        return None

    def delete(self, file_keys: typing.Sequence[str]) -> None:
        _validate_paths(file_keys)
        for file_key in file_keys:
            response = requests.delete(
                f"{self.base_url}{file_key}",
                auth=self.auth,
                verify=self.verify_ssl
            )
            response.raise_for_status()

    def list_objects(self, dirpath: str = "") -> list[libression.entities.io.ListDirectoryObject]:
        """List directory contents using GET request and parsing Nginx's autoindex"""
        _validate_paths([dirpath])
        
        url = f"{self.base_url}{dirpath.lstrip('/')}"
        logger.debug(f"GET request to: {url}")
        
        response = requests.get(
            url,
            auth=self.auth,
            verify=self.verify_ssl,
            headers={'Accept': 'text/html'}
        )
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        files = []
        
        # Find the <pre> tag that contains the file listing
        pre = soup.find('pre')
        if not pre:
            return files
        
        # Each line in the <pre> tag represents a file/directory
        for line in pre.text.split('\n'):
            if not line.strip() or line.startswith('../'):
                continue
            
            parts = line.strip().split()
            if len(parts) >= 3:
                # Get raw name from the parts (includes trailing slash for dirs)
                raw_name = parts[0]
                
                # Determine if it's a directory and get clean filename
                is_dir = raw_name.endswith('/')
                filename = raw_name[:-1] if is_dir else raw_name
                
                # Get the href from the <a> tag
                link = pre.find('a', href=True, string=lambda x: x and x.strip() == raw_name)
                if not link:
                    continue
                    
                # Build absolute path
                href = link['href']
                if dirpath:
                    # Combine current directory path with href
                    absolute_path = f"{dirpath.strip('/')}/{href.strip('/')}"
                else:
                    absolute_path = href.strip('/')
                
                # Parse modification time (format: "09-Dec-2024 11:56")
                modified = None
                try:
                    date_str = f"{parts[-3]} {parts[-2]}"
                    modified = datetime.datetime.strptime(date_str, '%d-%b-%Y %H:%M')
                except:
                    pass
                
                # Parse size (last column, might be "-" for directories)
                size = 0
                if not is_dir and parts[-1] != '-':
                    try:
                        size = _parse_nginx_ls_size(parts[-1])
                    except:
                        pass
                
                files.append(
                    libression.entities.io.ListDirectoryObject(
                        filename=filename,  # Just the name of the file/folder
                        absolute_path=absolute_path,  # Full path from root
                        size=size,
                        modified=modified,
                        is_dir=is_dir
                    )
                )
        
        return files

    def _presigned_url(self, expires_in_seconds: int, file_key: str) -> str:
        expires = int(time.time()) + expires_in_seconds

        # Create the md5 hash that Nginx expects
        string_to_hash = f"{expires}/secure/{file_key} {self.secret_key}"
        md5_hash = hashlib.md5(string_to_hash.encode()).hexdigest()
        
        # Build the secure URL
        secure_url = (
            f"{self.base_url}/secure/{file_key}"
            f"?md5={md5_hash}&expires={expires}"
        )
        
        return secure_url


    def get_urls(
        self,
        file_keys: typing.Sequence[str],
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
