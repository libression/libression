import boto3
import logging
import datetime
from typing import Dict, IO, List

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

    def upload(self, file_streams: Dict[str, IO[bytes]]) -> None:
        for filepath, stream in file_streams.items():
            try:
                stream.seek(0)
                content = stream.read()
                
                self.client.put_object(
                    Body=content,
                    Bucket=self.bucket_name,
                    Key=filepath.lstrip('/'),
                )
            except Exception as e:
                logger.error(f"Upload failed for {filepath}: {e}")
                raise

    def list_objects(self, dirpath: str = "", subfolder_contents: bool = False) -> List[libression.entities.io.ListDirectoryObject]:
        """
        List objects in S3 bucket with directory-like hierarchy
        
        Args:
            dirpath: The directory path to list
            subfolder_contents: If True, only show immediate contents (like ls)
                              If False, show all nested contents
        """
        prefix = dirpath.lstrip('/')
        if prefix and not prefix.endswith('/'):
            prefix += '/'
        
        response = self.client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=prefix,
            Delimiter='/' if subfolder_contents else None  # Use delimiter for ls-like behavior
        )
        
        files = []
        common_prefixes = set()
        
        # Handle regular objects
        for obj in response.get('Contents', []):
            key = obj['Key']
            
            # Skip the directory itself
            if key == prefix:
                continue
            
            # For ls-like behavior, skip objects in subdirectories
            if subfolder_contents:
                relative_path = key[len(prefix):]
                if '/' in relative_path:
                    continue
            
            # Get filename from the full path
            filename = key.split('/')[-1]
            
            # Only process files (not empty directory markers)
            if filename:
                files.append(
                    libression.entities.io.ListDirectoryObject(
                        filename=filename,
                        absolute_path=key,
                        size=obj.get('Size', 0),
                        modified=obj['LastModified'],
                        is_dir=False
                    )
                )
        
        # Handle directories
        if subfolder_contents:
            # When using delimiter, S3 returns CommonPrefixes for directories
            for prefix_obj in response.get('CommonPrefixes', []):
                prefix_path = prefix_obj.get('Prefix', '')
                if prefix_path:
                    dirname = prefix_path.rstrip('/').split('/')[-1]
                    files.append(
                        libression.entities.io.ListDirectoryObject(
                            filename=dirname,
                            absolute_path=prefix_path.rstrip('/'),
                            size=0,
                            modified=datetime.datetime.now(),
                            is_dir=True
                        )
                    )
        else:
            # For recursive listing, find implicit directories
            seen_dirs = set()
            for obj in response.get('Contents', []):
                parts = obj['Key'].split('/')
                for i in range(len(parts) - 1):
                    prefix_path = '/'.join(parts[:i+1])
                    if prefix_path and prefix_path not in seen_dirs:
                        seen_dirs.add(prefix_path)
                        files.append(
                            libression.entities.io.ListDirectoryObject(
                                filename=parts[i],
                                absolute_path=prefix_path,
                                size=0,
                                modified=datetime.datetime.now(),
                                is_dir=True
                            )
                        )
        
        return files

    def move(self, source_path: str, destination_path: str) -> None:
        """Move/rename an object in S3"""
        # S3 doesn't have a direct move operation, so copy then delete
        self.copy(source_path, destination_path)
        self.delete(source_path)

    def copy(self, source_path: str, destination_path: str) -> None:
        """Copy an object in S3"""
        copy_source = {
            'Bucket': self.bucket_name,
            'Key': source_path.lstrip('/')
        }
        self.client.copy_object(
            CopySource=copy_source,
            Bucket=self.bucket_name,
            Key=destination_path.lstrip('/')
        )

    def delete(self, filepath: str) -> None:
        """Delete an object from S3"""
        self.client.delete_object(
            Bucket=self.bucket_name,
            Key=filepath.lstrip('/')
        )

    def bytestream(self, filepath: str) -> IO[bytes]:
        """Get a file's contents as a byte stream"""
        try:
            response = self.client.get_object(
                Bucket=self.bucket_name,
                Key=filepath.lstrip('/')
            )
            return response['Body']
        except self.client.exceptions.NoSuchKey:
            raise FileNotFoundError(f"Object {filepath} not found in bucket {self.bucket_name}")
