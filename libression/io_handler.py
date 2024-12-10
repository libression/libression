import io
import entities

class S3IOHandler(entities.IOHandler):
    def upload(self, key: str, body: bytes, bucket_name: str) -> None:
        # Implement S3 upload logic
        pass

    def list_objects(self, bucket_name: str) -> list[str]:
        # Implement S3 list objects logic
        pass

    def move(self, source_key: str, destination_key: str, bucket_name: str) -> None:
        # Implement S3 move logic
        pass

    def copy(self, source_key: str, destination_key: str, bucket_name: str) -> None:
        # Implement S3 copy logic
        pass

    def delete(self, key: str, bucket_name: str) -> None:
        # Implement S3 delete logic
        pass

    def bytestream(self, key: str, bucket_name: str) -> io.IOBase:
        # Implement S3 bytestream logic
        pass


class WebDAVIOHandler(entities.IOHandler):
    def upload(self, key: str, body: bytes, bucket_name: str) -> None:
        # Implement WebDAV upload logic
        pass

    def list_objects(self, bucket_name: str) -> list[str]:
        # Implement WebDAV list objects logic
        pass

    def move(self, source_key: str, destination_key: str, bucket_name: str) -> None:
        # Implement WebDAV move logic
        pass

    def copy(self, source_key: str, destination_key: str, bucket_name: str) -> None:
        # Implement WebDAV copy logic
        pass

    def delete(self, key: str, bucket_name: str) -> None:
        # Implement WebDAV delete logic
        pass

    def bytestream(self, key: str, bucket_name: str) -> io.IOBase:
        # Implement WebDAV bytestream logic
        pass
