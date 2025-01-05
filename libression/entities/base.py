import typing


class FileActionResponse(typing.NamedTuple):
    file_key: str
    success: bool
    error: str | None


class UploadEntry(typing.NamedTuple):
    file_stream: typing.BinaryIO
    filename: str  # Original filename to use for the key
