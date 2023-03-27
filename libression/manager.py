from typing import List, Optional
import io
import logging

import pydantic
from botocore.response import StreamingBody
from wand.image import Image

from libression import config, s3

logger = logging.getLogger(__name__)


def init_buckets() -> None:
    for bucket in (config.CACHE_BUCKET, config.DATA_BUCKET):
        s3.create_bucket(bucket)


def get(s3_key: str):
    return s3.get(key=s3_key, bucket_name=config.DATA_BUCKET)


def ls(prefix_filter: Optional[str] = None):
    return s3.list_objects(
        config.DATA_BUCKET,
        prefix_filter=prefix_filter,
        get_all=True,
    )


def _cache_key(key: str) -> str:
    return f"{key}_{config.CACHE_SUFFIX}"


def to_cache(key: str) -> Optional[bytes]:
    logging.info(f"getting content for {key}")
    original_content = s3.get_body(key=key, bucket_name=config.DATA_BUCKET)
    logging.info(f"generating cache {key}")
    try:
        cached_content = generate_cache(original_content)
        logging.info(f"putting cache {key}")
        s3.put(
            key=_cache_key(key),
            body=cached_content,
            bucket_name=config.CACHE_BUCKET,
        )
        return cached_content

    except Exception as e:
        logger.info(f"Exception: {e}, can't read key {key}...")
        return None


def from_cache(key: str) -> StreamingBody:
    return s3.get_body(
        key=_cache_key(key),
        bucket_name=config.CACHE_BUCKET,
    )


def update_caches(
        list_of_keys: List[str],
        overwrite: bool = False,
) -> dict[str, Optional[bytes]]:
    # limit calling list_object to only once...
    existing_cache = s3.list_objects(bucket=config.CACHE_BUCKET, get_all=True)

    if overwrite:
        cache_to_render = list_of_keys  # overwrite everything!
    else:
        cache_to_render = [
            key for key in list_of_keys
            if _cache_key(key) not in existing_cache
        ]

    output = {key: to_cache(key) for key in cache_to_render}
    logger.info(f"completed checking/generating cache for {len(cache_to_render)} entities...")

    return output


def generate_cache(original_contents: StreamingBody, width: int = config.CACHE_WIDTH) -> bytes:
    with Image(file=original_contents) as img:
        converted = img.convert("jpg")

        width_percent = (width / float(img.width))
        height = int((float(img.height) * float(width_percent)))

        converted.resize(width=width, height=height)

        buf = io.BytesIO()
        converted.save(file=buf)
        byte_im = buf.getvalue()
        return byte_im


class PageParamsRequest(pydantic.BaseModel):
    cur_dir: str
    show_subdirs: bool
    show_hidden_content: bool


class PageParamsResponse(pydantic.BaseModel):
    file_keys: list[str]
    inner_dirs: list[str]
    outer_dir: str


def fetch_page_params(request: PageParamsRequest) -> PageParamsResponse:
    """
    e.g. Given these exist in the "data" bucket:
        folder0/subfolder0/subsubfolder0/file0.png
        folder1/subfolder1/subsubfolder1/file1.png
        folder1/subfolder1/subsubfolder2/file2.png
        folder1/subfolder1/file3.png
        file4.png
    When specified a partial path "folder1/subfolder1",
    this function should return the subdirectories/files of subfolder1, i.e.
    (
        ["folder1/subfolder1/subsubfolder1","folder1/subfolder1/subsubfolder2"],
        ["folder1/subfolder1/file3.png", "folder1/subfolder1/file1", "folder1/subfolder2/file2"],
    )

    When no partial path specified (i.e. root), should return:
    (
        ["folder0", "folder1"],
        ["file4.png", "folder1/subfolder1/file3.png", "folder1/subfolder1/file1", "folder1/subfolder2/file2"],
    )
    """
    object_keys = s3.list_objects(
        config.DATA_BUCKET,
        prefix_filter=request.cur_dir,
        get_all=True,
    )

    dirs, file_keys = _get_dirs_and_file_keys(
        object_keys,
        request.cur_dir,
        request.show_subdirs,
        request.show_hidden_content,
    )

    dir_levels = request.cur_dir.split("/")
    outer_dir = request.cur_dir
    if dir_levels:
        outer_dir = "/".join(dir_levels[:-1])

    return PageParamsResponse(
        file_keys=file_keys,
        inner_dirs=dirs,
        outer_dir=outer_dir,
    )


def _get_dirs_and_file_keys(
        object_keys: list[str],
        cur_dir: str,
        show_subdirs: bool,
        show_hidden_content: bool,
) -> tuple[list[str], list[str]]:

    subdirs = []
    file_keys = []

    for object_key in object_keys:
        object_key_tokens = object_key[len(cur_dir):].split("/")
        prefix = ""
        if cur_dir:
            object_key_tokens = object_key_tokens[1:]  # first token is empty str [""]...remove...
            prefix = f"{cur_dir}/"

        subdir = f"{prefix}{object_key_tokens[0]}"
        if len(object_key_tokens) > 1:
            if subdir not in subdirs:  # this is a subdir in dir of interest, add it...
                subdirs.append(subdir)
        elif len(object_key_tokens) == 1:
            file_keys.append(object_key)
        else:
            logger.error(
                "get_subdirs_and_content called with errors\n"
                f"when calling with root dir\n"
                f"and processing object key {object_key}"
            )

    if show_subdirs:
        file_keys = object_keys

    if not show_hidden_content:
        file_keys = [
            key for key in file_keys
            if not key.split("/")[-1].startswith(".") and key.startswith(cur_dir)
        ]

    return subdirs, file_keys
