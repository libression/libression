import logging
from typing import List, Tuple, Optional, Iterable
import concurrent.futures
from file.cache import generate
from file import s3
from botocore.response import StreamingBody

logger = logging.getLogger(__name__)
CACHE_BUCKET = "libressioncache"
DATA_BUCKET = "testphotos"
CACHE_SUFFIX = "cache.jpg"


def init_buckets(buckets: Iterable[str] = (CACHE_BUCKET, DATA_BUCKET)) -> None:
    for bucket in buckets:
        s3.create_bucket_if_not_exists(bucket)


def list_objects(
        max_keys: int = 1000,
        prefix_filter: Optional[str] = None,
):
    return s3.list_objects(
        DATA_BUCKET,
        max_keys=max_keys,
        prefix_filter=prefix_filter,
    )


def _get_cache_key(key: str) -> str:
    return f"{key}_{CACHE_SUFFIX}"


def save_to_cache(
        key: str,
        data_bucket: str = DATA_BUCKET,
        cache_bucket: str = CACHE_BUCKET,
) -> Optional[bytes]:
    logging.info(f"getting content for {key}")
    original_content = s3.get_object_body(key=key, bucket_name=data_bucket)
    logging.info(f"generating cache {key}")
    cached_content = generate(original_content, key=key)

    logging.info(f"putting cache {key}")
    s3.put_object(
        key=_get_cache_key(key),
        body=cached_content,
        bucket_name=cache_bucket,
    )
    return cached_content


def load_from_cache(key: str, cache_bucket: str = CACHE_BUCKET) -> StreamingBody:
    return s3.get_object_body(
        key=_get_cache_key(key),
        bucket_name=cache_bucket,
    )


def ensure_cache_bulk(
        list_of_keys: List[str],
        overwrite: bool = False,
        run_parallel: bool = False,
        cache_bucket: str = CACHE_BUCKET,
) -> list:
    # limit calling list_object to only once...
    existing_cache = s3.list_objects(
        bucket=cache_bucket,
        get_all=True,
    )

    if overwrite:
        cache_to_render = list_of_keys  # overwrite everything!
    else:
        cache_to_render = [
            key for key in list_of_keys
            if _get_cache_key(key) not in existing_cache
        ]

    logger.info(f"checking/generating cache for {len(cache_to_render)} entities...")
    if run_parallel:
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            cache_future = [executor.submit(save_to_cache, key)
                            for key in cache_to_render]
        waited_cache, _ = concurrent.futures.wait(cache_future)  # wait for all to finish
        output = [x.result() for x in waited_cache]

    else:
        output = [save_to_cache(key) for key in cache_to_render]

    logger.info(f"completed checking/generating cache for {len(cache_to_render)} entities...")

    # try dict, if using results to generate phash and updating db?
    return output


def get_rel_dirs_and_content(
        get_subdir_content: bool,
        show_hidden_content: bool,
        rel_dir_no_leading_slash: str = "",
        data_bucket: str = DATA_BUCKET,

) -> Tuple[List[str], List[str]]:
    dirs, file_keys = s3.get_subdirs_and_content(
        data_bucket,
        get_subdir_content=get_subdir_content,
        rel_dir_no_leading_slash=rel_dir_no_leading_slash,
    )

    if len(rel_dir_no_leading_slash.split("/")) > 1:
        dirs.insert(0, "/".join(rel_dir_no_leading_slash.split("/")[:-1]))

    if not show_hidden_content:
        file_keys = [
            key for key in file_keys
            if not key.split("/")[-1].startswith(".") and key.startswith(rel_dir_no_leading_slash)
        ]

    return dirs, file_keys
