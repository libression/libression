from typing import Collection, Optional
import logging
import botocore

from libression import config, entities, impression, s3_old

logger = logging.getLogger(__name__)


def init_buckets(
    buckets: Collection = (config.CACHE_BUCKET, config.DATA_BUCKET)
) -> None:

    for bucket in buckets:
        s3.create_bucket(bucket)


def get_content(
    s3_key: str,
    bucket: str = config.DATA_BUCKET,
) -> botocore.response.StreamingBody:
    return s3.get_body(key=s3_key, bucket_name=bucket)


def load_cache(
    key: str,
    bucket: str = config.CACHE_BUCKET,
) -> botocore.response.StreamingBody:
    return s3.get_body(
        key=_cache_key(key),
        bucket_name=bucket,
    )


def update_caches(
    list_of_keys: Collection[str],
    overwrite: bool = False,
    data_bucket=config.DATA_BUCKET,
    cache_bucket=config.CACHE_BUCKET,
) -> dict[str, Optional[bytes]]:

    if overwrite:
        cache_to_render = list_of_keys
    else:
        existing_cache = s3.list_objects(
            bucket=cache_bucket,
            get_all=True,
        )
        cache_to_render = [
            key for key in list_of_keys
            if _cache_key(key) not in existing_cache
        ]

    output = {
        key: _to_cache(key, data_bucket, cache_bucket)
        for key in cache_to_render
    }

    logger.info(
        "completed checking/generating cache"
        f"for {len(cache_to_render)} entities..."
    )

    return output


def move(
    file_keys: Collection[str],
    target_dir: str,
    data_bucket: str = config.DATA_BUCKET,
    cache_bucket: str = config.CACHE_BUCKET,
) -> None:
    """
    Assume:
        - No duplications in file names
        - All cache is present
    """

    copy(file_keys, target_dir, data_bucket, cache_bucket)

    change_mapper = _file_movement_mapper(file_keys, target_dir)

    delete(change_mapper.keys(), data_bucket, cache_bucket)

    logger.info("move files success")


def copy(
    file_keys: Collection[str],
    target_dir: str,
    data_bucket: str = config.DATA_BUCKET,
    cache_bucket: str = config.CACHE_BUCKET,
) -> None:
    """
    Assume:
        - No duplications in file names
        - All cache is present
    """

    change_mapper = _file_movement_mapper(file_keys, target_dir)

    # put data and cache
    for original_path, target_path in change_mapper.items():
        body = s3.get_body(original_path, data_bucket)
        content = body.read()
        s3.put(target_path, content, data_bucket)

        cache_body = s3.get_body(
            _cache_key(original_path),
            cache_bucket,
        )
        cache_content = cache_body.read()
        s3.put(
            _cache_key(target_path),
            cache_content,
            cache_bucket,
        )

    logger.info("file copy success")


def delete(
    file_keys: Collection[str],
    data_bucket: str = config.DATA_BUCKET,
    cache_bucket: str = config.CACHE_BUCKET,
) -> None:
    """
    Assume:
        - No duplications in file names
        - All cache is present
    """

    # delete original files and cache
    if file_keys:
        s3.delete(file_keys, data_bucket)
        original_cache_keys = [_cache_key(x) for x in file_keys]
        s3.delete(original_cache_keys, cache_bucket)

        logger.info("delete files success")
    else:
        logger.info("no file_keys specified for deletion")


def fetch_page_params(
    request: entities.PageParamsRequest,
    data_bucket: str = config.DATA_BUCKET,
) -> entities.PageParamsResponse:
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
        [
            "folder1/subfolder1/file3.png",
            "folder1/subfolder1/file1",
            "folder1/subfolder2/file2"
        ],
    )

    When no partial path specified (i.e. root), should return:
    (
        ["folder0", "folder1"],
        [
            "file4.png",
            "folder1/subfolder1/file3.png",
            "folder1/subfolder1/file1",
            "folder1/subfolder2/file2"
        ],
    )
    """
    object_keys = s3.list_objects(
        data_bucket,
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
    par_dir = request.cur_dir
    if dir_levels:
        par_dir = "/".join(dir_levels[:-1])

    return entities.PageParamsResponse(
        file_keys=file_keys,
        inner_dirs=dirs,
        par_dir=par_dir,
    )


def _cache_key(key: str) -> str:
    return f"{key}_{config.CACHE_SUFFIX}"


def _file_movement_mapper(
    file_keys: Collection[str],
    target_dir: str,
) -> dict[str, str]:

    raw_mapper = {
        k: f"{target_dir}/{k.split('/')[-1]}"
        for k in file_keys
    }

    if len(file_keys) != len(set(raw_mapper.values())):
        raise Exception("file keys duplicated when moving")

    return {
        k: v for k, v in raw_mapper.items()
        if k != v
    }


def _to_cache(
    key: str,
    data_bucket: str,
    cache_bucket: str,
) -> Optional[bytes]:
    logging.info(f"getting content for {key}")
    original_content = s3.get_body(key=key, bucket_name=data_bucket)

    file_format = key.split(".")[-1]

    return impression.to_cache_preloaded(
        _cache_key(key),
        original_content,
        file_format,
        cache_bucket,
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
            object_key_tokens = object_key_tokens[1:]  # remove 1st empty str
            prefix = f"{cur_dir}/"

        subdir = f"{prefix}{object_key_tokens[0]}"
        if len(object_key_tokens) > 1:
            if subdir not in subdirs:  # add subdir in dir of interest
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
            if not key.split("/")[-1].startswith(".")
            and key.startswith(cur_dir)
        ]

    return subdirs, file_keys
