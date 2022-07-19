import logging
from typing import List, Tuple, Optional
import concurrent.futures
from file.cache import generate
from file.s3 import S3
from botocore.response import StreamingBody


CACHE_BUCKET = "libression-cache"
_data_bucket = "test_photos"
CACHE_SUFFIX = "cache.jpg"
logger = logging.getLogger(__name__)


class Organiser:
    def __init__(
            self,
            s3_url: Optional[str] = None,
    ):
        self._data_bucket = _data_bucket
        self._cache_bucket = CACHE_BUCKET
        self._file_io = S3(s3_endpoint_url=s3_url)
        self._cache_suffix = CACHE_SUFFIX

    def _init_buckets(self) -> None:
        existing_buckets = self._file_io.list_buckets()
        for bucket in [self._cache_bucket, self._data_bucket]:
            if bucket not in existing_buckets:
                self._file_io.create_bucket(bucket)

    def list_objects(
            self,
            max_keys: int = 1000,
            prefix_filter: Optional[str] = None,
    ):
        return self._file_io.list_objects(
            self._data_bucket,
            max_keys=max_keys,
            prefix_filter=prefix_filter,
        )

    def _get_cache_key(self, key: str) -> str:
        return f"{key}_{self._cache_suffix}"

    def save_to_cache(self, key: str) -> Optional[bytes]:

        logging.info(f"getting content for {key}")
        original_content = self._file_io.get_object_body(key=key, bucket_name=self._data_bucket)
        logging.info(f"generating cache {key}")
        cached_content = generate(original_content, key=key)

        logging.info(f"putting cache {key}")
        self._file_io.put_object(
            key=self._get_cache_key(key),
            body=cached_content,
            bucket_name=self._cache_bucket,
        )
        return cached_content

    def load_from_cache(
            self,
            key: str,
    ) -> StreamingBody:
        self._init_buckets()
        return self._file_io.get_object_body(
            key=self._get_cache_key(key),
            bucket_name=self._cache_bucket,
        )

    def load_from_data_bucket(
            self,
            key: str,
    ):
        self._init_buckets()

        return self._file_io.get_object(
            key=key,
            bucket_name=self._data_bucket,
        )

    def ensure_cache_bulk(
            self,
            list_of_keys: List[str],
            overwrite: bool = False,
            run_parallel: bool = False,
    ) -> list:
        self._init_buckets()

        # limit calling list_object to only once...
        existing_cache = self._file_io.list_objects(
            bucket=self._cache_bucket,
            get_all=True,
        )

        if overwrite:
            cache_to_render = list_of_keys  # overwrite everything!
        else:
            cache_to_render = [
                key for key in list_of_keys
                if self._get_cache_key(key) not in existing_cache
            ]

        logger.info(f"checking/generating cache for {len(cache_to_render)} entities...")
        if run_parallel:
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                cache_future = [executor.submit(self.save_to_cache, key)
                                for key in cache_to_render]
            waited_cache, _ = concurrent.futures.wait(cache_future)  # wait for all to finish
            output = [x.result() for x in waited_cache]

        else:
            output = [self.save_to_cache(key) for key in cache_to_render]

        logger.info(f"completed checking/generating cache for {len(cache_to_render)} entities...")

        # try dict, if using results to generate phash and updating db?
        return output

    def get_rel_dirs_and_content(
            self,
            get_subdir_content: bool,
            show_hidden_content: bool,
            rel_dir_no_leading_slash: str = "",
    ) -> Tuple[List[str], List[str]]:
        self._init_buckets()

        dirs, file_keys = self._file_io.get_subdirs_and_content(
            self._data_bucket,
            get_subdir_content=get_subdir_content,
            rel_dir_no_leading_slash=rel_dir_no_leading_slash,
        )

        if len(rel_dir_no_leading_slash.split("/")) > 1:
            dirs.insert(0, "/".join(rel_dir_no_leading_slash.split("/")[:-1]))

        if not show_hidden_content:
            file_keys = [
                key for key in file_keys
                if not key.split("/")[-1].startswith(".")
                   and key.startswith(rel_dir_no_leading_slash)
            ]

        return dirs, file_keys
