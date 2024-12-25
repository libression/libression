import io
import logging
import libression.entities.io
import libression.thumbnail
import libression.entities.media


logger = logging.getLogger(__name__)

DEFAULT_CACHE_STATIC_SUFFIX = "_thumbnail.jpg"
DEFAULT_CACHE_DYNAMIC_SUFFIX = "_thumbnail.gif"


def _thumbnail_type_from_mime_type(
    mime_type_enum: libression.entities.media.SupportedMimeType
) -> libression.entities.media.SupportedMimeType | None:

    if isinstance(
        mime_type_enum,
        (
            libression.entities.media.OpenCvProccessableImageMimeType,
            libression.entities.media.HeifMimeType,
        ),
    ):
        return libression.entities.media.OpenCvProccessableImageMimeType.JPEG

    elif isinstance(
        mime_type_enum,
        libression.entities.media.AvProccessableMimeType,
    ):
        return libression.entities.media.AvProccessableMimeType.GIF

    return None


def _build_thumbnail_file_key(file_key: str) -> str | None:
    mime_type_enum = _thumbnail_type_from_mime_type(file_key)
    if mime_type_enum == libression.entities.media.OpenCvProccessableImageMimeType.JPEG:
        return f"{file_key}_{DEFAULT_CACHE_STATIC_SUFFIX}"
    elif mime_type_enum == libression.entities.media.AvProccessableMimeType.GIF:
        return f"{file_key}_{DEFAULT_CACHE_DYNAMIC_SUFFIX}"
    return None


class MediaCacher:
    def __init__(
        self,
        data_io_handler: libression.entities.io.IOHandler,
        cache_io_handler: libression.entities.io.IOHandler,
        thumbnail_width_in_pixels: int,
        presigned_url_expires_in_seconds: int = 60*60*24*30,
    ):
        self.data_io_handler = data_io_handler
        self.cache_io_handler = cache_io_handler
        self.thumbnail_width_in_pixels = thumbnail_width_in_pixels


    # Actions that we will probably use as a direct API

    def get_thumbnail_presigned_urls(
        self,
        file_keys: list[str],
    ) -> libression.entities.io.GetUrlsResponse:
        thumbnail_file_keys = [
            _build_thumbnail_file_key(file_key)
            for file_key in file_keys
            if _build_thumbnail_file_key(file_key) is not None
        ]
        return self.cache_io_handler.get_readonly_urls(
            thumbnail_file_keys,
            expires_in_seconds=self.presigned_url_expires_in_seconds,
        )

    def ensure_thumbnail_cache(
        self,
        file_key: str,
    ) -> None:
        self.cache_io_handler.get([file_key])


    ######### TODO: CHECK MULTIPLE FILES INPUT LENGTH MATCHES!!!!        

    def _refresh_cache(
        self,
        file_key: str,
    ) -> io.IOBase | None:

        mime_type_enum = _thumbnail_type_from_mime_type(file_key)
        if mime_type_enum is None:  # skip if not supported
            return None

        original_bytestream = self.original_bytestream(file_key)
        thumbnail = libression.thumbnail.generate(
            original_bytestream,
            self.thumbnail_width_in_pixels,
            mime_type_enum,
        )

        thumbnail_bytestream = io.BytesIO(thumbnail)

        self.cache_io_handler.upload(
            libression.entities.io.FileStreams(
                file_streams={file_key: thumbnail_bytestream}
            )
        )

        thumbnail_bytestream.seek(0)  # reset seek to 0

        return thumbnail_bytestream


    def cache_bytestreams(
        self,
        file_keys: list[str],
    ) -> dict[str, io.IOBase | None]:
        output: dict[str, io.IOBase | None] = dict()
        for file_key in file_keys:
            mime_type_enum = _thumbnail_type_from_mime_type(file_key)
            if mime_type_enum is None:  # skip if not supported
                output[file_key] = None
            else:
                output[file_key] = self.cache_io_handler.get([file_key])
                if output[file_key] is None:
                    output[file_key] = self._refresh_cache(file_key)

        return output


    def copy(
        self,
        file_keys: list[str],
        target_dir_path: str,
        delete_source: bool,
    ) -> None:
        """
        Assume:
        - No duplications in file names
        - All cache is present
        """

        # Check all files exist (and render caches)
        self.cache_bytestreams(file_keys)

        self.data_io_handler.copy(file_keys, target_dir_path)
        self.cache_io_handler.copy(file_keys, target_dir_path, allow_missing=True)

        if delete_source:
            self.delete(file_keys)


    def delete(
        self,
        file_keys: list[str],
    ) -> None:
        self.data_io_handler.delete(file_keys)
        self.cache_io_handler.delete(file_keys, allow_missing=True)
