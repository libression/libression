import enum
import mimetypes
import typing


class SupportedMimeType(enum.Enum):
    @classmethod
    def from_value(cls, value: str) -> typing.Union["SupportedMimeType", None]:
        for member in cls:
            if member.value == value:
                return member
        return None

    @classmethod
    def from_filename(cls, filename: str) -> typing.Union["SupportedMimeType", None]:
        mime_type, _ = mimetypes.guess_type(filename)
        if mime_type is None:
            return None
        return cls.from_value(mime_type)

    @classmethod
    def best_guess(
        cls,
        filename: str,
        given_mime_type_str: str | None,
    ) -> typing.Union["SupportedMimeType", None]:
        output: SupportedMimeType | None = None

        if given_mime_type_str is not None:  # first priority is given mime type
            output = cls.from_value(given_mime_type_str)

        if output is None and filename is not None:  # second priority is filename
            output = cls.from_filename(filename)

        return output

    # Heif processing (special containers)
    HEIC = "image/heic"
    HEIF = "image/heif"

    # OpenCv proccessing (photos)
    JPEG = "image/jpeg"
    PNG = "image/png"
    TIFF = "image/tiff"
    WEBP = "image/webp"
    SVG_XML = "image/svg+xml"
    ICON = "image/vnd.microsoft.icon"
    BMP = "image/x-ms-bmp"
    X_C_MU_RASTER = "image/x-cmu-raster"
    X_PORTABLE_ANYMAP = "image/x-portable-anymap"
    X_PORTABLE_BITMAP = "image/x-portable-bitmap"
    X_PORTABLE_GRAYMAP = "image/x-portable-graymap"
    X_PORTABLE_PIXMAP = "image/x-portable-pixmap"
    X_RGB = "image/x-rgb"
    X_XBITMAP = "image/x-xbitmap"
    X_XPIXMAP = "image/x-xpixmap"
    X_XWINDOWDUMP = "image/x-xwindowdump"

    # AV processing (videos)
    GIF = "image/gif"  # imdecode in opencv doesn't handle GIFs well
    MP4 = "video/mp4"
    MPEG = "video/mpeg"
    QUICKTIME = "video/quicktime"
    WEBM = "video/webm"
    X_MS_VIDEO = "video/x-msvideo"  # AVI


HEIC_PROCESSING_MIME_TYPES = [
    SupportedMimeType.HEIC,
    SupportedMimeType.HEIF,
]

OPEN_CV_PROCESSING_MIME_TYPES = [
    SupportedMimeType.JPEG,
    SupportedMimeType.PNG,
    SupportedMimeType.TIFF,
    SupportedMimeType.WEBP,
    SupportedMimeType.SVG_XML,
    SupportedMimeType.ICON,
    SupportedMimeType.BMP,
]

AV_PROCESSING_MIME_TYPES = [
    SupportedMimeType.GIF,
    SupportedMimeType.MP4,
    SupportedMimeType.MPEG,
    SupportedMimeType.QUICKTIME,
    SupportedMimeType.WEBM,
    SupportedMimeType.X_MS_VIDEO,
]
