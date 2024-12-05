from enum import Enum
from typing import Optional
import io
import logging
import botocore.response
from pillow_heif import register_heif_opener
from PIL import Image, ImageOps

register_heif_opener()
from libression import config, s3

logger = logging.getLogger(__name__)

class SupportedMimeType(Enum):
    # Images
    JPEG = 'image/jpeg'
    PNG = 'image/png'
    TIFF = 'image/tiff'
    HEIC = 'image/heic'
    # GIF = 'image/gif'
    # WEBP = 'image/webp'
    # HEIF = 'image/heif'

    # Video
    MP4 = 'video/mp4'
    MPEG = 'video/mpeg'
    MOV = 'video/quicktime'
    WEBM = 'video/webm'
    AVI = 'video/x-msvideo'




class FileExtension(Enum):
    JPG = "jpg"
    JPEG = "jpeg"
    PNG = "png"
    TIF = "tif"
    TIFF = "tiff"
    HEIC = "heic"
    '.doc': 'application/msword',
    '.dot': 'application/msword',
    '.wiz': 'application/msword',
    '.nq': 'application/n-quads',
    '.nt': 'application/n-triples',
    '.bin': 'application/octet-stream', '.a': 'application/octet-stream', '.dll': 'application/octet-stream', '.exe': 'application/octet-stream', '.o': 'application/octet-stream', '.obj': 'application/octet-stream', '.so': 'application/octet-stream', '.oda': 'application/oda', '.pdf': 'application/pdf', '.p7c': 'application/pkcs7-mime', '.ps': 'application/postscript', '.ai': 'application/postscript', '.eps': 'application/postscript', '.trig': 'application/trig', '.m3u': 'application/vnd.apple.mpegurl', '.m3u8': 'application/vnd.apple.mpegurl', '.xls': 'application/vnd.ms-excel', '.xlb': 'application/vnd.ms-excel', '.ppt': 'application/vnd.ms-powerpoint', '.pot': 'application/vnd.ms-powerpoint', '.ppa': 'application/vnd.ms-powerpoint', '.pps': 'application/vnd.ms-powerpoint', '.pwz': 'application/vnd.ms-powerpoint', '.wasm': 'application/wasm', '.bcpio': 'application/x-bcpio', '.cpio': 'application/x-cpio', '.csh': 'application/x-csh', '.dvi': 'application/x-dvi', '.gtar': 'application/x-gtar', '.hdf': 'application/x-hdf', '.h5': 'application/x-hdf5', '.latex': 'application/x-latex', '.mif': 'application/x-mif', '.cdf': 'application/x-netcdf', '.nc': 'application/x-netcdf', '.p12': 'application/x-pkcs12', '.pfx': 'application/x-pkcs12', '.ram': 'application/x-pn-realaudio', '.pyc': 'application/x-python-code', '.pyo': 'application/x-python-code', '.sh': 'application/x-sh', '.shar': 'application/x-shar', '.swf': 'application/x-shockwave-flash', '.sv4cpio': 'application/x-sv4cpio', '.sv4crc': 'application/x-sv4crc', '.tar': 'application/x-tar', '.tcl': 'application/x-tcl', '.tex': 'application/x-tex', '.texi': 'application/x-texinfo', '.texinfo': 'application/x-texinfo', '.roff': 'application/x-troff', '.t': 'application/x-troff', '.tr': 'application/x-troff', '.man': 'application/x-troff-man', '.me': 'application/x-troff-me', '.ms': 'application/x-troff-ms', '.ustar': 'application/x-ustar', '.src': 'application/x-wais-source', '.xsl': 'application/xml', '.rdf': 'application/xml', '.wsdl': 'application/xml', '.xpdl': 'application/xml', '.zip': 'application/zip', '.3gp': 'audio/3gpp', '.3gpp': 'audio/3gpp', '.3g2': 'audio/3gpp2', '.3gpp2': 'audio/3gpp2', '.aac': 'audio/aac', '.adts': 'audio/aac', '.loas': 'audio/aac', '.ass': 'audio/aac', '.au': 'audio/basic', '.snd': 'audio/basic', '.mp3': 'audio/mpeg', '.mp2': 'audio/mpeg', '.opus': 'audio/opus', '.aif': 'audio/x-aiff', '.aifc': 'audio/x-aiff', '.aiff': 'audio/x-aiff', '.ra': 'audio/x-pn-realaudio', '.wav': 'audio/x-wav',
    '.avif': 'image/avif', '.bmp': 'image/bmp', '.gif': 'image/gif', '.ief': 'image/ief', '.jpg': 'image/jpeg', '.jpe': 'image/jpeg', '.jpeg': 'image/jpeg', '.heic': 'image/heic', '.heif': 'image/heif',
    '.png': 'image/png', '.svg': 'image/svg+xml', '.tiff': 'image/tiff', '.tif': 'image/tiff', '.ico': 'image/vnd.microsoft.icon', '.ras': 'image/x-cmu-raster', 
    '.pnm': 'image/x-portable-anymap', '.pbm': 'image/x-portable-bitmap', '.pgm': 'image/x-portable-graymap', '.ppm': 'image/x-portable-pixmap', '.rgb': 'image/x-rgb', '.xbm': 'image/x-xbitmap', 
    '.xpm': 'image/x-xpixmap', '.xwd': 'image/x-xwindowdump', '.eml': 'message/rfc822', '.mht': 'message/rfc822', '.mhtml': 'message/rfc822', '.nws': 'message/rfc822', '.css': 'text/css', '.csv': 'text/csv', 
    '.html': 'text/html', '.htm': 'text/html', '.n3': 'text/n3', '.txt': 'text/plain', '.bat': 'text/plain', '.c': 'text/plain', '.h': 'text/plain', '.ksh': 'text/plain', '.pl': 'text/plain', 
    '.srt': 'text/plain', '.rtx': 'text/richtext', '.tsv': 'text/tab-separated-values', '.vtt': 'text/vtt', '.py': 'text/x-python', '.etx': 'text/x-setext', '.sgm': 'text/x-sgml', '.sgml': 'text/x-sgml', 
    '.vcf': 'text/x-vcard', '.xml': 'text/xml', '.mp4': 'video/mp4', '.mpeg': 'video/mpeg', '.m1v': 'video/mpeg', '.mpa': 'video/mpeg', '.mpe': 'video/mpeg', '.mpg': 'video/mpeg',
    '.mov': 'video/quicktime', '.qt': 'video/quicktime', '.webm': 'video/webm', '.avi': 'video/x-msvideo', '.movie': 'video/x-sgi-movie'}


def to_cache_preloaded(
    cache_key: str,
    raw_content: botocore.response.StreamingBody,
    file_format: str,
    cache_bucket: str,
) -> Optional[bytes]:

    cached_content = _generate_cache(
        raw_content,
        file_format=FileExtension(file_format),
    )

    s3.put(
        key=cache_key,
        body=cached_content,
        bucket_name=cache_bucket,
    )
    logging.info(f"saved cache {cache_key}")
    return cached_content


def _generate_cache(
    original_contents: botocore.response.StreamingBody,
    file_format: FileExtension,
    width: int = config.CACHE_WIDTH,
) -> Optional[bytes]:

    image = Image.open(original_contents)
    if file_format in [FileExtension.JPEG, FileExtension.JPG]:
        image = ImageOps.exif_transpose(image)

    return _shrink_image(image.convert('RGB'), fixed_width=width)


def _shrink_image(original_image: Image, fixed_width: int):

    width_percent = (fixed_width / float(original_image.size[1]))
    height = int((float(original_image.size[0]) * float(width_percent)))
    original_image.thumbnail((fixed_width, height))
    buf = io.BytesIO()
    original_image.save(buf, format='JPEG')
    byte_im = buf.getvalue()

    return byte_im
