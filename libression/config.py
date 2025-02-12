import os


DEFAULT_CHUNK_BYTE_SIZE = int(
    os.environ.get("DEFAULT_CHUNK_BYTE_SIZE", 1024 * 1024 * 5)
)  # 5MB

# Nginx/WebDAV
WEBDAV_BASE_URL = os.environ.get("WEBDAV_BASE_URL", "https://localhost:8443")
WEBDAV_URL_PATH = os.environ.get("WEBDAV_URL_PATH", "libression_photos")
WEBDAV_PRESIGNED_URL_PATH = os.environ.get(
    "WEBDAV_PRESIGNED_URL_PATH", "readonly_libression_photos"
)
WEBDAV_VERIFY_SSL = bool(os.environ.get("WEBDAV_VERIFY_SSL", "True").lower() == "true")

WEBDAV_CACHE_URL_PATH = os.environ.get(
    "WEBDAV_CACHE_URL_PATH", "libression_photos_cache"
)
WEBDAV_CACHE_PRESIGNED_URL_PATH = os.environ.get(
    "WEBDAV_CACHE_PRESIGNED_URL_PATH", "readonly_libression_photos_cache"
)

WEBDAV_USER = os.environ.get("WEBDAV_USER", "libression_user")
WEBDAV_PASSWORD = os.environ.get("WEBDAV_PASSWORD", "libression_password")
NGINX_SECURE_LINK_KEY = os.environ.get("NGINX_SECURE_LINK_KEY", "libression_secret_key")

# DB
DB_PATH = os.environ.get("DB_PATH", "libression.db")

# Media Vault
MEDIA_VAULT_MAX_CONCURRENT_UPLOADS = int(
    os.environ.get("MEDIA_VAULT_MAX_CONCURRENT_UPLOADS", 10)
)

# Thumbnails
THUMBNAIL_WIDTH_IN_PIXELS = int(os.environ.get("THUMBNAIL_WIDTH_IN_PIXELS", 400))
THUMBNAIL_FRAME_COUNT = int(os.environ.get("THUMBNAIL_FRAME_COUNT", 5))
