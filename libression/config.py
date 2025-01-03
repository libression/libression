import os


DEFAULT_CHUNK_BYTE_SIZE = int(
    os.environ.get("DEFAULT_CHUNK_BYTE_SIZE", 1024 * 1024 * 5)
)  # 5MB


# Nginx/WebDAV
WEBDAV_USER = os.environ.get("WEBDAV_USER", "libression_user")
WEBDAV_PASSWORD = os.environ.get("WEBDAV_PASSWORD", "libression_password")
NGINX_SECURE_LINK_KEY = os.environ.get("NGINX_SECURE_LINK_KEY", "libression_secret_key")
