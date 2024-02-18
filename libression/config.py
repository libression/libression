import os

# S3
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID", "custom_minioadmin")
S3_SECRET = os.getenv("S3_SECRET", "custom_miniopassword")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
AWS_REGION = os.getenv("LIBRESSION_AWS_REGION", "us-east-2")

CACHE_WIDTH = int(os.getenv("LIBRESSION_CACHE_WIDTH", 400))
CACHE_BUCKET = os.getenv("LIBRESSION_CACHE_BUCKET", "libressioncache")
DATA_BUCKET = os.getenv("LIBRESSION_DATA_BUCKET", "samplephotos")
CACHE_SUFFIX = os.getenv("LIBRESSION_CACHE_SUFFIX", "cache.jpg")
