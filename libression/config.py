import os

# S3
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID", "minioadmin")
S3_SECRET = os.getenv("S3_SECRET", "miniopassword")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://127.0.0.1:9000")
AWS_REGION = "us-east-2"

CACHE_WIDTH = 200

CACHE_BUCKET = "libressioncache"
DATA_BUCKET = "testphotos"
CACHE_SUFFIX = "cache.jpg"
