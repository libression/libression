import os

# S3
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID", "custom_minioadmin")
S3_SECRET = os.getenv("S3_SECRET", "custom_miniopassword")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
AWS_REGION = "us-east-2"

CACHE_WIDTH = 400

CACHE_BUCKET = "libressioncache"
DATA_BUCKET = "test_photos"
CACHE_SUFFIX = "cache.jpg"
