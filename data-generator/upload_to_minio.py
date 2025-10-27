"""
Upload generated data to MinIO (S3-compatible storage)
"""

import os
from minio import Minio
from minio.error import S3Error
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def upload_to_minio(
    file_path: str,
    bucket_name: str = "manufacturing-data",
    object_name: str = None
):
    """
    Upload file to MinIO bucket
    
    Args:
        file_path: Path to local file
        bucket_name: MinIO bucket name
        object_name: S3 object name (defaults to filename)
    """
    # Initialize MinIO client
    client = Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ROOT_USER"),  
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),  
        secure=False  # Use HTTP (not HTTPS) for local
    )
    
    # Default object name to filename
    if object_name is None:
        object_name = os.path.basename(file_path)
    
    try:
        # Create bucket if it doesn't exist
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Created bucket: {bucket_name}")
        
        # Upload file
        client.fput_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=file_path
        )
        
        file_size = os.path.getsize(file_path) / 1024 / 1024  # MB
        logger.info(f"Uploaded {file_path} → s3://{bucket_name}/{object_name}")
        logger.info(f"   File size: {file_size:.2f} MB")
        
        # Generate presigned URL (shareable link)
        url = client.presigned_get_object(bucket_name, object_name)
        logger.info(f"   URL: {url}")
        
    except S3Error as e:
        logger.error(f"❌ Upload failed: {e}")
        raise


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Upload data to MinIO")
    parser.add_argument("--file", required=True, help="File to upload")
    parser.add_argument("--bucket", default="manufacturing-data", help="Bucket name")
    
    args = parser.parse_args()
    
    upload_to_minio(args.file, args.bucket)