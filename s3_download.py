import os
import boto3
import yaml
import logging
from botocore.exceptions import ClientError

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("s3transfer").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def format_size(size):
    """
    Convert a file size (in bytes) into a human-readable string.
    For example: 34MB, 5GB, etc.
    """
    # We'll use binary units (multiples of 1024)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.0f}{unit}"
        size /= 1024
    return f"{size:.0f}PB"

def load_config(config_path="config.yaml"):
    """Load configuration from a YAML file."""
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        logger.info(f"Loaded configuration from '{config_path}': {config}")
        return config
    except Exception as e:
        logger.error(f"Error loading config file {config_path}: {e}")
        return {}

def should_ignore_bucket(bucket_name, ignore_patterns):
    """
    Check if the given bucket_name should be ignored based on ignore_patterns.
    Logs which pattern (if any) caused the decision.
    """
    logger.info(f"Evaluating bucket '{bucket_name}' against ignore patterns.")
    for prefix in ignore_patterns.get("starts_with", []):
        if bucket_name.startswith(prefix):
            logger.info(f"Bucket '{bucket_name}' ignored because it starts with '{prefix}'.")
            return True
    for suffix in ignore_patterns.get("ends_with", []):
        if bucket_name.endswith(suffix):
            logger.info(f"Bucket '{bucket_name}' ignored because it ends with '{suffix}'.")
            return True
    for substr in ignore_patterns.get("contains", []):
        if substr in bucket_name:
            logger.info(f"Bucket '{bucket_name}' ignored because it contains '{substr}'.")
            return True
    logger.info(f"Bucket '{bucket_name}' will be processed.")
    return False

def ensure_directory(path):
    """
    Ensures that the given path exists as a directory.
    If the path exists as a file, it renames the file to allow directory creation.
    """
    if os.path.exists(path):
        if os.path.isdir(path):
            return
        else:
            conflict_path = path
            new_conflict_path = path + "_file_conflict"
            try:
                os.rename(conflict_path, new_conflict_path)
                logger.info(f"Renamed conflicting file '{conflict_path}' to '{new_conflict_path}' to create directory.")
            except Exception as e:
                logger.error(f"Error renaming conflicting file {conflict_path}: {e}")
                raise
    os.makedirs(path, exist_ok=True)
    logger.info(f"Ensured directory exists: {path}")

def delete_s3_object(s3_client, bucket_name, key, delete_file: bool = False):
    """Deletes an object from S3 if delete_file is True."""
    if delete_file:
        try:
            s3_client.delete_object(Bucket=bucket_name, Key=key)
            logger.info(f"Deleted s3://{bucket_name}/{key}")
        except ClientError as e:
            logger.error(f"Error deleting s3://{bucket_name}/{key}: {e}")

def list_bucket_objects(s3_client, bucket_name):
    """
    Lists all objects in a bucket.
    Returns a tuple: (file_objects, dir_objects)
      - file_objects: list of objects that do NOT have keys ending with '/'
      - dir_objects: list of objects that represent directory markers (keys ending with '/')
    """
    file_objects = []
    dir_objects = []
    paginator = s3_client.get_paginator('list_objects_v2')
    try:
        for page in paginator.paginate(Bucket=bucket_name):
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith('/'):
                    dir_objects.append(obj)
                else:
                    file_objects.append(obj)
    except ClientError as e:
        logger.error(f"Error listing objects in bucket {bucket_name}: {e}")
    return file_objects, dir_objects

def download_file_resumable(s3_client, bucket_name, key, local_path, expected_size,
                              current_file_number, total_files, downloaded_bytes_total, total_bytes):
    """
    Downloads the S3 object specified by (bucket_name, key) into local_path,
    resuming if a partial file exists.
    
    Returns a tuple (success, new_bytes_downloaded) where:
      - success is True if the file is fully downloaded.
      - new_bytes_downloaded is the number of additional bytes downloaded in this call.
    """
    if os.path.exists(local_path):
        current_local_size = os.path.getsize(local_path)
    else:
        current_local_size = 0

    if current_local_size >= expected_size:
        logger.info(f"File '{key}' already fully downloaded at {local_path}.")
        return True, 0

    file_size_str = format_size(expected_size)
    percentage = int((downloaded_bytes_total / total_bytes) * 100) if total_bytes > 0 else 0
    logger.info(f"Downloading {file_size_str} ({current_file_number}/{total_files} — {percentage}%) {key} ---> {local_path}")

    chunk_size = 8 * 1024 * 1024  # 8 MB
    new_bytes_downloaded = 0
    downloaded = current_local_size

    while downloaded < expected_size:
        end_byte = min(downloaded + chunk_size - 1, expected_size - 1)
        range_header = f"bytes={downloaded}-{end_byte}"
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=key, Range=range_header)
        except ClientError as e:
            logger.error(f"Error downloading range {range_header} for {key}: {e}")
            return False, new_bytes_downloaded

        data = response['Body'].read()
        if not data:
            break

        with open(local_path, "ab") as f:
            f.write(data)

        downloaded += len(data)
        new_bytes_downloaded += len(data)
        current_percentage = int(((downloaded_bytes_total + new_bytes_downloaded) / total_bytes) * 100) if total_bytes > 0 else 0
        logger.debug(f"Progress for {key}: {downloaded}/{expected_size} bytes ({current_percentage}%)")

    if downloaded >= expected_size:
        logger.info(f"Download complete for {key}.")
        return True, new_bytes_downloaded
    else:
        logger.warning(f"Download incomplete for {key}: {downloaded}/{expected_size} bytes.")
        return False, new_bytes_downloaded

def download_bucket_objects(s3_client, bucket_name, download_dir, config):
    """
    Downloads all objects from the given S3 bucket into a local directory.
    Uses resumable downloads so that incomplete transfers can be continued.
    Files are deleted from S3 only after a successful full download.
    Also processes directory marker objects.
    """
    file_objects, dir_objects = list_bucket_objects(s3_client, bucket_name)
    total_files = len(file_objects)
    total_bytes = sum(obj.get('Size', 0) for obj in file_objects)
    delete_file = config.get('delete_after_download', False)
    
    logger.info(f"Bucket '{bucket_name}' has {total_files} file(s) with a total size of {total_bytes} bytes.")
    downloaded_bytes = 0

    for idx, obj in enumerate(file_objects, start=1):
        key = obj['Key']
        file_size = obj.get('Size', 0)
        local_path = os.path.join(download_dir, bucket_name, key)
        local_dir = os.path.dirname(local_path)

        try:
            ensure_directory(local_dir)
        except Exception as e:
            logger.error(f"Skipping key {key} due to error ensuring directory {local_dir}: {e}")
            continue

        success, new_bytes = download_file_resumable(
            s3_client, bucket_name, key, local_path, file_size,
            current_file_number=idx, total_files=total_files,
            downloaded_bytes_total=downloaded_bytes, total_bytes=total_bytes
        )

        if success and os.path.exists(local_path) and os.path.getsize(local_path) >= file_size:
            downloaded_bytes += file_size
            logger.info(f"File {idx}/{total_files} ({key}) downloaded successfully. Total downloaded bytes: {downloaded_bytes}/{total_bytes}.")
            delete_s3_object(s3_client, bucket_name, key, delete_file)
        else:
            logger.warning(f"File {key} was not fully downloaded or is missing. It will not be deleted from S3.")

    for obj in dir_objects:
        key = obj['Key']
        local_path = os.path.join(download_dir, bucket_name, key)
        try:
            if not os.path.exists(local_path):
                os.makedirs(local_path, exist_ok=True)
                logger.info(f"Created directory for key {key}")
        except Exception as e:
            logger.error(f"Error creating directory for key {key}: {e}")
            continue
        delete_s3_object(s3_client, bucket_name, key, delete_file)

def list_and_download_all_buckets(download_dir, ignore_patterns, config):
    """
    Lists all S3 buckets and downloads the contents of each bucket into the specified local directory.
    Uses resumable downloads to allow stopping and starting transfers.
    Files are deleted from S3 only after being fully downloaded.
    Buckets matching the ignore_patterns are skipped.
    """
    s3_client = boto3.client('s3')
    try:
        response = s3_client.list_buckets()
    except ClientError as e:
        logger.error(f"Error listing buckets: {e}")
        return

    buckets = response.get('Buckets', [])
    if not buckets:
        logger.info("No buckets found.")
        return

    for bucket in buckets:
        bucket_name = bucket['Name']
        if should_ignore_bucket(bucket_name, ignore_patterns):
            logger.info(f"Skipping bucket: {bucket_name}")
            continue

        logger.info(f"Processing bucket: {bucket_name}")
        download_bucket_objects(s3_client, bucket_name, download_dir, config)

if __name__ == "__main__":
    config = load_config("config.yaml")
    ignore_patterns = config.get("ignore_pattern", {})
    target_path = config.get("target_path", "./s3_download")
    logger.info(f"Target download path set to: {target_path}")
    
    list_and_download_all_buckets(target_path, ignore_patterns, config)
