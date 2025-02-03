# S3 Downloader with Resumable Transfers

A Python-based utility to download all objects from AWS S3 buckets with resumable transfers and configurable behavior. This tool can:

- List all S3 buckets (with optional ignore rules).
- Download files with resumable support (using HTTP range requests).
- Log download progress with human-readable file sizes.
- Optionally delete objects from S3 after a successful full download.
- Load configuration (ignore patterns, target path, deletion options, etc.) from a YAML file.

> **Note:** This project is designed to operate with S3 buckets. It requires appropriate AWS credentials and permissions to list, download, and (optionally) delete objects from S3.

## Features

- **Resumable Downloads:**  
  Continues file transfers where they left off, using S3 range requests, to avoid re-downloading already transferred data.

- **Configurable Behavior:**  
  Loads configuration from a YAML file (`config.yaml`), which allows you to set ignore patterns for bucket names, specify the target download directory, and control deletion behavior.

- **Detailed Logging:**  
  Uses Python’s logging module (with suppression for AWS/boto3 logs) to log progress, decisions, and errors. Log messages include human-readable file sizes in the download progress.

- **Bucket Filtering:**  
  Skip buckets based on user-defined ignore patterns (e.g., names starting with, ending with, or containing specific strings).

## Prerequisites

- **Python 3.7+** (prefer Python 3.11)
- **AWS Credentials:**  
  The script uses the default AWS credential chain (environment variables, shared credentials file, IAM roles, etc.).

- **Required Python Packages:**  
  Install dependencies using `pip`:

    ```bash
    pip install boto3 pyyaml
    or
    pip install -r requirements.txt # preferred method
    ```

## Installation

  1. Clone the repository:
```bash
git clone https://github.com/your-username/s3-downloader.git
cd s3-downloader
````

  1. (Optional) Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
pip install --upgrade pip
pip install boto3 pyyaml
```

## Configuration

Create a config.yaml file in the project root. Below is an example configuration:

```yaml
# config.yaml
ignore_pattern:
  starts_with:
    - cloudtrail-logs
  ends_with:
    - 2029
  contains:
    - Jinya
target_path: ./s3_download
delete_after_download: true
```

* **ignore_pattern**:
Defines patterns for bucket names to ignore. The script checks for buckets starting with, ending with, or containing the specified strings.

* **target_path**:
Specifies the **local directory** where the S3 objects will be downloaded.

* **delete_after_download**:
Set to `true` to delete objects from S3 after a successful download; set to `false` to keep them.

## Usage

Run the script using Python:

```bash
python s3_downloader.py
```

The script will:
1. Load the configuration from config.yaml.
2. List all S3 buckets.
3. Skip buckets matching the ignore patterns.
4. Download objects from each bucket into the target directory.
5. Use resumable downloads to continue interrupted transfers.
6. Log progress details (including file sizes and percentage completed).
7. Delete objects from S3 if the download is successful and if configured to do so.

## Logging

* **Log Output**:
    The script logs messages to standard output. The logging level is set to INFO for project messages and DEBUG for download progress (only if you adjust the logging level).

* **Suppressing External Logs**:
    Log messages from libraries like `boto3`, `botocore`, `s3transfer`, and `urllib3` are suppressed to keep the output clean.

## Contributing

Contributions are welcome! Feel free to fork the repository and submit pull requests with enhancements, bug fixes, or new features.

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Submit a pull request detailing your changes.

## License

This project is licensed under the **MIT License**.




# Happy downloading!