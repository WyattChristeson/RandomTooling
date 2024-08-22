# SFTP Uploader Tool

## Overview

The SFTP Uploader Tool is a Python-based utility designed to monitor a local directory for new or modified files and upload them to a specified SFTP server.

There are three flavors of this tool:

Batch Uploader - will scan a directory for files, and can easily be controlled with a cron job, has the fewest dependencies.
SFTPWatchAndUpload - Like batch uploader, but typically used as a service instead of a cronjob
SFTPWatchHashAndUpload - Just like SFTPWatchAndUpload, except it hashes filenames in the application database which may be beneficial for security or database memory footprint reasons. This application database is incompatible with the other two flavors of the tool. 

## Features

- Monitors a local directory for new or modified files.
- Uploads files to a specified SFTP server.
- Supports retry logic for failed uploads.
- Cleans up old records based on a configurable retention policy.
- Logs all activities for easy monitoring and debugging.

## Requirements

- Python 3.12 or higher
- Required Python packages for all three versions of this tool:
  - `paramiko`
  - `configparser`
- Required Python packages for persistently uploading:
  - `watchdog`
- Required Python packages for hashing file names:
  - `cryptography`

## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/yourusername/sftp-uploader-tool.git
    cd sftp-uploader-tool
    ```

2. Install the required Python packages:
    ```sh
    pip install -r requirements.txt
    ```

3. Configure the tool by editing the `config.ini` file:
    ```ini
    [DEFAULT]
    NUM_WORKERS = 4
    SFTP_SERVER = your.sftp.server
    SFTP_PORT = 22
    SFTP_USERNAME = your_username
    PRIVATE_KEY_PATH = /path/to/your/private/key
    KNOWN_HOST_KEY_FINGERPRINT = your_known_host_key_fingerprint
    SOURCE_FOLDER = /path/to/source/folder
    DB_PATH = /path/to/database.db
    DATA_RETENTION_DAYS = 30
    LOG_FILE = /path/to/logfile.log
    MAX_RETRIES = 3
    RETRY_DELAY_BASE = 2
    MIN_FILE_AGE = 60

    [logging]
    level = INFO
    ```

## Usage

1. Run the tool:
    ```sh
    python BatchWrapper.py
    ```

2. To manually requeue files for upload based on modification time, use the `--requeue-start` and `--requeue-end` options:
    ```sh
    python BatchWrapper.py --requeue-start "2023-01-01 00:00:00" --requeue-end "2023-01-01 23:59:59"
    ```

## Logging

The tool logs all activities to the file specified in the `LOG_FILE` configuration option. The log level can be adjusted in the `config.ini` file under the `[logging]` section.

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request with your changes.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
