#!/usr/bin/env python3.12

import os
import sys
import queue
import threading
import sqlite3
import datetime
import logging
import time
import argparse
import configparser
from concurrent.futures import ThreadPoolExecutor
import warnings
from cryptography.utils import CryptographyDeprecationWarning
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import hashlib

with warnings.catch_warnings(action="ignore", category=CryptographyDeprecationWarning):
    import paramiko

# Read configuration from config.ini
config = configparser.ConfigParser()
config.read('config.ini')

# Configuration
NUM_WORKERS = config.getint('sftpUploader', 'NUM_WORKERS')
SFTP_SERVER = config.get('sftpUploader', 'SFTP_SERVER')
SFTP_PORT = config.getint('sftpUploader', 'SFTP_PORT')
SFTP_USERNAME = config.get('sftpUploader', 'SFTP_USERNAME')
PRIVATE_KEY_PATH = config.get('sftpUploader', 'PRIVATE_KEY_PATH')
KNOWN_HOST_KEY_FINGERPRINT = config.get('sftpUploader', 'KNOWN_HOST_KEY_FINGERPRINT')
SOURCE_FOLDER = config.get('sftpUploader', 'SOURCE_FOLDER')
DB_PATH = config.get('sftpUploader', 'DB_PATH')
LOG_FILE = config.get('sftpUploader', 'LOG_FILE')
MAX_RETRIES = config.getint('sftpUploader', 'MAX_RETRIES')
RETRY_DELAY_BASE = config.getint('sftpUploader', 'RETRY_DELAY_BASE')
MIN_FILE_AGE = config.getint('sftpUploader', 'MIN_FILE_AGE')

# Get logging configuration
log_level_str = config['logging']['level'].upper()
log_level = getattr(logging, log_level_str, logging.DEBUG)

# Setup logging
logging.basicConfig(filename=LOG_FILE, level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger("paramiko").setLevel(logging.WARNING)

# Register adapters and converters for SQLite
def adapt_datetime(dt):
    return dt.isoformat()

def convert_datetime(s):
    return datetime.datetime.fromisoformat(s.decode('utf-8'))

sqlite3.register_adapter(datetime.datetime, adapt_datetime)
sqlite3.register_converter("timestamp", convert_datetime)

def hash_filename(filename):
    return hashlib.sha256(filename.encode('utf-8')).hexdigest()

STATUS_MAPPING = {
    'pending': 0,
    'uploading': 1,
    'uploaded': 2,
    'error': 3
}

def get_status_value(status):
    return STATUS_MAPPING.get(status, -1)

def get_status_string(value):
    for k, v in STATUS_MAPPING.items():
        if v == value:
            return k
    return 'unknown'

def setup_database():
    logging.info("Setting up database.")
    logging.debug("SQLite3 version: %s", sqlite3.sqlite_version)
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS files (filename_hash TEXT PRIMARY KEY, status INTEGER, last_modified TIMESTAMP)')
    conn.commit()
    conn.close()

def get_db_connection():
    return sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)

def setup_sftp_client():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.WarningPolicy())
    private_key = paramiko.RSAKey.from_private_key_file(PRIVATE_KEY_PATH)
    client.connect(SFTP_SERVER, port=SFTP_PORT, username=SFTP_USERNAME, pkey=private_key, timeout=20)
    
    # Log the server's host key (SSL certificate equivalent)
    server_key = client.get_transport().get_remote_server_key()
    key_type = server_key.get_name()
    key_fingerprint = server_key.get_fingerprint().hex()
    logging.debug(f"SFTP connection established. Server key type: {key_type}, Fingerprint: {key_fingerprint}")

    sftp = client.open_sftp()
    return sftp, client

# Create a pool of SFTP connections
sftp_connection_pool = ThreadPoolExecutor(max_workers=(4*(int(NUM_WORKERS))), initializer=setup_sftp_client)

def ensure_sftp_path_exists(sftp, remote_path):
    dirs = []
    while remote_path:
        remote_path, dir_name = os.path.split(remote_path)
        if dir_name:
            dirs.append(dir_name)
        else:
            if remote_path:
                dirs.append(remote_path)
            break
    while dirs:
        remote_path = os.path.join(remote_path, dirs.pop())
        try:
            sftp.stat(remote_path)
        except FileNotFoundError:
            sftp.mkdir(remote_path)

def upload_file(filepath, db_conn, sftp):
    cursor = db_conn.cursor()
    now = datetime.datetime.now()
    filename_hash = hash_filename(filepath)
    cursor.execute('SELECT status FROM files WHERE filename_hash=?', (filename_hash,))
    result = cursor.fetchone()
    if result and get_status_string(result[0]) == 'uploaded':
        logging.debug(f"Skipping {filepath}, already uploaded.")
        return True

    cursor.execute('UPDATE files SET status=?, last_modified=? WHERE filename_hash=?', (get_status_value('uploading'), now, filename_hash))
    db_conn.commit()
    try:
        remote_path = filepath
        ensure_sftp_path_exists(sftp, os.path.dirname(remote_path))
        sftp.put(os.path.join(SOURCE_FOLDER, filepath), remote_path)
        cursor.execute('UPDATE files SET status=?, last_modified=? WHERE filename_hash=?', (get_status_value('uploaded'), now, filename_hash))
        db_conn.commit()
        logging.info(f"Uploaded {filepath}")
        return True
    except Exception as e:
        logging.error(f"Failed to upload {filepath}: {e}")
        cursor.execute('UPDATE files SET status=?, last_modified=? WHERE filename_hash=?', (get_status_value('error'), now, filename_hash))
        db_conn.commit()
        return False

def retry_upload(filepath, retry_count):
    db_conn = get_db_connection()
    sftp, client = sftp_connection_pool.submit(setup_sftp_client).result()
    try:
        success = upload_file(filepath, db_conn, sftp)
        if not success:
            if retry_count < MAX_RETRIES:
                logging.info(f"Retrying upload for {filepath}, attempt {retry_count + 1}")
                time.sleep(RETRY_DELAY_BASE ** retry_count)
                retry_upload(filepath, retry_count + 1)
            else:
                logging.error(f"Failed to upload {filepath} after {MAX_RETRIES} retries.")
    except Exception as e:
        logging.error(f"Error during upload attempt: {e}")
    finally:
        db_conn.close()
        client.close()

def worker(file_queue, stop_event):
    while not stop_event.is_set() or not file_queue.empty():
        try:
            filepath = file_queue.get(timeout=1)
            if filepath is None:  # Stop signal
                break
            retry_upload(filepath, 0)
            file_queue.task_done()
        except queue.Empty:
            continue

def initial_file_scan():
    db_conn = get_db_connection()
    cursor = db_conn.cursor()

    for root, dirs, files in os.walk(SOURCE_FOLDER):
        for name in files:
            filepath = os.path.relpath(os.path.join(root, name), SOURCE_FOLDER)
            filename_hash = hash_filename(filepath)
            mtime = os.path.getmtime(os.path.join(root, name))
            file_age = (datetime.datetime.now() - datetime.datetime.fromtimestamp(mtime)).total_seconds()
            if file_age >= MIN_FILE_AGE:
                cursor.execute('INSERT OR IGNORE INTO files (filename_hash, status, last_modified) VALUES (?, ?, ?)',
                               (filename_hash, get_status_value('pending'), datetime.datetime.fromtimestamp(mtime)))
                logging.info(f"File found and marked as pending: {filepath}")

    db_conn.commit()
    db_conn.close()

class FileEventHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            filepath = os.path.relpath(event.src_path, SOURCE_FOLDER)
            filename_hash = hash_filename(filepath)
            logging.info(f"Detected new file: {filepath}")
            db_conn = get_db_connection()
            cursor = db_conn.cursor()
            mtime = os.path.getmtime(event.src_path)
            cursor.execute('INSERT OR IGNORE INTO files (filename_hash, status, last_modified) VALUES (?, ?, ?)',
                           (filename_hash, get_status_value('pending'), datetime.datetime.fromtimestamp(mtime)))
            db_conn.commit()
            db_conn.close()
            file_queue.put(filepath)

    def on_modified(self, event):
        if not event.is_directory:
            filepath = os.path.relpath(event.src_path, SOURCE_FOLDER)
            filename_hash = hash_filename(filepath)
            logging.info(f"Detected modified file: {filepath}")
            db_conn = get_db_connection()
            cursor = db_conn.cursor()
            mtime = os.path.getmtime(event.src_path)
            cursor.execute('UPDATE files SET status=?, last_modified=? WHERE filename_hash=?',
                           (get_status_value('pending'), datetime.datetime.fromtimestamp(mtime), filename_hash))
            db_conn.commit()
            db_conn.close()
            file_queue.put(filepath)

def main():
    logging.info("SFTP Uploader Tool started.")
    setup_database()
    initial_file_scan()  # Initial file scan to detect existing files

    global file_queue
    file_queue = queue.Queue()
    stop_event = threading.Event()

    threads = [threading.Thread(target=worker, args=(file_queue, stop_event)) for _ in range(NUM_WORKERS)]
    for t in threads:
        t.start()

    # Setup file system event handler
    event_handler = FileEventHandler()
    observer = Observer()
    observer.schedule(event_handler, SOURCE_FOLDER, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Process interrupted by user.")
        stop_event.set()
    finally:
        for _ in range(NUM_WORKERS):
            file_queue.put(None)  # Signal the worker threads to exit
        for t in threads:
            t.join()
        observer.stop()
        observer.join()
        logging.debug("SFTP connections closed.")

if __name__ == "__main__":
    main()
