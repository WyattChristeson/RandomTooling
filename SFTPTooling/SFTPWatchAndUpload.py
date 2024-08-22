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
DATA_RETENTION_DAYS = config.getint('sftpUploader', 'DATA_RETENTION_DAYS')
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

def setup_database():
    logging.info("Setting up database.")
    logging.debug("SQLite3 version: %s", sqlite3.sqlite_version)
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS files (filename TEXT PRIMARY KEY, status TEXT, last_modified TIMESTAMP)')
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
    cursor.execute('SELECT status FROM files WHERE filename=?', (filepath,))
    result = cursor.fetchone()
    if result and result[0] == 'uploaded':
        logging.debug(f"Skipping {filepath}, already uploaded.")
        return True

    cursor.execute('UPDATE files SET status=?, last_modified=? WHERE filename=?', ('uploading', now, filepath))
    db_conn.commit()
    try:
        remote_path = filepath
        ensure_sftp_path_exists(sftp, os.path.dirname(remote_path))
        sftp.put(os.path.join(SOURCE_FOLDER, filepath), remote_path)
        cursor.execute('UPDATE files SET status=?, last_modified=? WHERE filename=?', ('uploaded', now, filepath))
        db_conn.commit()
        logging.info(f"Uploaded {filepath}")
        return True
    except Exception as e:
        logging.error(f"Failed to upload {filepath}: {e}")
        cursor.execute('UPDATE files SET status=?, last_modified=? WHERE filename=?', ('error', now, filepath))
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

def manual_requeue(start_time, end_time):
    db_conn = get_db_connection()
    cursor = db_conn.cursor()

    cursor.execute("SELECT filename, last_modified FROM files WHERE last_modified BETWEEN ? AND ?", (start_time, end_time))
    files = cursor.fetchall()
    now = datetime.datetime.now()

    for (filepath, last_modified) in files:
        file_age = (now - last_modified).total_seconds()
        if file_age >= MIN_FILE_AGE:
            cursor.execute("UPDATE files SET status=? WHERE filename=?", ("pending", filepath))
            file_queue.put(filepath)
            logging.info(f"Re-queued file {filepath} for re-upload.")
        else:
            logging.info(f"Skipped re-queueing {filepath} because it was modified recently.")

    db_conn.commit()
    db_conn.close()

def cleanup_old_files():
    conn = get_db_connection()
    c = conn.cursor()
    cutoff_date = datetime.datetime.now() - datetime.timedelta(days=DATA_RETENTION_DAYS)
    c.execute('DELETE FROM files WHERE last_modified < ?', (cutoff_date,))
    conn.commit()
    conn.close()
    logging.info("Cleaned up old files based on retention policy.")

def process_files():
    db_conn = get_db_connection()
    cursor = db_conn.cursor()
    now = datetime.datetime.now()
    cursor.execute("SELECT filename, last_modified FROM files WHERE status IN ('pending', 'error')")
    files = cursor.fetchall()

    for filepath, last_modified in files:
        file_age = (now - last_modified).total_seconds()
        if file_age >= MIN_FILE_AGE:
            file_queue.put(filepath)
            logging.info(f"Queued file {filepath} for upload.")
        else:
            logging.info(f"Skipped file {filepath} because it was modified recently.")

    db_conn.close()

def run_daily_batch(stop_event):
    logging.info("Starting daily batch process.")
    process_files()
    logging.info("File Processing completed.")

def initial_file_scan():
    db_conn = get_db_connection()
    cursor = db_conn.cursor()

    for root, dirs, files in os.walk(SOURCE_FOLDER):
        for name in files:
            filepath = os.path.relpath(os.path.join(root, name), SOURCE_FOLDER)
            mtime = os.path.getmtime(os.path.join(root, name))
            file_age = (datetime.datetime.now() - datetime.datetime.fromtimestamp(mtime)).total_seconds()
            if file_age >= MIN_FILE_AGE:
                cursor.execute('INSERT OR IGNORE INTO files (filename, status, last_modified) VALUES (?, ?, ?)',
                               (filepath, 'pending', datetime.datetime.fromtimestamp(mtime)))
                logging.info(f"File found and marked as pending: {filepath}")

    db_conn.commit()
    db_conn.close()

class FileEventHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            filepath = os.path.relpath(event.src_path, SOURCE_FOLDER)
            logging.info(f"Detected new file: {filepath}")
            db_conn = get_db_connection()
            cursor = db_conn.cursor()
            mtime = os.path.getmtime(event.src_path)
            cursor.execute('INSERT OR IGNORE INTO files (filename, status, last_modified) VALUES (?, ?, ?)',
                           (filepath, 'pending', datetime.datetime.fromtimestamp(mtime)))
            db_conn.commit()
            db_conn.close()
            file_queue.put(filepath)

    def on_modified(self, event):
        if not event.is_directory:
            filepath = os.path.relpath(event.src_path, SOURCE_FOLDER)
            logging.info(f"Detected modified file: {filepath}")
            db_conn = get_db_connection()
            cursor = db_conn.cursor()
            mtime = os.path.getmtime(event.src_path)
            cursor.execute('UPDATE files SET status=?, last_modified=? WHERE filename=?',
                           ('pending', datetime.datetime.fromtimestamp(mtime), filepath))
            db_conn.commit()
            db_conn.close()
            file_queue.put(filepath)

def main():
    logging.info("Batch Upload process started.")
    setup_database()
    initial_file_scan()  # Initial file scan to detect existing files
    logging.info("Local files scanned, cleaning up old files from the queue based on retention policy.")
    cleanup_old_files()

    global file_queue
    file_queue = queue.Queue()
    stop_event = threading.Event()

    threads = [threading.Thread(target=worker, args=(file_queue, stop_event)) for _ in range(NUM_WORKERS)]
    for t in threads:
        t.start()

    # Run the initial batch process
    process_files()

    # Start the daily batch process
    batch_thread = threading.Thread(target=run_daily_batch, args=(stop_event,))
    batch_thread.start()

    # Setup file system event handler
    event_handler = FileEventHandler()
    observer = Observer()
    observer.schedule(event_handler, SOURCE_FOLDER, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Batch process interrupted by user.")
        stop_event.set()
    finally:
        for _ in range(NUM_WORKERS):
            file_queue.put(None)  # Signal the worker threads to exit
        batch_thread.join()
        for t in threads:
            t.join()
        observer.stop()
        observer.join()
        logging.debug("SFTP connections closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage file uploads to SFTP server.")
    parser.add_argument("--requeue-start", metavar="START", type=str, help="Re-queue files modified starting from this date and time (e.g., '2023-01-01 00:00:00').")
    parser.add_argument("--requeue-end", metavar="END", type=str, help="Re-queue files modified up to this date and time (e.g., '2023-01-01 23:59:59').")
    args = parser.parse_args()

    if args.requeue_start and args.requeue_end:
        try:
            start_time = datetime.datetime.strptime(args.requeue_start, "%Y-%m-%d %H:%M:%S")
            end_time = datetime.datetime.strptime(args.requeue_end, "%Y-%m-%d %H:%M:%S")
            manual_requeue(start_time, end_time)
        except ValueError as e:
            logging.error(f"Failed to parse requeue date and time: {e}")
        sys.exit(0)
    else:
        main()
