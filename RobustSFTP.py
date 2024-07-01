import os
import queue
import threading
import paramiko
import sqlite3
import datetime
import logging
import time
import argparse
import re
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from concurrent.futures import ThreadPoolExecutor

# Configuration
NUM_WORKERS = 5
SFTP_SERVER = 'sftp.server.com'
SFTP_PORT = 2222
SFTP_USERNAME = 'user'
PRIVATE_KEY_PATH = '/path/to/private/key'
KNOWN_HOST_KEY_FINGERPRINT = 'xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx'
SOURCE_FOLDER = '/path/to/source/folder'
DB_PATH = '/path/to/database.db'
DATA_RETENTION_DAYS = 30
LOG_FILE = '/path/to/logfile.log'
MAX_RETRIES = 5
RETRY_DELAY_BASE = 2  

# Setup logging
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_database():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS files (filename TEXT PRIMARY KEY, status TEXT, last_modified TIMESTAMP)')
    conn.commit()
    conn.close()

def get_db_connection():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def setup_sftp_client():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.WarningPolicy())
    private_key = paramiko.RSAKey.from_private_key_file(PRIVATE_KEY_PATH)
    client.connect(SFTP_SERVER, port=SFTP_PORT, username=SFTP_USERNAME, pkey=private_key, timeout=20)
    sftp = client.open_sftp()
    logging.info("SFTP connection established.")
    return sftp, client

# Create a pool of SFTP connections
sftp_connection_pool = ThreadPoolExecutor(max_workers=NUM_WORKERS, initializer=setup_sftp_client)

def upload_file(filename, db_conn, sftp):
    cursor = db_conn.cursor()
    now = datetime.datetime.now()
    cursor.execute('SELECT status FROM files WHERE filename=?', (filename,))
    result = cursor.fetchone()
    if result and result[0] == 'uploaded':
        logging.debug(f"Skipping {filename}, already uploaded.")
        return True

    cursor.execute('UPDATE files SET status=?, last_modified=? WHERE filename=?', ('uploading', now, filename))
    db_conn.commit()
    try:
        sftp.put(os.path.join(SOURCE_FOLDER, filename), filename)
        cursor.execute('UPDATE files SET status=?, last_modified=? WHERE filename=?', ('uploaded', now, filename))
        db_conn.commit()
        logging.info(f"Uploaded {filename}")
        return True
    except Exception as e:
        logging.error(f"Failed to upload {filename}: {e}")
        cursor.execute('UPDATE files SET status=?, last_modified=? WHERE filename=?', ('error', now, filename))
        db_conn.commit()
        return False

def worker(file_queue):
    while True:
        filename = file_queue.get()
        if filename is None:
            break
        db_conn = get_db_connection()
        sftp, client = sftp_connection_pool.submit(setup_sftp_client).result()
        try:
            success = upload_file(filename, db_conn, sftp)
            if not success:
                logging.debug(f"Retrying upload for {filename}")
                time.sleep(RETRY_DELAY_BASE)
                file_queue.put(filename)  # Requeue for retry
        finally:
            db_conn.close()
            client.close()
        file_queue.task_done()

def cleanup_old_files():
    conn = get_db_connection()
    c = conn.cursor()
    cutoff_date = datetime.datetime.now() - datetime.timedelta(days=DATA_RETENTION_DAYS)
    c.execute('DELETE FROM files WHERE last_modified < ?', (cutoff_date,))
    conn.commit()
    conn.close()
    logging.info("Cleaned up old files based on retention policy.")

def run_cleanup_every_day():
    while True:
        cleanup_old_files()
        time.sleep(86400)  # Sleep for a day

cleanup_thread = threading.Thread(target=run_cleanup_every_day)
cleanup_thread.start()

file_queue = queue.Queue()
threads = [threading.Thread(target=worker, args=(file_queue,)) for _ in range(NUM_WORKERS)]
for t in threads:
    t.start()

class Watcher:
    def __init__(self, directory, queue):
        self.observer = Observer()
        this.directory = directory
        this.queue = queue

    def run(self):
        event_handler = Handler(this.queue)
        this.observer.schedule(event_handler, this.directory, recursive=True)
        this.observer.start()

class Handler(FileSystemEventHandler):
    def __init__(self, queue):
        this.queue = queue

    def on_created(self, event):
        if not event.is_directory:
            this.queue.put(event.src_path)
            logging.debug(f"New file detected: {event.src_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage file uploads to SFTP server.")
    parser.add_argument("--requeue", metavar="FILENAME", type=str, help="Re-queue a file for uploading.")
    parser.add_argument("--delay", metavar="DELAY", type=str, default="0", help="Delay before re-queuing the file.")
    args = parser.parse_args()

    if args.requeue:
        manual_requeue(args.requeue, args.delay)
        sys.exit(0)

    watcher = Watcher(SOURCE_FOLDER, file_queue)
    watcher.run()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Service interrupted by user.")
    finally:
        for _ in range(NUM_WORKERS):
            file_queue.put(None)
        for t in threads:
            t.join()
        logging.info("SFTP connection closed.")
