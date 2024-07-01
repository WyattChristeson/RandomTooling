import os
import queue
import threading
import paramiko
import sqlite3
import datetime
import logging
import time
import argparse
import sys
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Configuration
NUM_WORKERS = 5
SFTP_SERVER = 'sftp.server.com'
SFTP_PORT = 2222  # Custom port
SFTP_USERNAME = 'user'
PRIVATE_KEY_PATH = '/path/to/private/key'
KNOWN_HOST_KEY_FINGERPRINT = 'xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx'
SOURCE_FOLDER = '/path/to/source/folder'
DB_PATH = '/path/to/database.db'
DATA_RETENTION_DAYS = 30
LOG_FILE = '/path/to/logfile.log'

# Setup logging
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def setup_database():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS files (
            filename TEXT PRIMARY KEY,
            status TEXT,
            last_modified TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def get_db_connection():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def setup_sftp_client():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.WarningPolicy())
    private_key = paramiko.RSAKey.from_private_key_file(PRIVATE_KEY_PATH)
    
    try:
        client.connect(SFTP_SERVER, port=SFTP_PORT, username=SFTP_USERNAME, pkey=private_key, timeout=20)
        sftp = client.open_sftp()
        logging.info("SFTP connection established.")
    except paramiko.SSHException as e:
        logging.error(f"SSH connection error: {e}")
        raise
    
    host_keys = client.get_host_keys()
    server_fingerprint = host_keys[SFTP_SERVER]['ssh-rsa'].get_fingerprint().hex()
    if KNOWN_HOST_KEY_FINGERPRINT != server_fingerprint:
        client.close()
        logging.error("Server fingerprint does not match known fingerprint.")
        raise ValueError("Server fingerprint mismatch.")

    return sftp

sftp = setup_sftp_client()

def worker(file_queue):
    db_conn = get_db_connection()
    cursor = db_conn.cursor()
    while True:
        filename = file_queue.get()
        if filename is None:
            db_conn.close()
            break
        upload_file(filename, cursor, db_conn)
        file_queue.task_done()

def upload_file(filename, cursor, db_conn):
    now = datetime.datetime.now()
    cursor.execute('SELECT status FROM files WHERE filename=?', (filename,))
    result = cursor.fetchone()
    if result and result[0] == 'uploaded':
        logging.debug(f"Skipping {filename}, already uploaded.")
        return
    cursor.execute('UPDATE files SET status=?, last_modified=? WHERE filename=?', ('uploading', now, filename))
    db_conn.commit()
    try:
        sftp.put(os.path.join(SOURCE_FOLDER, filename), filename)
        cursor.execute('UPDATE files SET status=?, last_modified=? WHERE filename=?', ('uploaded', now, filename))
        logging.info(f"Uploaded {filename}")
    except Exception as e:
        logging.error(f"Failed to upload {filename}: {e}")
        cursor.execute('UPDATE files SET status=?, last_modified=? WHERE filename=?', ('error', now, filename))
    db_conn.commit()

def parse_delay(delay_str):
    import re
    matches = re.match(r"(\d+)([dhm])", delay_str)
    if not matches:
        return 0
    amount, unit = matches.groups()
    amount = int(amount)
    if unit == 'd':
        return amount * 86400  # days to seconds
    elif unit == 'h':
        return amount * 3600   # hours to seconds
    elif unit == 'm':
        return amount * 60     # minutes to seconds
    return 0

def manual_requeue(filename, delay):
    db_conn = get_db_connection()
    cursor = db_conn.cursor()
    delay_seconds = parse_delay(delay)
    time.sleep(delay_seconds)
    cursor.execute("UPDATE files SET status=? WHERE filename=?", ("pending", filename))
    db_conn.commit()
    file_queue.put(filename)
    db_conn.close()
    logging.info(f"Manually re-queued file {filename} with a delay of {delay}.")

file_queue = queue.Queue()
threads = [threading.Thread(target=worker, args=(file_queue,)) for _ in range(NUM_WORKERS)]
for t in threads:
    t.start()

class Watcher:
    def __init__(self, directory, queue):
        self.observer = Observer()
        self.directory = directory
        self.queue = queue

    def run(self):
        event_handler = Handler(self.queue)
        self.observer.schedule(event_handler, self.directory, recursive=True)
        self.observer.start()

class Handler(FileSystemEventHandler):
    def __init__(self, queue):
        self.queue = queue

    def on_created(self, event):
        if not event.is_directory:
            self.queue.put(event.src_path)
            logging.debug(f"New file detected: {event.src_path}")

# Parse command line arguments
parser = argparse.ArgumentParser(description="Manage file uploads to SFTP server.")
parser.add_argument("--requeue", metavar="FILENAME", type=str, help="Re-queue a file for uploading.")
parser.add_argument("--delay", metavar="DELAY", type=str, default="0", help="Delay before re-queuing the file (e.g., 30m, 2h, 1d).")
args = parser.parse_args()

if args.requeue:
    manual_requeue(args.requeue, args.delay)
    sys.exit(0)  # Exit after handling the CLI command

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
    sftp.close()
    logging.info("SFTP connection closed.")
