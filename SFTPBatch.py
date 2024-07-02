import os
import queue
import threading
import paramiko
import sqlite3
import datetime
import logging
import time
import argparse
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
RETRY_DELAY_BASE = 2  # Base delay in seconds for exponential backoff
MIN_FILE_AGE = 300  # Minimum file age in seconds (5 minutes)

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

    # Log the server's host key (SSL certificate equivalent)
    server_key = client.get_transport().get_remote_server_key()
    key_type = server_key.get_name()
    key_fingerprint = server_key.get_fingerprint().hex()
    logging.info(f"SFTP connection established. Server key type: {key_type}, Fingerprint: {key_fingerprint}")

    sftp = client.open_sftp()
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

def retry_upload(filename, retry_count):
    db_conn = get_db_connection()
    sftp, client = sftp_connection_pool.submit(setup_sftp_client).result()
    try:
        success = upload_file(filename, db_conn, sftp)
        if not success:
            if retry_count < MAX_RETRIES:
                logging.info(f"Retrying upload for {filename}, attempt {retry_count + 1}")
                time.sleep(RETRY_DELAY_BASE ** retry_count)
                retry_upload(filename, retry_count + 1)
            else:
                logging.error(f"Failed to upload {filename} after {MAX_RETRIES} retries.")
    finally:
        db_conn.close()
        client.close()

def worker(file_queue, stop_event):
    while not stop_event.is_set():
        try:
            filename = file_queue.get(timeout=1)
        except queue.Empty:
            continue
        retry_upload(filename, 0)
        file_queue.task_done()

def manual_requeue(start_time, end_time):
    db_conn = get_db_connection()
    cursor = db_conn.cursor()

    cursor.execute("SELECT filename, last_modified FROM files WHERE last_modified BETWEEN ? AND ?", (start_time, end_time))
    files = cursor.fetchall()
    now = datetime.datetime.now()

    for (filename, last_modified) in files:
        file_age = (now - datetime.datetime.strptime(last_modified, '%Y-%m-%d %H:%M:%S.%f')).total_seconds()
        if file_age >= MIN_FILE_AGE:
            cursor.execute("UPDATE files SET status=? WHERE filename=?", ("pending", filename))
            file_queue.put(filename)
            logging.info(f"Re-queued file {filename} for re-upload.")
        else:
            logging.info(f"Skipped re-queueing {filename} because it was modified recently.")

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

    for filename, last_modified in files:
        file_age = (now - datetime.datetime.strptime(last_modified, '%Y-%m-%d %H:%M:%S.%f')).total_seconds()
        if file_age >= MIN_FILE_AGE:
            file_queue.put(filename)
            logging.info(f"Queued file {filename} for upload.")
        else:
            logging.info(f"Skipped file {filename} because it was modified recently.")

    db_conn.close()

def run_daily_batch(stop_event):
    while not stop_event.is_set():
        logging.info("Starting daily batch process.")
        process_files()
        for _ in range(86400):  # Sleep for a day, checking for stop_event every second
            if stop_event.is_set():
                break
            time.sleep(1)

def main():
    setup_database()

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

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Batch process interrupted by user.")
        stop_event.set()
        batch_thread.join()
        for _ in range(NUM_WORKERS):
            file_queue.put(None)
        for t in threads:
            t.join()
        logging.info("SFTP connections closed.")

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