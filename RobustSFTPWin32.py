import os
import queue
import threading
import paramiko
import sqlite3
import datetime
import logging
import time
import win32serviceutil
import win32service
import win32event
import servicemanager
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from concurrent.futures import ThreadPoolExecutor

# Configuration
NUM_WORKERS = 5
SFTP_SERVER = 'sftp.server.com'
SFTP_PORT = 2222
SFTP_USERNAME = 'user'
PRIVATE_KEY_PATH = 'path/to/private/key'
KNOWN_HOST_KEY_FINGERPRINT = 'xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx'
SOURCE_FOLDER = 'path/to/source/folder'
DB_PATH = 'path/to/database.db'
DATA_RETENTION_DAYS = 30
LOG_FILE = 'path/to/logfile.log'
MAX_RETRIES = 5
RETRY_DELAY_BASE = 2  # Base delay in seconds for exponential backoff
MIN_FILE_AGE = 300  # Minimum file age in seconds (5 minutes)

# Setup logging
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class MyService(win32serviceutil.ServiceFramework):
    _svc_name_ = "MyPythonService"
    _svc_display_name_ = "My Python Service"
    _svc_description_ = "This service uploads files to an SFTP server upon file creation."

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.stop_requested = False

    def SvcStop(self):
        self.stop_requested = True
        win32event.SetEvent(self.hWaitStop)
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        self.main()

    def setup_database(self):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS files (filename TEXT PRIMARY KEY, status TEXT, last_modified TIMESTAMP)')
        conn.commit()
        conn.close()

    def get_db_connection(self):
        return sqlite3.connect(DB_PATH, check_same_thread=False)

    def setup_sftp_client(self):
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

    def upload_file(self, filename, db_conn, sftp):
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

    def retry_upload(self, filename, retry_count):
        db_conn = self.get_db_connection()
        sftp, client = sftp_connection_pool.submit(self.setup_sftp_client).result()
        try:
            success = self.upload_file(filename, db_conn, sftp)
            if not success:
                if retry_count < MAX_RETRIES:
                    logging.info(f"Retrying upload for {filename}, attempt {retry_count + 1}")
                    time.sleep(RETRY_DELAY_BASE ** retry_count)
                    self.retry_upload(filename, retry_count + 1)
                else:
                    logging.error(f"Failed to upload {filename} after {MAX_RETRIES} retries.")
        finally:
            db_conn.close()
            client.close()

    def worker(self, file_queue):
        while not self.stop_requested:
            filename = file_queue.get()
            if filename is None:
                break
            self.retry_upload(filename, 0)
            file_queue.task_done()

    def process_files(self):
        db_conn = self.get_db_connection()
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

    def cleanup_old_files(self):
        conn = self.get_db_connection()
        c = conn.cursor()
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=DATA_RETENTION_DAYS)
        c.execute('DELETE FROM files WHERE last_modified < ?', (cutoff_date,))
        conn.commit()
        conn.close()
        logging.info("Cleaned up old files based on retention policy.")

    def run_cleanup_every_day(self):
        while not self.stop_requested:
            self.cleanup_old_files()
            time.sleep(86400)  # Sleep for a day

    def main(self):
        self.setup_database()

        cleanup_thread = threading.Thread(target=self.run_cleanup_every_day)
        cleanup_thread.start()

        global file_queue
        file_queue = queue.Queue()
        threads = [threading.Thread(target=self.worker, args=(file_queue,)) for _ in range(NUM_WORKERS)]
        for t in threads:
            t.start()

        # Process files at startup
        self.process_files()

        # Set up file monitoring
        self.watcher = Watcher(SOURCE_FOLDER, file_queue)
        self.watcher.run()

        # Wait for stop signal
        win32event.WaitForSingleObject(self.hWaitStop, win32event.INFINITE)

        # Stop the threads and clean up
        for _ in range(NUM_WORKERS):
            file_queue.put(None)
        for t in threads:
            t.join()
        logging.info("SFTP connection closed.")

class Watcher:
    def __init__(self, directory, queue):
        self.observer = Observer()
        self.directory = directory
        self.queue = queue

    def run(self):
        event_handler = Handler(self.queue)
        self.observer.schedule(event_handler, self.directory, recursive=True)
        self.observer.start()

    def stop(self):
        self.observer.stop()
        self.observer.join()

class Handler(FileSystemEventHandler):
    def __init__(self, queue):
        self.queue = queue

    def on_created(self, event):
        if not event.is_directory:
            now = datetime.datetime.now()
            file_mtime = datetime.datetime.fromtimestamp(os.path.getmtime(event.src_path))
            if (now - file_mtime).total_seconds() >= MIN_FILE_AGE:
                self.queue.put(event.src_path)
                logging.debug(f"New file detected and queued: {event.src_path}")
            else:
                logging.debug(f"New file detected but not queued due to recent modification: {event.src_path}")

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(MyService)
