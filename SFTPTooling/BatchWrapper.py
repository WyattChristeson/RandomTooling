#!python

import subprocess
import sys
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_script(script_name: str):
    try:
        logging.info(f"Starting {script_name}")
        execPrefix = sys.prefix
        execPath = os.path.join(execPrefix + "/" + script_name)
        result = subprocess.run([sys.executable, execPath], check=True, capture_output=True, text=True)
        logging.info(f"Completed {script_name} with output:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running {script_name}: {e.stderr}")
        raise

def main():
    try:
        logging.info("Dumping and Uploading")
        run_script("DataCollection.py") #Easily collect data to be be uploaded by the BatchUploader tool
        run_script("BatchUploader.py")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
