import argparse
import json
import os
import shutil
import sqlite3
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional

def setup_logger(log_file_path):
    """Set up logging to both console and file."""
    logger = logging.getLogger('migration')
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

LOGGER = setup_logger(f"remove_corrupted_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

def inner_main(
        conn: sqlite3.Connection, root_dir: str, 
        orphan_files: List[str], orphan_directories: List[str], incomplete_download_master_exists: List[int], incomplete_download_master_dne: List[int],
):
    
    LOGGER.info("Cleaning up orphan files...")
    for orphan_file in orphan_files:
        LOGGER.info(f"Removing orphan file {orphan_file}...")
        os.remove(orphan_file)
    LOGGER.info("Cleaning up orphan directories...")
    for orphan_directory in orphan_directories:
        LOGGER.info(f"Removing orphan directory {orphan_directory}...")
        shutil.rmtree(orphan_directory)
    LOGGER.info("Cleaning up orphan files and directories complete.")

    LOGGER.info("Cleaning up incomplete download master files...")
    image_ids_to_delete: List[int] = incomplete_download_master_exists + incomplete_download_master_dne
    cursor = conn.cursor()
    for image_id in image_ids_to_delete:
        for row in cursor.execute("SELECT save_name FROM pixiv_manga_image WHERE image_id = ?", (image_id,)).fetchall():
            save_name: Optional[str] = row[0]
            __save_name: Optional[str] = save_name
            if __save_name is None:
                continue
            if save_name.endswith(".zip"):
                __save_name = os.path.splitext(save_name)[0] + ".gif"
            
            if os.path.exists(__save_name):
                os.remove(__save_name)
        cursor.execute("DELETE FROM pixiv_manga_image WHERE image_id = ?", (image_id,))
        cursor.execute("DELETE FROM pixiv_master_image WHERE image_id = ?", (image_id,))
        cursor.execute("DELETE FROM pixiv_image_to_tag WHERE image_id = ?", (image_id,))
        conn.commit()
        LOGGER.info(f"[{image_id}] Removed save_name: {__save_name}")
    cursor.close()
    LOGGER.info("Cleaning up incomplete download master files complete.")

    LOGGER.info("Cleaning up empty directories...")
    for root, dirs, files in os.walk(root_dir):
        if not dirs and not files:
            LOGGER.info(f"Removing empty directory {root}...")
            shutil.rmtree(root)
    LOGGER.info("Cleaning up empty directories complete.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=str, default="db.sqlite")
    parser.add_argument("--root-dir", type=str, default="/workdir/downloads")
    parser.add_argument("--validation-result-file", type=str, default="validation_result.json")
    args = parser.parse_args()
    database_path: str = args.database
    root_dir: str = args.root_dir
    validation_result_file: str = args.validation_result_file

    start_time = time.time()

    # check files exist.
    assert os.path.exists(root_dir), f"Root directory {root_dir} does not exist."
    assert os.path.isdir(root_dir), "Root directory is not a directory."
    assert os.path.exists(database_path), f"SQLite database {database_path} does not exist."
    assert os.path.isfile(database_path), f"{database_path} is not a file."
    assert os.path.exists(validation_result_file), f"Validation result file {validation_result_file} does not exist."
    assert os.path.isfile(validation_result_file), f"{validation_result_file} is not a file."

    # load validation result.
    with open(validation_result_file, "r") as f:
        validation_result: Dict[str, List] = json.load(f)
    orphan_files: List[str] = validation_result.get("orphan_files", [])
    orphan_directories: List[str] = validation_result.get("orphan_directories", [])
    incomplete_download_master_exists: List[int] = validation_result.get("INCOMPLETE_DOWNLOAD_MASTER_EXISTS", [])
    incomplete_download_master_dne: List[int] = validation_result.get("INCOMPLETE_DOWNLOAD_MASTER_DNE", [])

    for orphan_file in orphan_files:
        assert os.path.exists(orphan_file), f"Orphan file {orphan_file} does not exist."
    for orphan_directory in orphan_directories:
        assert os.path.exists(orphan_directory), f"Orphan directory {orphan_directory} does not exist."

    # connect to database.
    LOGGER.info(f"Establishing connection to database at {database_path}...")
    conn: sqlite3.Connection
    try:
        conn = sqlite3.connect(database_path)
        conn.execute("SELECT 1")
    except sqlite3.DatabaseError:
        raise AssertionError(f"Cannot verify {database_path} is a valid SQLite database.")
    assert isinstance(conn, sqlite3.Connection), f"Connection to {database_path} is not a valid SQLite connection."
    LOGGER.info("Connection to database established.")

    try:
        inner_main(conn, root_dir, orphan_files, orphan_directories, incomplete_download_master_exists, incomplete_download_master_dne)
    except Exception as e:
        LOGGER.error(f"Validation failed! {e}")
        raise e
    finally:
        conn.close()

    end_time = time.time()
    total_time = end_time - start_time
    LOGGER.info(f"Validation completed in {total_time:.2f} seconds.")

if __name__ == "__main__":
    main()
