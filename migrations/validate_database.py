"""
PixivUtil2 database validation script.

This will perform several types of validation.

- general file system validation
- database connection validation
- file existence validation
- orphan file scanning
"""

import argparse
import enum
import json
import os
import sqlite3
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional

class ValidationResultType(enum.Enum):

    OK = "OK"
    IMAGE_ID_HAS_NO_MEMBER_ID = "IMAGE_ID_HAS_NO_MEMBER_ID"
    IMAGE_ID_HAS_NO_SAVE_NAME = "IMAGE_ID_HAS_NO_SAVE_NAME"

    INCOMPLETE_DOWNLOAD_MASTER_DNE = "INCOMPLETE_DOWNLOAD_MASTER_DNE" # manga images are missing, including master image.
    INCOMPLETE_DOWNLOAD_MASTER_EXISTS = "INCOMPLETE_DOWNLOAD_MASTER_EXISTS" # only part of manga images downloaded, excluding master image.

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

LOGGER = setup_logger(f"validate_database_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

def inner_main(conn: sqlite3.Connection, root_dir: str):

    # validate database tables exist.
    LOGGER.info("Validating database tables...")
    for table_name in ["pixiv_manga_image", "pixiv_master_image"]:
        try:
            conn.execute(f"SELECT * FROM {table_name} LIMIT 1").fetchall()
        except sqlite3.OperationalError:
            raise AssertionError(f"Table {table_name} does not exist in database!")
    LOGGER.info("Database tables validated.")

    # setup report statistics.
    validation_results_by_image_id: Dict[int, ValidationResultType] = {}
    orphan_files: List[str] = []
    orphan_directories: List[str] = [] # these are empty directories; if they contain files, we check if the files are in the database.

    cursor = conn.cursor()
    LOGGER.info("Initializing validation results...")
    for master_image_row in cursor.execute("SELECT DISTINCT image_id, save_name FROM pixiv_master_image").fetchall():
        image_id: int = master_image_row[0]
        save_name: int = master_image_row[1]
        validation_results_by_image_id[image_id] = ValidationResultType.OK
        assert cursor.execute("SELECT 1 FROM pixiv_manga_image WHERE image_id = ? AND save_name = ? LIMIT 1").fetchone() is not None, f"[{image_id}] pixiv_manga_image does not contain pixiv_master_image."
    LOGGER.info("Validation results initialized.")

    LOGGER.info("Validating master images...")
    for master_image_row in cursor.execute("SELECT image_id, member_id, save_name FROM pixiv_master_image").fetchall():
        image_id: int = master_image_row[0]
        member_id: Optional[int] = master_image_row[1]
        save_name: Optional[str] = master_image_row[2]
        if not member_id:
            validation_results_by_image_id[image_id] = ValidationResultType.IMAGE_ID_HAS_NO_MEMBER_ID
            LOGGER.error(f"[{image_id}] has no member ID.")
        if not save_name:
            validation_results_by_image_id[image_id] = ValidationResultType.IMAGE_ID_HAS_NO_SAVE_NAME
            LOGGER.error(f"[{image_id}] has no save name.")
        if validation_results_by_image_id[image_id] != ValidationResultType.OK:
            continue

        # check if manga images exist in filesystem.
        __manga_image_save_name: str
        __is_incomplete_download: bool = False
        for manga_image_row in cursor.execute("SELECT save_name FROM pixiv_manga_image WHERE image_id = ? ORDER BY page ASC", (image_id,)).fetchall():
            manga_image_save_name: str = manga_image_row[0]
            if not manga_image_save_name:
                LOGGER.error(f"[{image_id}] has no save name.")
                __is_incomplete_download = True
                continue
            __manga_image_save_name: str = manga_image_save_name
            if __manga_image_save_name.endswith(".zip"):
                __manga_image_save_name = os.path.splitext(__manga_image_save_name)[0] + ".gif"
            if not os.path.exists(__manga_image_save_name):
                __is_incomplete_download = True
                LOGGER.error(f"[{image_id}] has incomplete download. Missing {__manga_image_save_name}.")
                break
        del __manga_image_save_name

        # check if master image exists in filesystem, and is contained in manga image.
        __save_name: str = save_name
        if save_name.endswith(".zip"):
            __save_name = os.path.splitext(save_name)[0] + ".gif"
        if not os.path.exists(__save_name):
            validation_results_by_image_id[image_id] = ValidationResultType.INCOMPLETE_DOWNLOAD_MASTER_DNE
            LOGGER.error(f"[{image_id}] has incomplete download. Missing {__save_name}.")
        elif __is_incomplete_download:
            validation_results_by_image_id[image_id] = ValidationResultType.INCOMPLETE_DOWNLOAD_MASTER_EXISTS
            LOGGER.error(f"[{image_id}] has incomplete download. Missing {__save_name}.")
        del __save_name
    LOGGER.info("Master images validation complete.")

    # next scan for files and directories in root directory but not in database.
    # it suffices to match against pixiv_manga_image table.
    LOGGER.info("Scanning for orphan files and directories.")
    for root, dirs, files in os.walk(root_dir):
        
        for dirname in dirs: # only check empty directories in the root directory.
            dir_path: str = os.path.join(root, dirname)
            if os.listdir(dir_path):
                continue
            if cursor.execute("SELECT 1 FROM pixiv_manga_image WHERE INSTR(save_name, ?) > 0 LIMIT 1", (dirname,)).fetchone() is None:
                orphan_directories.append(dir_path)
                LOGGER.warning(f"Found orphan directory: {dir_path}")
        
        for filename in files:
            file_path: str = os.path.join(root, filename)
            __file_path: str = file_path
            if file_path.endswith(".gif"):
                __file_path = os.path.splitext(__file_path)[0] + ".zip"
            if cursor.execute("SELECT 1 FROM pixiv_manga_image WHERE save_name = ? LIMIT 1", (__file_path,)).fetchone() is None:
                orphan_files.append(file_path)
                LOGGER.warning(f"Found orphan file: {file_path}")
    LOGGER.info("Orphan file and directory scanning complete.")

    # close connection to database.
    cursor.close()
    conn.close()

    # summarization stage
    LOGGER.info("Summarizing validation results.")
    results = {
        "orphan_files": orphan_files,
        "orphan_directories": orphan_directories,
    }
    for image_id, result in validation_results_by_image_id.items():
        if result != ValidationResultType.OK:
            if result.value not in results:
                results[result.value] = []
            results[result.value].append(image_id)
    
    with open("validation_result.json", 'w') as f:
        json.dump(results, f, indent=4)
    LOGGER.info("Validation results written to validation_result.json.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=str, default="db.sqlite")
    parser.add_argument("--root-dir", type=str, default="/workdir/downloads")
    args = parser.parse_args()
    database_path: str = args.database
    root_dir: str = args.root_dir
    
    start_time = time.time()

    # check files exist.
    assert os.path.exists(root_dir), f"Root directory {root_dir} does not exist."
    assert os.path.isdir(root_dir), "Root directory is not a directory."
    assert os.path.exists(database_path), f"SQLite database {database_path} does not exist."
    assert os.path.isfile(database_path), f"{database_path} is not a file."

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
        inner_main(conn, root_dir)
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
