"""
Script to handle filenameformat migration.

Renames filenameformat  {%member_id%} %artist%/{%image_id%} %title%/p_0%page_number% to %member_id%/%image_id%/p_0%page_number%.
Does not archive or change page filename.
"""

import argparse
import os
import re
import shutil
import sqlite3
import time
import logging
from datetime import datetime

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

def get_member_id(dir: str) -> int:
    """
    Return member ID from directory name.

    Example:
    - a/b/c/{123} member name/{456} image title/p_01.jpg -> 123
    """
    match = re.search(r'{(\d+)}', os.path.dirname(os.path.dirname(dir)))
    if match:
        return int(match.group(1))
    return None

def get_image_id(dir: str) -> int:
    """
    Return image ID from directory name.

    Example:
    - a/b/c/{123} member name/{456} image title/p_01.jpg -> 456
    """
    match = re.search(r'{(\d+)}', os.path.dirname(dir))
    if match:
        return int(match.group(1))
    return None

def update_manga_image(conn: sqlite3.Connection, image_id: int, manga_image_save_name: str, new_manga_image_save_name: str):
    shutil.move(manga_image_save_name, new_manga_image_save_name)
    conn.execute("UPDATE pixiv_manga_image SET save_name = ? WHERE image_id = ?", (new_manga_image_save_name, image_id))
    conn.commit()

def update_master_image(conn: sqlite3.Connection, image_id: int, save_name: str, new_save_name: str):
    shutil.move(save_name, new_save_name)
    conn.execute("UPDATE pixiv_master_image SET save_name = ? WHERE image_id = ?", (new_save_name, image_id))
    conn.commit()

def verify_migration(conn: sqlite3.Connection):
    """Verify that the migration was successful."""
    logger.info("Verifying migration...")
    master_images = conn.execute("SELECT image_id, save_name FROM pixiv_master_image").fetchall()
    errors = 0
    for image_id, save_name in master_images:
        if not os.path.exists(save_name):
            logger.error(f"[{image_id}] Master image file not found: {save_name}")
            errors += 1
        manga_images = conn.execute("SELECT save_name FROM pixiv_manga_image WHERE image_id = ?", (image_id,)).fetchall()
        for (manga_save_name,) in manga_images:
            if not os.path.exists(manga_save_name):
                logger.error(f"[{image_id}] Manga image file not found: {manga_save_name}")
                errors += 1
    if errors == 0:
        logger.info("Migration verification completed successfully!")
    else:
        logger.error(f"Migration verification failed with {errors} errors")
    return errors == 0

def perform_migration(conn: sqlite3.Connection, root_dir: str, dry_run: bool=True):
    # verify that the directories to create exist.
    # basically a dry run.
    pixiv_master_images = conn.execute("SELECT * FROM pixiv_master_image").fetchall()
    for master_image in pixiv_master_images:
        image_id: int       = master_image[0]
        member_id: int      = master_image[1]
        save_name: str      = master_image[3]
        assert image_id
        assert member_id
        assert save_name
        assert image_id == get_image_id(save_name), f"[{image_id}] image_id mismatch: got {get_image_id(save_name)}"
        assert member_id == get_member_id(save_name), f"[{image_id}] member_id mismatch: got {get_member_id(save_name)}"
        assert os.path.exists(save_name), f"[{image_id}] save_name {save_name} does not exist"
        new_save_name: str = os.path.join(root_dir, str(member_id), str(image_id))
        new_save_name_parent: str = os.path.dirname(new_save_name)
        assert not os.path.exists(new_save_name_parent), f"[{image_id}] directory_to_create {new_save_name_parent} already exists"

        if not dry_run:
            os.makedirs(new_save_name_parent)
        logger.info(f"[{image_id}] CREATE DIRECTORY:            {new_save_name_parent}")

        pixiv_manga_images = conn.execute("SELECT * FROM pixiv_manga_image WHERE image_id = ?", (image_id,)).fetchall()
        for manga_image in pixiv_manga_images:
            manga_image_save_name: str = manga_image[2]
            assert os.path.exists(manga_image_save_name), f"[{image_id}] manga_image_save_name {manga_image_save_name} does not exist"
            __basename = os.path.basename(manga_image_save_name)
            new_manga_image_save_name: str = os.path.join(new_save_name_parent, __basename)

            # # this is not needed since the directory shouldn't exist.
            # assert not os.path.exists(file_to_create), f"[{image_id}] file_to_create {file_to_create} already exists"

            # Simulate move and database update.
            if not dry_run:
                update_manga_image(conn, image_id, manga_image_save_name, new_manga_image_save_name)
            logger.info(f"[{image_id}] UPDATE MANGA IMAGE:          {manga_image_save_name} -> {new_manga_image_save_name}")
        if not dry_run:
            update_master_image(conn, image_id, save_name, new_save_name)
        logger.info(f"[{image_id}] UPDATE MASTER IMAGE:         {save_name} -> {new_save_name}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=str, default="db.sqlite")
    parser.add_argument("--root-dir", type=str, default="/workdir/downloads")
    parser.add_argument("--not-dry-run", action="store_true", help="actually perform the migration (MAKE SURE YOU KNOW WHAT YOU ARE DOING)")
    parser.add_argument("--log-file", type=str, default=f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log", 
                        help="Log file path")
    args = parser.parse_args()

    database_path = args.database
    conn = sqlite3.connect(database_path)
    root_dir = args.root_dir
    not_dry_run = args.not_dry_run
    
    # Setup logging
    global logger
    logger = setup_logger(args.log_file)
    logger.info(f"Starting migration script with database: {database_path}, root_dir: {root_dir}")
    
    # VERIFY STAGE.
    logger.info("Running in DRY RUN mode to verify migration plan")
    perform_migration(conn, root_dir, dry_run=True)

    # EXECUTE STAGE.
    if not_dry_run:
        for i in range(20, 0, -1):
            print(f"\rWARNING: THIS WILL ACTUALLY MIGRATE THE DATA. YOU HAVE {i} SECONDS TO CANCEL.", end="", flush=True)
            time.sleep(1)
        logger.info("\n\n         !!!PERFORMING MIGRATION!!!")
        perform_migration(conn, root_dir, dry_run=False)
        verify_migration(conn)
    else:
        logger.info("Dry run completed. Use --not-dry-run to perform the actual migration.")

if __name__ == "__main__":
    main()
