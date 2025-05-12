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
from typing import Optional

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
    # For a file path like "/workdir/downloads/{4159} ありくい/{8097} ロ/p_0.png"
    # We need to get "{8097} ロ" part, which is the parent directory of the file
    parent_dir = os.path.dirname(dir)  # gets "/workdir/downloads/{4159} ありくい/{8097} ロ"
    match = re.search(r'{(\d+)}', os.path.basename(parent_dir))  # look at just "{8097} ロ"
    if match:
        return int(match.group(1))
    return None

def delete_images_by_member(conn: sqlite3.Connection, member_id: int):
    master_images = conn.execute("SELECT image_id FROM pixiv_master_image WHERE member_id = ?", (member_id,)).fetchall()
    for image_id, in master_images:
        delete_manga_images_by_image_id(conn, image_id)
    conn.execute("DELETE FROM pixiv_master_image WHERE member_id = ?", (member_id,))
    conn.commit()

def delete_manga_images_by_image_id(conn: sqlite3.Connection, image_id: int):
    conn.execute("DELETE FROM pixiv_manga_image WHERE image_id = ?", (image_id,))
    conn.commit()

def delete_manga_image(conn: sqlite3.Connection, image_id: int, page: int):
    conn.execute("DELETE FROM pixiv_manga_image WHERE image_id = ? AND page = ?", (image_id, page))
    conn.commit()

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
        assert image_id is not None, f"[{image_id}] image_id is None"
        assert member_id is not None, f"[{image_id}] member_id is None"
        assert save_name is not None, f"[{image_id}] save_name is None"
        assert image_id == get_image_id(save_name), f"[{image_id}] image_id mismatch: got {get_image_id(save_name)}"
        assert member_id == get_member_id(save_name), f"[{image_id}] member_id mismatch: got {get_member_id(save_name)}"

        # Check for .zip files and convert to .gif for existence check
        check_save_name = save_name.replace('.zip', '.gif') if save_name.endswith('.zip') else save_name
        if not os.path.exists(check_save_name):
            logger.warning(f"[{image_id}] save_name {check_save_name} does not exist (incomplete migration): checking directory.")
            if not os.path.exists(os.path.dirname(check_save_name)):
                logger.error(f"[{image_id}] directory {os.path.dirname(check_save_name)} does not exist")
                if not dry_run:
                    delete_images_by_member(conn, member_id)
                    logger.warning(f"[{image_id}] DELETED ALL IMAGES FOR MEMBER: {member_id}")
            continue
        new_save_name: str = os.path.join(root_dir, str(member_id), str(image_id))
        new_save_name_parent: str = os.path.dirname(new_save_name)
        assert not os.path.exists(new_save_name_parent), f"[{image_id}] directory_to_create {new_save_name_parent} already exists"

        if not dry_run:
            os.makedirs(new_save_name_parent)
        logger.info(f"[{image_id}] CREATE DIRECTORY:            {new_save_name_parent}")

        pixiv_manga_images = conn.execute("SELECT * FROM pixiv_manga_image WHERE image_id = ?", (image_id,)).fetchall()
        for manga_image in pixiv_manga_images:
            page: int = manga_image[1]
            manga_image_save_name: Optional[str] = manga_image[2]
            assert page is not None, f"[{image_id}] page is None"

            if manga_image_save_name is None:
                logger.error(f"[{image_id}] Encountered Null manga_image_save_name for page {page}")
                if not dry_run:
                    delete_manga_image(conn, image_id, page)
                    logger.warning(f"[{image_id}] DELETED MANGA IMAGE:          {manga_image_save_name}")
                continue
            # Check for .zip files and convert to .gif for existence check
            check_manga_save_name = manga_image_save_name.replace('.zip', '.gif') if manga_image_save_name.endswith('.zip') else manga_image_save_name

            if not os.path.exists(check_manga_save_name):
                logger.error(f"[{image_id}] manga_image_save_name {check_manga_save_name} does not exist")
                if not dry_run:
                    delete_manga_image(conn, image_id, page)
                    logger.warning(f"[{image_id}] DELETED MANGA IMAGE:          {manga_image_save_name}")
                continue
            __basename = os.path.basename(manga_image_save_name)
            new_manga_image_save_name: str = os.path.join(new_save_name_parent, __basename)

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
