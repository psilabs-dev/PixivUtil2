"""
Script to handle archive mode migration from directory.

Maintains same filename format, compresses each artwork directory into a zip file.

Required filenameformat: %member_id%/%image_id%/p_0%page_number% (i.e. %member_id%/%image_id%.zip with pages of format %p_0%page_number%).

This will:
- relocate all artwork directories a/b/c to archives a/b/c.zip, and update database save_name at pixiv_master_image
- update all manga_image save_name from a/b/c/p_001.jpg to p_001.jpg and their directory-less equivalents.
- remove all artwork directories.
"""

import argparse
import os
import re
import shutil
import sqlite3
import time
import logging
import zipfile
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
    # For a file path like "/workdir/downloads/{4159} ありくい/{8097} ロ/p_0.png"
    # We need to get "{8097} ロ" part, which is the parent directory of the file
    parent_dir = os.path.dirname(dir)  # gets "/workdir/downloads/{4159} ありくい/{8097} ロ"
    match = re.search(r'{(\d+)}', os.path.basename(parent_dir))  # look at just "{8097} ロ"
    if match:
        return int(match.group(1))
    return None

def update_manga_image(conn: sqlite3.Connection, image_id: int, new_manga_image_save_name: str):
    conn.execute("UPDATE pixiv_manga_image SET save_name = ? WHERE image_id = ?", (new_manga_image_save_name, image_id))
    conn.commit()

def update_master_image(conn: sqlite3.Connection, image_id: int, new_save_name: str):
    conn.execute("UPDATE pixiv_master_image SET save_name = ? WHERE image_id = ?", (new_save_name, image_id))
    conn.commit()

def verify_migration(conn: sqlite3.Connection):
    """Verify that the migration was successful."""
    logger.info("Verifying migration...")
    master_images = conn.execute("SELECT image_id, save_name FROM pixiv_master_image").fetchall()
    errors = 0
    for image_id, save_name in master_images:
        if not os.path.exists(save_name):
            logger.error(f"[{image_id}] Master image file (zip archive) not found: {save_name}")
            errors += 1
            continue
        # For zip files, we need to verify the internal structure
        try:
            with zipfile.ZipFile(save_name, 'r') as zip_ref:
                manga_images = conn.execute("SELECT save_name FROM pixiv_manga_image WHERE image_id = ?", (image_id,)).fetchall()
                for (manga_save_name,) in manga_images:
                    # For manga images, we just check if they exist in the zip
                    if manga_save_name not in zip_ref.namelist():
                        logger.error(f"[{image_id}] Manga image file not found in zip: {manga_save_name}")
                        errors += 1
        except zipfile.BadZipFile:
            logger.error(f"[{image_id}] Invalid zip file: {save_name}")
            errors += 1
    
    if errors == 0:
        logger.info("Migration verification completed successfully!")
    else:
        logger.error(f"Migration verification failed with {errors} errors")
    return errors == 0

def perform_migration(root_dir: str,conn: sqlite3.Connection, dry_run: bool=True):
    pixiv_master_images = conn.execute("SELECT * FROM pixiv_master_image").fetchall()
    for master_image in pixiv_master_images:
        image_id: int       = master_image[0]
        member_id: int      = master_image[1]
        save_name: str      = master_image[3]
        
        # Validate input
        assert image_id
        assert member_id
        assert os.path.exists(save_name), f"[{image_id}] save_name {save_name} does not exist"
        assert os.path.isfile(save_name), f"[{image_id}] save_name {save_name} is not a file"
        assert image_id == get_image_id(save_name), f"[{image_id}] image_id mismatch: got {get_image_id(save_name)}"
        assert member_id == get_member_id(save_name), f"[{image_id}] member_id mismatch: got {get_member_id(save_name)}"

        zip_file_path = f"{os.path.dirname(save_name)}.zip"
        assert not os.path.exists(zip_file_path), f"[{image_id}] zip file already exists: {zip_file_path}"
        assert os.path.join(root_dir, str(member_id), "pixiv_" + str(image_id) + ".zip") == zip_file_path, f"[{image_id}] zip file path mismatch: got {zip_file_path}"

        logger.info(f"[{image_id}] PREPARING TO CREATE ZIP:     {zip_file_path}")
        pixiv_manga_images = conn.execute("SELECT * FROM pixiv_manga_image WHERE image_id = ?", (image_id,)).fetchall()
        if not dry_run:
            with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for manga_image in pixiv_manga_images:
                    manga_image_save_name: str = manga_image[2]
                    assert os.path.exists(manga_image_save_name), f"[{image_id}] manga_image_save_name {manga_image_save_name} does not exist"
                    filename = os.path.basename(manga_image_save_name)
                    zipf.write(manga_image_save_name, filename)
                    update_manga_image(conn, image_id, filename)
                    logger.info(f"[{image_id}] ADDED TO ZIP AND UPDATED DB: {manga_image_save_name} -> {filename}")
            update_master_image(conn, image_id, zip_file_path)
            shutil.rmtree(save_name)
            logger.info(f"[{image_id}] REMOVED ORIGINAL DIRECTORY: {save_name}")
        else:
            # Dry run - just log what would happen
            for manga_image in pixiv_manga_images:
                manga_image_save_name: str = manga_image[2]
                assert os.path.exists(manga_image_save_name), f"[{image_id}] manga_image_save_name {manga_image_save_name} does not exist"
                filename = os.path.basename(manga_image_save_name)
                logger.info(f"[{image_id}] WOULD ADD TO ZIP:           {manga_image_save_name} -> {filename}")
            
            logger.info(f"[{image_id}] WOULD CREATE ZIP FILE:       {zip_file_path}")
            logger.info(f"[{image_id}] WOULD UPDATE MASTER IMAGE:   {save_name} -> {zip_file_path}")
            logger.info(f"[{image_id}] WOULD REMOVE DIRECTORY:      {save_name}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=str, default="db.sqlite")
    parser.add_argument("--not-dry-run", action="store_true", help="actually perform the migration (MAKE SURE YOU KNOW WHAT YOU ARE DOING)")
    parser.add_argument("--log-file", type=str, default=f"archive_migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log", 
                        help="Log file path")
    args = parser.parse_args()

    database_path = args.database
    conn = sqlite3.connect(database_path)
    not_dry_run = args.not_dry_run
    
    # Setup logging
    global logger
    logger = setup_logger(args.log_file)
    logger.info(f"Starting archive migration script with database: {database_path}")

    # VERIFY STAGE.
    logger.info("Running in DRY RUN mode to verify migration plan")
    perform_migration(conn, dry_run=True)

    # EXECUTE STAGE.
    if not_dry_run:
        for i in range(20, 0, -1):
            print(f"\rWARNING: THIS WILL ACTUALLY MIGRATE THE DATA. YOU HAVE {i} SECONDS TO CANCEL.", end="", flush=True)
            time.sleep(1)
        logger.info("\n\n         !!!PERFORMING MIGRATION!!!")
        perform_migration(conn, dry_run=False)
        verify_migration(conn)
    else:
        logger.info("Dry run completed. Use --not-dry-run to perform the actual migration.")

if __name__ == "__main__":
    main()
