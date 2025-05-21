"""
Script to handle filenameformat migration for a docker container (I've already rsync restored at least 20 times already while running this script)

Renames filenameformat  {%member_id%} %artist%/{%image_id%} %title%/p_0%page_number% to %member_id%/pixiv_%image_id%/p_0%page_number%.
Does not archive or change page filename.
"""

import argparse
import os
import sqlite3
from datetime import datetime

def inner_main(conn: sqlite3.Connection, root_dir: str):
    
    # get all image IDs.
    cursor = conn.cursor()
    image_ids = [row[0] for row in cursor.execute("SELECT image_id FROM pixiv_master_image").fetchall()]
    for image_id in image_ids:
        print(f"[{image_id}] start handling image.")
        member_id, old_master_save_name = cursor.execute('SELECT member_id, save_name FROM pixiv_master_image WHERE image_id = ?', (image_id,)).fetchone()
        member_save_dir = os.path.join(root_dir, str(member_id))
        os.makedirs(member_save_dir, exist_ok=True)
        image_save_dir = os.path.join(member_save_dir, f"pixiv_{image_id}")
        os.makedirs(image_save_dir, exist_ok=True)
        for page_row in cursor.execute('SELECT page, save_name FROM pixiv_manga_image WHERE image_id = ?', (image_id,)).fetchall():
            page: int = page_row[0]
            old_save_name: str = page_row[1]
            basename = os.path.basename(old_save_name)
            new_save_name = os.path.join(image_save_dir, basename)
            assert not os.path.exists(new_save_name), "Image already exists!"
            os.rename(old_save_name, new_save_name)
            cursor.execute('UPDATE pixiv_manga_image SET save_name = ? WHERE image_id = ? AND page = ?', (new_save_name, image_id, page))
        basename = os.path.basename(old_master_save_name)
        new_master_save_name = os.path.join(image_save_dir, basename)
        cursor.execute('UPDATE pixiv_master_image SET save_name = ? WHERE image_id = ?', (new_master_save_name, image_id))
        conn.commit()
        print(f"[{image_id}] Renamed {old_master_save_name} to {new_master_save_name}.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=str, default="db.sqlite")
    parser.add_argument("--root-dir", type=str, default="/workdir/downloads")
    args = parser.parse_args()

    database_path = args.database
    conn = sqlite3.connect(database_path)
    root_dir = args.root_dir
    try:
        inner_main(conn, root_dir)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
