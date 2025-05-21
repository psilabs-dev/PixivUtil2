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
import sqlite3
import zipfile

def inner_main(conn: sqlite3.Connection, root_dir: str):

    # get all image IDs.
    cursor = conn.cursor()
    image_ids = [row[0] for row in cursor.execute("SELECT image_id FROM pixiv_master_image").fetchall()]
    num_image_ids = len(image_ids)
    for i, image_id in enumerate(image_ids):
        print(f"[{image_id}][{i+1}/{num_image_ids}] start handling image.")
        member_id, old_master_save_name = cursor.execute('SELECT member_id, save_name FROM pixiv_master_image WHERE image_id = ?', (image_id,)).fetchone()
        member_save_dir = os.path.join(root_dir, str(member_id))
        os.makedirs(member_save_dir, exist_ok=True)
        image_archive_save_path = os.path.join(member_save_dir, f"pixiv_{image_id}.zip")
        assert not os.path.exists(image_archive_save_path), f"[{image_id}] archive already exists!"

        images_saved = 0
        with zipfile.ZipFile(image_archive_save_path, mode='w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as z:
            for page_row in cursor.execute('SELECT page, save_name FROM pixiv_manga_image WHERE image_id = ?', (image_id,)).fetchall():
                page: int = page_row[0]
                old_save_name: str = page_row[1]
                assert os.path.exists(old_save_name) and os.path.isfile(old_save_name)
                basename = os.path.basename(old_save_name)
                z.write(old_save_name, basename)
                os.remove(old_save_name)
                cursor.execute('UPDATE pixiv_manga_image SET save_name = ? WHERE image_id = ? AND page = ?', (basename, image_id, page))
                images_saved += 1
        cursor.execute('UPDATE pixiv_master_image SET save_name = ? WHERE image_id = ?', (image_archive_save_path, image_id))
        conn.commit()
        print(f"[{image_id}][{i+1}/{num_image_ids}] Moved {images_saved} images to {image_archive_save_path}.")

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
