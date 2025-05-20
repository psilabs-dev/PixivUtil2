"""
Convert all .zip extensions to .gif.
"""

import argparse
import os
import sqlite3

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=str, required=True)
    args = parser.parse_args()

    assert os.path.exists(args.db), f"Database file {args.db} does not exist."

    conn = sqlite3.connect(args.db)
    c = conn.cursor()

    rows = c.execute("SELECT image_id, save_name FROM pixiv_master_image WHERE save_name LIKE '%.zip'")

    number_of_rows = 0
    for row in rows:
        image_id: int = row[0]
        save_name: str = row[1]
        print(f"{image_id} {save_name}")
        number_of_rows += 1
        conn.execute("UPDATE pixiv_master_image SET save_name = ? WHERE image_id = ?", (save_name.replace(".zip", ".gif"), image_id))
        conn.commit()
    del rows
    rows = c.execute("SELECT image_id, save_name FROM pixiv_manga_image WHERE save_name LIKE '%.zip'")
    for row in rows:
        image_id: int = row[0]
        save_name: str = row[1]
        print(f"{image_id} {save_name}")
        number_of_rows += 1
        conn.execute("UPDATE pixiv_manga_image SET save_name = ? WHERE image_id = ?", (save_name.replace(".zip", ".gif"), image_id))
        conn.commit()
    del rows

    print(f"Number of rows: {number_of_rows}")
    conn.close()

if __name__ == "__main__":
    main()