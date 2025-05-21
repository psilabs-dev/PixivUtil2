import sqlite3

def main():
    conn = sqlite3.connect("db.sqlite")
    cursor = conn.cursor()
    for master_row in cursor.execute("SELECT image_id FROM pixiv_master_image").fetchall():
        image_id = master_row[0]
        assert len(cursor.execute("SELECT * FROM pixiv_manga_image WHERE image_id = ?", (image_id,)).fetchall()) > 0

    for manga_row in cursor.execute("SELECT DISTINCT image_id FROM pixiv_manga_image").fetchall():
        image_id = manga_row[0]
        assert len(cursor.execute("SELECT * FROM pixiv_master_image WHERE image_id = ?", (image_id,)).fetchall()) > 0

main()
