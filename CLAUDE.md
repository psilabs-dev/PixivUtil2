# PixivUtil2 Context

PixivUtil2 is a Pixiv media and metadata downloader.

PixivUtil2 interfaces with Pixiv through cookie authentication via PHPSESSID access.
Metadata retrieval is done through AJAX endpoints, and images are retrieved from `i.pximg.net`
CDN servers.

PixivUtil2 has the following general project structure.
```
/root
|- test/                    # test data (e.g. "94891885-trans.json", "bookmarks.json")
|- Dockerfile
|- PixivUtil2.py            # command line entrypoint
|- PixivDBManager.py        # SQLite database operations and schema management
|- PixivImage.py            # image data structure
|- PixivArtist.py           # artist data structure
|- PixivHelper.py           # common functionality and file operations
|- Pixiv*Handler.py         # processing logic for content types (PixivImageHandler, PixivListHandler, ...)
|- PixivBrowserFactory.py   # mechanize browser factory, API client and HTTP request handling
|- Pixiv*.py                # other source code files
|- readme.md
|- requirements.txt
|- setup.py
|- test_*.py                # unittest test files
```

PixivUtil2 database and persistence uses SQLite and involves the following tables:
- `pixiv_master_member`: Pixiv member (artist) info including member\_id, name, save\_folder, created\_date, last\_update_date, last\_image, is\_deleted flag, and member\_token
- `pixiv_master_image`: Image metadata including image\_id, member\_id, title, save\_name, dates, is\_manga flag, and caption
- `pixiv_manga_image`: Individual manga page data with image\_id, page number, save\_name, and dates (composite primary key: image\_id, page)
- `fanbox_master_post`: FANBOX post metadata including member\_id, post\_id, title, fee\_required, dates, and post\_type
- `pixiv_ai_info`: AI-generated image classification data with image\_id, ai\_type, and dates (used for filtering AI content)
