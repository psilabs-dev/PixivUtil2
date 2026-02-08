# PixivUtil2 - Server Mode

This README document serves to distinguish this branch from the upstream PixivUtil2 master branch.

Server Mode is a specific operation mode of PixivUtil2 which is not meant to be directly used by a user; instead, it is meant to be used as an engine and API client under the PixivUtil Server cluster.

Do not merge this with the upstream branch.

## Master Branch Deviations

Server mode has certain differences from the upstream master branch, as it is optimized to be used by [PixivUtil Server](https://github.com/psilabs-dev/pixivutil-server) and compatibility with [LANraragi](https://github.com/psilabs-dev/LANraragi).

### Content

Unlike PixivUtil2, Server Mode prioritizes data integrity and therefore imposes certain limitations to ensure that data is stable. Unless you know exactly what you're doing and understand SQL, direct interaction with downloaded artworks, including updating and deleting of artworks and artwork metadata, is not recommended.

- All artworks will be downloaded under the `./downloads` directory under the current working directory.
- Archive mode is enabled by default, and Pixiv artworks are automatically sorted to their corresponding Pixiv member ID subdirectories. This format preserves data integrity, as the names of artists and artworks may change over time.
- All videos are saved as GIFs.

### Metadata

- Additional metadata of Pixiv artworks, including an artwork's uploaded and created dates, are fetched. This is to maintain parity with LANraragi's Pixiv metadata plugin.
- Member, tag, caption and series info are automatically added (by default, they are disabled in upstream).

Schema changes include the inclusion of a `pixiv_date_info` table.

### Authentication

- `PIXIVUTIL2_COOKIE` environment variable-based cookie loading is enabled. This allows PixivUtil2 to be run as a Dockerized worker container for convenient image deployment.

## Changelog

The following changes are downstream features and bugfixes only. Upstream features, bug fixes, and other changes shall not be discussed in this changelog.

2026-02-07

- Fixed import bug causing created and uploaded dates of artworks to not be saved, and added corresponding unit tests.
