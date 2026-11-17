# yt_takeout_enrich

Takes a **YouTube Google Takeout** export and fetches the live details for everything in it.

| Item | What you get |
|---|---|
| **Video** (from comments, live chats, watch history, playlists, your uploads) | title, description, likes, **estimated dislikes**, views, comment count, tags, categories, chapters, duration, upload date, channel info, **thumbnail**, **top N comments**, and **each of your comments** with its replies, or the comment it replied to if yours was a reply |
| **Community post** (from comments on posts, your own posts, activity) | text (truncated links expanded to the real URL), **images at original resolution**, poll/quiz/video/playlist/shared-post attachments, likes, **top N comments**, and your comments (same rules as videos) |
| **Channel** (from subscriptions) | **description**, **banner**, avatar, subscriber count, handle, verification, keywords |

## Install

```bash
pip install -r requirements.txt
```

Python 3.9+. `beautifulsoup4` is only needed for old HTML-style exports.

## Usage

```bash
# Run it on the archive(s) directly; multi-part exports are fine.
python yt_takeout_enrich.py takeout-20261101T000000Z-001.zip takeout-20261101T000000Z-002.zip -o enriched

# Or on an extracted folder. Any of these works:
python yt_takeout_enrich.py ~/Downloads/Takeout -o enriched                            # the Takeout folder
python yt_takeout_enrich.py "~/Downloads/Takeout/YouTube и YouTube Music" -o enriched  # just the YouTube folder
python yt_takeout_enrich.py ~/Downloads/takeout-parts -o enriched                      # a folder holding takeout-*.zip parts

# See what the script finds in the Takeout without hitting YouTube
python yt_takeout_enrich.py Takeout --dry-run        # writes enriched/inventory.json

# Only things you commented on, 50 top comments each, with 5 replies per top comment
python yt_takeout_enrich.py Takeout --only-commented -n 50 --top-comment-replies 5

# Skip watch history (it can hold tens of thousands of videos)
python yt_takeout_enrich.py Takeout --exclude-files watch-history --exclude-files "My Activity"
python yt_takeout_enrich.py Takeout --exclude-files история-просмотров      # Russian export
```

The script is **resumable**. Finished items are skipped on the next run, and items that failed with an error are retried. If YouTube starts rate-limiting, the script stops cleanly and you can just run it again later. Videos and posts you commented on are processed first.

### Main options

| Option | Default | Meaning |
|---|---|---|
| `-n / --top-comments` | 20 | top comments per video/post ("Top" sort, same as the website) |
| `--top-comment-replies` | 0 | replies loaded for each top comment |
| `--max-replies` | 100 | replies loaded for each of *your* comments (`-1` = all) |
| `--kinds` | video,post,channel | which kinds to enrich |
| `--only-commented` | off | only videos/posts that have your comments |
| `--exclude-files SUBSTR` | – | skip Takeout files whose path contains SUBSTR |
| `--limit N` | 0 | at most N items per kind (for testing) |
| `--workers` | 2 | parallel workers (items processed in parallel) |
| `--concurrency` / `--parallel-requests` | max(4, workers * 2) | maximum concurrent in-flight HTTP requests across all workers |
| `--sleep` | 1.0 s | minimum delay between requests per slot (0 = no delay) |
| `--cookies cookies.txt` | – | Netscape cookie file: age-restricted and members-only content, fewer rate limits |
| `--no-thumbnails`, `--resized-images`, `--no-dislikes` | – | skip media, keep post images as webp, skip the RYD lookup |
| `--lang` / `--region` | en / US | language of comment dates and labels ("3 years ago" or, with `ru`, "3 года назад"). Video and channel metadata are always fetched in English so that counts parse reliably |

## Output

```
enriched/
  inventory.json                 everything found in Takeout (also written by --dry-run)
  summary.csv                    one row per item: status, title, likes, your comments found online, ...
  videos/<videoId>/info.json     + thumbnail.jpg
  posts/<postId>/info.json       + image_1.png, image_2.png, ...
  channels/<channelId>/info.json + banner.jpg, avatar.jpg
```

`videos/<id>/info.json` (shortened):

```jsonc
{
  "kind": "video", "id": "dQw4w9WgXcQ", "status": "ok",          // ok | unavailable | error
  "metadata": { "title": "...", "description": "...", "like_count": 19501188,
                "dislike_count_estimate": 521783, "view_count": 1836270607, "tags": [...], ... },
  "thumbnail_file": "thumbnail.jpg",
  "comments": {
    "total_count_text": "2,462,021 Comments",
    "top": [ { "id": "...", "text": "...", "author": {...}, "likes": 71000, "reply_count": 688,
               "published": "6 years ago", "hearted_by_creator": false, "replies": [...] } ],
    "user_threads": [
      { "comment_id": "Ugy2X...", "is_reply": false, "status": "ok",
        "takeout_text": "...", "created": "2020-04-14T07:39:35+00:00",
        "comment": { ... live version of your comment ... },
        "replies": [ ... replies to your comment ... ] },
      { "comment_id": "UgyKf....931TM...", "is_reply": true, "status": "ok",
        "replying_to": { ... the comment you replied to ... },
        "comment": { ... your reply ... },
        "replies": [ ... later replies in that thread that @mention you ... ] }
    ]
  },
  "takeout": { "sources": ["comments", "watch_history"], "title": "...", "activity_count": 3,
               "user_comments": [...], "live_chat_messages": [...] }
}
```

`user_threads[].status` can be `ok`, `not_found_online` (the comment was deleted, is held for review, or is hidden), or `reply_not_found_online`.

## Supported Takeout formats

| File | Formats |
|---|---|
| `comments/comments.csv` | current format (Channel ID, Comment Create Timestamp, Price, Comment ID, Parent Comment ID, **Post ID**, Video ID, Comment Text with `{"text":..},{..}` segments), the pre-2024 column order with `{"takeoutSegments":[...]}`, and localized headers (columns are recognised by their contents) |
| `my-comments/*.html`, `my-live-chat-messages/*.html` | legacy HTML (the `lc=` link gives the comment ID) |
| `live chats/live chats.csv` | your messages are stored with the video |
| `history/watch-history.json` / `.html`, `My Activity/YouTube/*` | videos and posts; **"From Google Ads" entries are skipped**; the Takeout title is kept (useful for deleted videos) |
| `subscriptions/subscriptions.csv` | channels |
| `playlists/*-videos.csv`, `playlists/likes.json`, video metadata CSVs | videos |
| own posts CSV (`Post ID`, `Post Text`, ...) | posts |

Inputs can be `.zip` or `.tgz` archives, which are read in place without extracting them, or folders. A folder can be the whole `Takeout`, the `YouTube and YouTube Music` folder itself, or a folder holding `takeout-*.zip` parts.

### Localized exports

**Russian** exports are supported explicitly: the folder names (`Комментарии`, `история/история-просмотров.json`, `плейлисты/Видео в плейлисте _Name_.csv`, `подписки/подписки.csv`, `Метаданные видео`, `каналы`, `live chats`), the CSV headers (`Идентификатор комментария`, `Идентификатор видео`, `Текст комментария`, `URL канала`, ...), the watch-history titles ("Вы посмотрели ...") and the ad markers ("Из Google Рекламы").

| Russian Takeout | Used for |
|---|---|
| `Комментарии/Комментарии.csv` | your comments (videos and posts) |
| `live chats/live chats.csv` | your live chat messages |
| `история/история-просмотров.json` | watched videos (`история_поиска.json` is ignored) |
| `плейлисты/Видео в плейлисте _….csv` | playlist videos (`плейлисты.csv` only holds playlist settings) |
| `Метаданные видео/*.csv` | your uploaded videos |
| `подписки/подписки.csv` | subscribed channels |
| `каналы/*.csv`, `видео/*.mp4` | your own channel settings and video files, not enriched |

For other languages the script recognizes CSV columns by their *contents* (comment IDs, video IDs, `UC…` channel IDs, JSON comment text, timestamps). It does not need the header names, so most exports work as well (tested with German). To check how each file was recognized, run with `--dry-run` and look at `"files"` in `inventory.json`.

## How it works

* **Video and channel metadata** come from [yt-dlp](https://github.com/yt-dlp/yt-dlp). For a channel it uses the flat channel extraction, which provides `banner_uncropped` and `avatar_uncropped`.
* **Comments and posts** come straight from YouTube's internal `youtubei/v1` (InnerTube) API with no HTML scraping, because YouTube serves a captcha to many server/VPN IPs. The comment parsing and pagination follow [youtube-comment-downloader](https://github.com/egbertbouman/youtube-comment-downloader). Continuation tokens are built as protobuf messages. In that protobuf, field **16** is the "highlighted comment" (the `?lc=` URL parameter). It puts your comment first. If your comment is a reply, it returns the parent thread with your reply as a teaser, and the script uses this to find "the comment it was replying to". Post comments use the `FEcomment_post_detail_page_web_top_level` browse continuation.
* **Dislikes** come from the [Return YouTube Dislike](https://returnyoutubedislike.com) API. These are **estimates**, because YouTube no longer shows real dislike counts.
* The Takeout formats were taken from [google_takeout_parser](https://github.com/purarue/google_takeout_parser) and Google's [Data Portability schema](https://developers.google.com/data-portability/schema-reference/youtube).

## Limitations

* YouTube only provides relative comment dates ("3 years ago") and rounded like counts ("71K") for comments. The exact date of *your* comments comes from Takeout (`created`).
* YouTube threads are only one level deep. Nobody can reply to a reply directly; people reply in the same thread and `@mention` you. For your replies, `replies` therefore lists the later replies in that thread that mention your handle, within `--max-replies`.
* Members-only, private, age-restricted or region-locked content needs `--cookies` from an account that can see it.
* InnerTube is undocumented and changes from time to time. If comments stop appearing, check `info.json → comments.error` and run with `-vv`.
