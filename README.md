# yt_takeout_enrich

Takes a **YouTube Google Takeout** export and fetches the live details for everything in it.

| Item | What you get |
|---|---|
| **Video** (from comments, live chats, watch history, playlists, your uploads) | title, description, likes, **estimated dislikes**, views, comment count, tags, categories, chapters, duration, upload date, channel info, music track/artist/album, **thumbnail**, **subtitles** (`.srt` + plain-text transcript), **song lyrics** (YouTube Music), **top N comments**, and **each of your comments** with its replies, or the comment it replied to if yours was a reply |
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

# Only some parts, or everything except some parts (see "Parts" below)
python yt_takeout_enrich.py Takeout --include subtitles,comments
python yt_takeout_enrich.py Takeout --exclude banner,thumbnail

# Skip watch history (it can hold tens of thousands of videos)
python yt_takeout_enrich.py Takeout --exclude-files watch-history --exclude-files "My Activity"
python yt_takeout_enrich.py Takeout --exclude-files история-просмотров      # Russian export
```

The script is **resumable**, per part. Each `info.json` records every part it fetched, with its status and the options used. On the next run, only the parts that are missing, failed, or were fetched with options that don't cover the current ones are fetched; everything else is kept. Items that failed temporarily (`error`) are retried completely. If YouTube keeps rate-limiting, the script stops cleanly and you can just run it again later. Ctrl+C finishes the items in progress and stops. Videos and posts you commented on are processed first. Use `--retry-unavailable` (or `--retry-unavailable-videos`) to re-parse items that were marked as unavailable (e.g. after adding `--cookies`).

### Parts

| Part | Items | What |
|---|---|---|
| `metadata` | all | video details (yt-dlp), post content, channel details |
| `thumbnail` | videos | thumbnail image |
| `dislikes` | videos | Return YouTube Dislike estimate |
| `top-comments` | videos, posts | top N comments (`-n`, `--top-comment-replies`) |
| `my-comments` | videos, posts | your comments with replies / the comment you replied to (`--max-replies`) |
| `subtitles` | videos | subtitle files + transcripts (`--subs`, `--sub-langs`) |
| `lyrics` | videos | song lyrics (`--lyrics`) |
| `images` | posts | post images (`--resized-images`) |
| `banner`, `avatar` | channels | channel images |

Groups: `comments` (= `top-comments,my-comments`), `media` (= `thumbnail,images,banner,avatar`), `all`. Names can be singular or plural (`subtitle`, `comment`, `banners`).

`--include` selects parts (default: all) and `--exclude` removes parts from that selection. Excluded parts are not deleted from existing output, just not fetched. Item kinds with no selected part are skipped: `--include subtitles` doesn't touch channels or posts. Items still get their metadata fetched when a selected part needs it: subtitles need a fresh extraction because their URLs expire, and comments need to know the video exists.

When is a part complete?

* **status:** `ok`, `none` (nothing to fetch: no subtitles, no lyrics, no banner) or `skipped` (lyrics of a non-music video under `--lyrics auto`).
* **options:** its saved options cover the current ones. 20 saved top comments cover `-n 10` but not `-n 50`. `--subs auto` covers `manual`. `--lyrics all` covers `auto`. `--sub-langs en` doesn't cover "all languages".
* **my-comments:** every comment of yours that is in the Takeout has been looked up. A newer Takeout with new comments fetches only the new threads.

Output from older versions of the script, which has no part records, is understood: the parts it contains count as done (options inferred where possible), and the missing ones, such as `subtitles` and `lyrics`, are fetched. The log lists what is missing before the run (`Missing: subtitles x120, lyrics x120`), the parts each item got (`parts: subtitles:ok lyrics:skipped`), and a per-part summary at the end. `summary.csv` has a `missing_parts` column.

### Speed: adaptive by default

You don't need to tune the number of workers or parallel requests. Both are decided at run time from measurements, and what a run learns is saved in `OUT/.adaptive_state.json` for the next run:

* **Network probe.** At startup the script measures the latency to YouTube and the download speed. On a first run these values set the starting limits: more requests in flight on high-latency links, more parallel downloads on fast links.
* **Requests in flight**, per class: YouTube API, yt-dlp extraction, CDN media, Return YouTube Dislike. The limit grows by about one per round of requests while response times stay within 1.5× the best observed. It shrinks when latency rises, which means the server is busy or your connection is full. Downloads are measured per 256 KiB, so the limit stops growing once more parallel downloads no longer add speed.
* **Rate limits.** On HTTP 429 or "confirm you're not a bot", the limit halves and that class pauses with exponential back-off. The limit at which this happened is remembered as a ceiling, which is forgiven after 6 h and dropped after 3 days.
* **Workers** (items in parallel) are tuned by hill-climbing on the measured items/minute every 30 s. While throughput improves, the count keeps moving in the same direction; when it gets worse, it turns around; throttling cuts it by 30%. The throughput of every worker count is stored, and the next run starts with the best one.

The log shows the network measurement, the starting values and their source, every change (`adaptive: workers 2 -> 3 (24.1 items/min)`), and a speed summary at the end. `--workers N` and `--concurrency N` fix the values instead. `--reset-stats` forgets what earlier runs learned.

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
| `--include PARTS` | all | only these parts (see "Parts") |
| `--exclude PARTS` | – | leave out these parts |
| `--subs` | auto | `manual` (creator-made subtitles) or `auto` (creator-made plus auto-generated captions in the video's original language) |
| `--sub-langs` | all | subtitle languages to keep, e.g. `en,ru` (prefix match: `en` covers `en-US`, `en-GB`) |
| `--lyrics` | auto | song lyrics from YouTube Music: `auto` for music-like videos (Music category, track/artist metadata, "- Topic"/VEVO channels, "official video/audio", "lyrics", "feat." in the title) or `all` for every video |
| `--workers` | auto | items processed in parallel; `auto` adapts (see above), a number fixes it |
| `--max-workers` | 12 | upper bound for `--workers auto` |
| `--concurrency` / `--parallel-requests` | auto | YouTube API requests in flight; `auto` adapts, a number fixes it |
| `--sleep` | 0 | optional minimum gap in seconds between the starts of two YouTube requests, on top of the adaptive limits |
| `--no-network-probe`, `--reset-stats` | – | skip the startup measurement / ignore learned statistics |
| `--cookies cookies.txt` | – | Netscape cookie file: age-restricted and members-only content, fewer rate limits |
| `--retry-unavailable` | off | re-parse all items previously marked as unavailable (videos, posts, channels) |
| `--retry-unavailable-videos` | off | re-parse only videos previously marked as unavailable |
| `--resized-images` | off | keep post images as served (webp) instead of the originals |
| `--lang` / `--region` | en / US | language of comment dates and labels ("3 years ago" or, with `ru`, "3 года назад"). Video and channel metadata are always fetched in English so that counts parse reliably |
| `-v / --verbose` | off | verbose output: displays reasons, video stats, and debug logs (use `-vv` for library debug) |

## Output

```
enriched/
  inventory.json                 everything found in Takeout (also written by --dry-run)
  summary.csv                    one row per item: status, title, likes, your comments found online, ...
  .adaptive_state.json           what the adaptive concurrency learned (limits, ceilings, worker throughput, last 20 runs)
  videos/<videoId>/info.json     + thumbnail.jpg, lyrics.txt,
                                   subtitles/<lang>.srt + <lang>.txt, subtitles/<lang>.auto.srt + .txt
  posts/<postId>/info.json       + image_1.png, image_2.png, ...
  channels/<channelId>/info.json + banner.jpg, avatar.jpg
```

`videos/<id>/info.json` (shortened):

```jsonc
{
  "kind": "video", "id": "dQw4w9WgXcQ", "status": "ok",          // ok | partial | unavailable | error
  "parts": { "subtitles": { "status": "ok", "at": "2026-11-24T06:19:26+00:00", "params": { "mode": "auto", "langs": [] } },
             "lyrics": { "status": "skipped", ... }, ... },     // ok | none | skipped | error
  "metadata": { "title": "...", "description": "...", "like_count": 19501188,
                "dislike_count_estimate": 521783, "view_count": 1836270607, "tags": [...], ... },
  "thumbnail_file": "thumbnail.jpg",
  "subtitles": { "available_manual": ["de-DE", "en", "es-419", "ja", "pt-BR"], "auto_original_language": "en",
                 "tracks": [ { "lang": "en", "name": "English", "auto_generated": false,
                               "file": "subtitles/en.srt", "text_file": "subtitles/en.txt", "chars": 2067 } ] },
  "lyrics": { "status": "ok", "source": "Musixmatch", "file": "lyrics.txt", "text": "We're no strangers to love\n..." },
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

Item `status`:

* `ok` – everything was fetched.
* `partial` – saved, but a selected part failed (see `parts.<name>.error`); only the failed parts are retried on the next run.
* `unavailable` – a permanent reason (removed, private, terminated, age-restricted without cookies, ...); not retried.
* `error` – a temporary failure (bot check, rate limit, network, yt-dlp hiccup); retried on the next run.

`user_threads[].status` can be `ok`, `not_found_online` (the comment was deleted, is held for review, or is hidden), `reply_not_found_online`, or `error`.

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
* **Subtitles** are the timed-text tracks yt-dlp lists, downloaded as SRT. A plain transcript (`.txt`) is derived from each, with cue numbers, timings and the repeated lines of auto-generated captions removed.
* **Lyrics** come from YouTube Music's "Lyrics" tab: the InnerTube `next` call with the `WEB_REMIX` client gives the `MPLYt…` browse ID, and `browse` returns the text and its source (usually Musixmatch).
* **Dislikes** come from the [Return YouTube Dislike](https://returnyoutubedislike.com) API. These are **estimates**, because YouTube no longer shows real dislike counts.
* The Takeout formats were taken from [google_takeout_parser](https://github.com/purarue/google_takeout_parser) and Google's [Data Portability schema](https://developers.google.com/data-portability/schema-reference/youtube).

## Limitations

* YouTube only provides relative comment dates ("3 years ago") and rounded like counts ("71K") for comments. The exact date of *your* comments comes from Takeout (`created`).
* YouTube threads are only one level deep. Nobody can reply to a reply directly; people reply in the same thread and `@mention` you. For your replies, `replies` therefore lists the later replies in that thread that mention your handle, within `--max-replies`.
* Members-only, private, age-restricted or region-locked content needs `--cookies` from an account that can see it.
* Lyrics are plain text; YouTube Music's time-synced lyrics are not fetched. In `--lyrics auto` mode, music videos without any of the hints listed above are skipped; use `--lyrics all` for a music-heavy export.
* InnerTube is undocumented and changes from time to time. If comments stop appearing, check `info.json → comments.error` and run with `-vv`.
