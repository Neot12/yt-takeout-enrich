#!/usr/bin/env python3
"""
yt_takeout_enrich.py - enrich a YouTube Google Takeout export with live data.

Reads one or more Takeout archives (.zip/.tgz) or extracted folders, collects
every video, community post and subscribed channel referenced in them, and
fetches for each:

  video   : metadata (title, description, likes, estimated dislikes, views, tags,
            chapters, ...), thumbnail, subtitles (.srt + plain transcript), song
            lyrics (YouTube Music), top-N comments, and every comment *you*
            wrote on it: your comment, the replies to it, and the comment you were
            replying to (if yours was a reply).
  post    : post text (with real link targets), images (original resolution),
            poll / video / shared-post attachments, likes, top-N comments, and
            your comments with the same rules as for videos.
  channel : description, banner, avatar, subscriber count, handle, links.

Data sources:
  * yt-dlp                 - video and channel metadata (maintained upstream).
  * YouTube InnerTube API  - comments and community posts. Pages are not
                             scraped as HTML (YouTube serves a captcha to many
                             server IPs); requests go straight to youtubei/v1.
                             Comment pagination follows the approach used by
                             github.com/egbertbouman/youtube-comment-downloader.
  * Return YouTube Dislike - dislike *estimates* (YouTube hides real counts).

Concurrency is adaptive: per-class request limits follow latency and throttling,
the number of parallel items follows measured throughput, and both start from
what previous runs learned (OUT/.adaptive_state.json).

Takeout parsing follows the formats documented by
github.com/purarue/google_takeout_parser and Google's Data Portability schema
(comments.csv with "Post ID"/"Parent Comment ID" columns, the old pre-2024
'takeoutSegments' comment text, legacy my-comments.html, watch-history
json/html, subscriptions.csv, playlist CSVs, own posts CSV).

What is fetched can be chosen per part (--include / --exclude); every part's
status and options are recorded in info.json, so a re-run fetches exactly the
parts that are missing, failed, or were fetched with insufficient options.

Output layout:

  OUT/inventory.json              everything extracted from Takeout
  OUT/summary.csv                 one row per enriched item
  OUT/.adaptive_state.json        learned concurrency statistics
  OUT/videos/<id>/info.json       + thumbnail.<ext>, lyrics.txt, subtitles/
  OUT/posts/<id>/info.json        + image_<n>.<ext>
  OUT/channels/<id>/info.json     + banner.<ext>, avatar.<ext>
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures as cf
import contextlib
import csv
import hashlib
import importlib.util
import http.cookiejar
import io
import json
import logging
import re
import sys
import tarfile
import threading
import time
import urllib.parse
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator

import requests
import requests.adapters

__version__ = "1.2.0"
log = logging.getLogger("yt_takeout_enrich")

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

# --------------------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------------------

VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
CHANNEL_ID_RE = re.compile(r"^UC[A-Za-z0-9_-]{22}$")
COMMENT_ID_RE = re.compile(r"^Ug[A-Za-z0-9_-]{10,}(\.[A-Za-z0-9_-]+)?$")
POST_ID_RE = re.compile(r"^Ug[A-Za-z0-9_-]{10,}$")
LONG_ID_RE = re.compile(r"^[A-Za-z0-9_-]{18,}(\.[A-Za-z0-9_-]+)?$")
ISO_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}")

URL_VIDEO_RE = re.compile(
    r"(?:youtube\.com/(?:watch\?(?:[^\"'\s<>]*?&(?:amp;)?)?v=|shorts/|live/|embed/|v/)|youtu\.be/)"
    r"([A-Za-z0-9_-]{11})(?![A-Za-z0-9_-])"
)
URL_POST_RE = re.compile(
    r"youtube\.com/(?:post/|channel/[A-Za-z0-9_-]+/community\?(?:[^\"'\s<>]*?&(?:amp;)?)?lb=)"
    r"([A-Za-z0-9_-]{16,})"
)
URL_CHANNEL_RE = re.compile(r"youtube\.com/channel/(UC[A-Za-z0-9_-]{22})")
URL_LC_RE = re.compile(r"[?&](?:amp;)?lc=([A-Za-z0-9_.-]+)")

WEB_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# Localized Takeout CSV headers -> canonical English header (both normalized with
# norm_header: lower case, letters/digits only). Unknown languages still work
# through value-based column detection (TakeoutParser._profile).
HEADER_ALIASES: dict[str, list[str]] = {
    # Russian
    "commentid": ["идентификаторкомментария"],
    "channelid": ["идентификаторканала"],
    "commentcreatetimestamp": ["датапубликациикомментария", "временнаяметкасозданиякомментария", "датасозданиякомментария"],
    "price": ["цена"],
    "parentcommentid": ["идентификаторродительскогокомментария", "идентификаторосновногокомментария"],
    "postid": ["идентификаторзаписи", "идентификаторпубликации", "идентификаторпоста"],
    "videoid": ["идентификаторвидео"],
    "commenttext": ["тексткомментария"],
    "livechatid": ["идентификаторсообщениявчате"],
    "livechatcreatetimestamp": ["временнаяметкасозданиясообщениявчате"],
    "livechattext": ["текстсообщениявчате"],
    "channelurl": ["urlканала"],
    "channeltitle": ["названиеканала"],
    "posttext": ["текстзаписи", "текстпубликации", "текстпоста"],
    "playlistid": ["идентификаторплейлиста"],
    "playlistvideocreationtimestamp": ["датасозданиявидеовплейлисте"],
    "videocreatetimestamp": ["времясозданиявидео"],
    "videopublishtimestamp": ["времяпубликациивидео"],
    "videotitleoriginal": ["оригинальноеназваниевидео"],
}
_ALIAS_TO_CANON = {alias: canon for canon, aliases in HEADER_ALIASES.items() for alias in aliases}


def norm_header(h: str) -> str:
    """'Comment ID' -> 'commentid', 'Идентификатор видео' -> 'videoid'."""
    n = re.sub(r"[\W_]", "", (h or "").lower())
    return _ALIAS_TO_CANON.get(n, n)


def walk(obj: Any, key: str) -> Iterator[Any]:
    """Yield every value stored under `key` anywhere in obj, in document order."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == key:
                yield v
            else:
                yield from walk(v, key)
    elif isinstance(obj, list):
        for x in obj:
            yield from walk(x, key)


def walk_any(obj: Any, keys: set[str]) -> Iterator[tuple[str, Any]]:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in keys:
                yield k, v
            else:
                yield from walk_any(v, keys)
    elif isinstance(obj, list):
        for x in obj:
            yield from walk_any(x, keys)


def first(it: Iterable[Any], default: Any = None) -> Any:
    return next(iter(it), default)


def text_of(node: Any) -> str:
    """Flatten YouTube 'runs'/'simpleText'/'content' text objects."""
    if node is None:
        return ""
    if isinstance(node, str):
        return node
    if isinstance(node, dict):
        if "simpleText" in node:
            return node["simpleText"]
        if "runs" in node:
            return "".join(r.get("text", "") for r in node["runs"])
        if "content" in node and isinstance(node["content"], str):
            return node["content"]
    return ""


_SUFFIX = {"k": 1e3, "m": 1e6, "b": 1e9, "тыс": 1e3, "млн": 1e6, "млрд": 1e9}


def parse_count(s: Any) -> int | None:
    """'1,234' -> 1234, '1.2K likes' -> 1200, '3M' -> 3000000, '1,2 тыс.' -> 1200, '' -> None."""
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return int(s)
    s = str(s).strip().replace(" ", " ")
    m = re.search(r"(\d[\d,.\s]*)\s*(млрд|млн|тыс|[kKmMbB])?(?![a-zA-Z])", s)
    if not m:
        return None
    num, suf = m.group(1).replace(" ", ""), (m.group(2) or "").lower()
    if suf:
        try:
            return int(float(num.replace(",", ".") if num.count(",") == 1 and "." not in num else num.replace(",", "")) * _SUFFIX[suf])
        except ValueError:
            return None
    try:
        return int(re.sub(r"[,.]", "", num))
    except ValueError:
        return None


def best_thumb(thumbs: list[dict] | None) -> str | None:
    if not thumbs:
        return None
    t = max(thumbs, key=lambda t: (t.get("width") or 0) * (t.get("height") or 0))
    url = t.get("url")
    if url and url.startswith("//"):
        url = "https:" + url
    return url


def unwrap_redirect(url: str | None) -> str | None:
    """https://www.youtube.com/redirect?...&q=REAL -> REAL"""
    if not url:
        return url
    if url.startswith("/"):
        url = "https://www.youtube.com" + url
    p = urllib.parse.urlparse(url)
    if p.netloc.endswith("youtube.com") and p.path == "/redirect":
        q = urllib.parse.parse_qs(p.query).get("q")
        if q:
            return q[0]
    return url


def runs_to_text(node: Any) -> tuple[str, str, list[str]]:
    """Return (display_text, expanded_text, links). Expanded text replaces the
    truncated link captions ('https://site/...') with the real targets."""
    if not isinstance(node, dict) or "runs" not in node:
        t = text_of(node)
        return t, t, []
    disp, exp, links = [], [], []
    for r in node["runs"]:
        t = r.get("text", "")
        disp.append(t)
        ne = r.get("navigationEndpoint") or {}
        url = None
        if "urlEndpoint" in ne:
            url = unwrap_redirect(ne["urlEndpoint"].get("url"))
        elif "watchEndpoint" in ne:
            vid = ne["watchEndpoint"].get("videoId")
            url = f"https://www.youtube.com/watch?v={vid}" if vid else None
        elif "browseEndpoint" in ne:
            cbu = ne["browseEndpoint"].get("canonicalBaseUrl")
            url = "https://www.youtube.com" + cbu if cbu else None
        if url and (t.startswith("http") or t.endswith("...")):
            exp.append(url)
        else:
            exp.append(t)
        if url:
            links.append(url)
    return "".join(disp), "".join(exp), links


# --------------------------------------------------------------------------------------
# Protobuf encoding for InnerTube continuation tokens
# --------------------------------------------------------------------------------------


def _varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def pb(field_no: int, value: int | str | bytes) -> bytes:
    if isinstance(value, int):
        return _varint(field_no << 3) + _varint(value)
    if isinstance(value, str):
        value = value.encode()
    return _varint(field_no << 3 | 2) + _varint(len(value)) + value


def pbs(*parts: bytes) -> bytes:
    return b"".join(parts)


def b64tok(b: bytes) -> str:
    return urllib.parse.quote(base64.urlsafe_b64encode(b).decode())


SORT_TOP, SORT_NEW = 0, 1


def video_comments_token(video_id: str, sort: int = SORT_TOP, highlight: str | None = None) -> str:
    """Token for the 'next' endpoint. `highlight` = comment id shown first
    (same as the ?lc= URL parameter). Works for replies too: the parent thread
    is returned first with the reply in its teaser."""
    inner = pbs(pb(4, video_id), pb(6, sort), pb(16, highlight) if highlight else b"")
    return b64tok(
        pbs(
            pb(2, pbs(pb(2, video_id))),
            pb(3, 6),
            pb(6, pbs(pb(4, inner), pb(8, "engagement-panel-comments-section"))),
        )
    )


def post_comments_token(post_id: str, channel_id: str, sort: int = SORT_TOP, highlight: str | None = None) -> str:
    """Token for the 'browse' endpoint (community post comments)."""
    inner = pbs(pb(6, sort), pb(27, 1), pb(29, post_id), pb(30, channel_id), pb(16, highlight) if highlight else b"")
    params = pbs(
        pb(2, "posts"),
        pb(53, pbs(pb(4, inner), pb(8, "comments-section"))),
        pb(56, pbs(pb(2, channel_id), pb(3, post_id), pb(11, channel_id))),
    )
    return b64tok(pb(80226972, pbs(pb(2, "FEcomment_post_detail_page_web_top_level"), pb(3, b64tok(params)))))


# --------------------------------------------------------------------------------------
# Takeout parsing
# --------------------------------------------------------------------------------------


@dataclass
class UserComment:
    comment_id: str  # full id; replies are "<parent>.<reply>"
    parent_id: str | None
    video_id: str | None
    post_id: str | None
    text: str
    links: list[str]
    created: str | None
    channel_id: str | None  # the account that wrote it (you)
    price: str | None
    source: str

    @property
    def is_reply(self) -> bool:
        return bool(self.parent_id)


@dataclass
class Item:
    kind: str  # video | post | channel
    id: str
    sources: set[str] = field(default_factory=set)
    title: str | None = None  # title as recorded in Takeout (survives deletions)
    channel_name: str | None = None
    channel_url: str | None = None
    activity_times: list[str] = field(default_factory=list)
    user_comments: list[UserComment] = field(default_factory=list)
    live_chat_messages: list[dict] = field(default_factory=list)
    takeout_extra: list[dict] = field(default_factory=list)

    def to_json(self) -> dict:
        times = sorted(t for t in self.activity_times if t)
        d: dict[str, Any] = {"sources": sorted(self.sources)}
        if self.title:
            d["title"] = self.title
        if self.channel_name or self.channel_url:
            d["channel"] = {"name": self.channel_name, "url": self.channel_url}
        if self.activity_times:
            d["activity_count"] = len(self.activity_times)
            if times:
                d["first_activity"], d["last_activity"] = times[0], times[-1]
        if self.user_comments:
            d["user_comments"] = [asdict(uc) for uc in self.user_comments]
        if self.live_chat_messages:
            d["live_chat_messages"] = self.live_chat_messages
        if self.takeout_extra:
            d["extra"] = self.takeout_extra
        return d


class Inventory:
    def __init__(self) -> None:
        self.items: dict[str, dict[str, Item]] = {"video": {}, "post": {}, "channel": {}}
        self.files: list[dict] = []

    def get(self, kind: str, id_: str, source: str) -> Item:
        it = self.items[kind].get(id_)
        if it is None:
            it = self.items[kind][id_] = Item(kind, id_)
        it.sources.add(source)
        return it

    def add_comment(self, uc: UserComment) -> None:
        if uc.video_id:
            item = self.get("video", uc.video_id, uc.source)
        elif uc.post_id:
            item = self.get("post", uc.post_id, uc.source)
        else:
            return
        if all(c.comment_id != uc.comment_id or not uc.comment_id for c in item.user_comments):
            item.user_comments.append(uc)


def parse_takeout_text(raw: str) -> tuple[str, list[str]]:
    """Decode Takeout comment text.
    pre-2024 : {"takeoutSegments":[{"text":"..."}, {"link":{"linkUrl":"..."}}]}
    2024+    : {"text":"a"},{"text":"\n"},{"text":"b"}   (comma-joined objects)"""
    raw = (raw or "").strip()
    if not raw:
        return "", []
    data: Any
    try:
        data = json.loads(raw, strict=False)
    except ValueError:
        try:
            data = json.loads("[" + raw + "]", strict=False)
        except ValueError:
            return raw, []
    if isinstance(data, dict) and "takeoutSegments" in data:
        segs = data["takeoutSegments"]
    elif isinstance(data, dict):
        segs = [data]
    elif isinstance(data, list):
        segs = data
    else:
        return str(data), []
    text, links = [], []
    for s in segs:
        if not isinstance(s, dict):
            continue
        link = (s.get("link") or {}).get("linkUrl")
        if link:
            links.append(link)
        text.append(s.get("text") or link or "")
    return "".join(text), links


class TakeoutFile:
    def __init__(self, rel: str, opener: Callable[[], io.BufferedIOBase]):
        self.rel = rel
        self._opener = opener

    def read_bytes(self) -> bytes:
        with self._opener() as f:
            return f.read()

    def read_text(self) -> str:
        return self.read_bytes().decode("utf-8-sig", errors="replace")


def rel_takeout_path(name: str) -> str:
    parts = Path(name.replace("\\", "/")).parts
    for i, p in enumerate(parts):
        if p.lower() == "takeout":
            return "/".join(parts[i + 1 :])
    return "/".join(parts)


ARCHIVE_RE = re.compile(r"\.(zip|tgz|tar\.gz|tar)$", re.I)


def iter_takeout_files(paths: list[Path], stack: contextlib.ExitStack) -> Iterator[TakeoutFile]:
    """Yield files of Takeout archives and/or extracted folders. A folder may be
    the whole Takeout, the 'YouTube and YouTube Music' folder itself, any folder
    above them, or a folder holding takeout-*.zip / *.tgz parts. Archives stay
    open until `stack` is closed."""
    seen: set[str] = set()
    for p in paths:
        p = p.expanduser()
        if p.is_dir():
            for f in sorted(p.rglob("*")):
                if not f.is_file():
                    continue
                if ARCHIVE_RE.search(f.name) and "takeout" in f.name.lower():
                    yield from iter_takeout_files([f], stack)
                    continue
                rel = rel_takeout_path(str(f.relative_to(p)))
                if rel not in seen:
                    seen.add(rel)
                    yield TakeoutFile(rel, lambda f=f: open(f, "rb"))
        elif zipfile.is_zipfile(p):
            zf = stack.enter_context(zipfile.ZipFile(p))
            for info in zf.infolist():
                if info.is_dir():
                    continue
                rel = rel_takeout_path(info.filename)
                if rel not in seen:
                    seen.add(rel)
                    yield TakeoutFile(rel, lambda info=info, zf=zf: zf.open(info))
        elif tarfile.is_tarfile(p):
            tf = stack.enter_context(tarfile.open(p))
            for m in tf.getmembers():
                if not m.isfile():
                    continue
                rel = rel_takeout_path(m.name)
                if rel not in seen:
                    seen.add(rel)
                    yield TakeoutFile(rel, lambda m=m, tf=tf: tf.extractfile(m))
        elif p.is_file():
            rel = rel_takeout_path(p.name)
            yield TakeoutFile(rel, lambda p=p: open(p, "rb"))
        else:
            raise FileNotFoundError(p)


_MY_ACTIVITY_DIRS = ("my activity", "мои действия")


def is_youtube_file(rel: str) -> bool:
    low = rel.lower()
    return (
        low.startswith("youtube")
        or "/youtube" in low
        or any(low.startswith(d + "/youtube") for d in _MY_ACTIVITY_DIRS)
    )


def _playlist_name(stem: str) -> str:
    m = re.match(r"^Видео в плейлисте\s*_?(.*?)_?$", stem)  # Russian: 'Видео в плейлисте _Name_'
    if m:
        return m.group(1)
    return re.sub(r"-videos$", "", stem)


def source_label(rel: str) -> str:
    """Short, stable label for where an item was referenced."""
    low = rel.lower()
    segs = low.split("/")
    if any(k in low for k in ("watch-history", "watch history", "история-просмотров", "история просмотров", "история_просмотров")):
        return "watch_history"
    if any(k in low for k in ("search-history", "search history", "история_поиска", "история-поиска", "история поиска")):
        return "search_history"
    if any(low.startswith(d) for d in _MY_ACTIVITY_DIRS):
        return "my_activity"
    if any(k in low for k in ("live chat", "live-chat", "livechat", "чат")):
        return "live_chats"
    if any(s.startswith(("comment", "my-comment", "комментари")) for s in segs):
        return "comments"
    if "subscri" in low or "подписк" in low:
        return "subscriptions"
    if "playlist" in low or "плейлист" in low:
        return "playlist:" + _playlist_name(Path(rel).stem)
    if any(s in ("posts", "записи", "публикации", "посты") for s in segs[:-1]) or low.endswith("posts.csv"):
        return "own_posts"
    if any(k in low for k in ("video metadata", "метаданные видео")) or any(s in ("videos", "видео") for s in segs[:-1]):
        return "own_videos"
    return "file:" + rel


class TakeoutParser:
    def __init__(self, inv: Inventory, exclude: list[str]):
        self._done_rel: set[str] = set()
        self.inv = inv
        self.exclude = [e.lower() for e in exclude]

    def run(self, paths: list[Path]) -> None:
        for p in paths:
            with contextlib.ExitStack() as stack:
                files = list(iter_takeout_files([p], stack))
                yt_files = [f for f in files if is_youtube_file(f.rel)]
                if not yt_files and files:
                    # e.g. the 'YouTube и YouTube Music' folder itself was given
                    log.info("No 'YouTube...' folder inside %s - treating all of its files as YouTube data", p)
                    yt_files = files
                self._run_files(yt_files)

    def _run_files(self, files: list[TakeoutFile]) -> None:
        for tf in files:
            if tf.rel in self._done_rel:
                continue  # same file in several inputs (e.g. overlapping folders)
            self._done_rel.add(tf.rel)
            if any(e in tf.rel.lower() for e in self.exclude):
                self.inv.files.append({"file": tf.rel, "handled_as": "excluded"})
                continue
            ext = Path(tf.rel).suffix.lower()
            try:
                if ext == ".csv":
                    kind, n = self.parse_csv(tf)
                elif ext == ".json":
                    kind, n = self.parse_json(tf)
                elif ext in (".html", ".htm"):
                    kind, n = self.parse_html(tf)
                else:
                    kind, n = "skipped", 0
            except Exception as e:  # never die on one odd file
                log.warning("Could not parse %s: %s", tf.rel, e)
                kind, n = f"error: {e}", 0
            self.inv.files.append({"file": tf.rel, "handled_as": kind, "records": n})
            log.info("  %-60s %-14s %d", tf.rel[-60:], kind, n)

    # ---------------- CSV ----------------

    @staticmethod
    def _rows(tf: TakeoutFile) -> tuple[list[str], list[list[str]]]:
        reader = csv.reader(io.StringIO(tf.read_text(), newline=""))
        rows = [r for r in reader if any(c.strip() for c in r)]
        if not rows:
            return [], []
        return rows[0], rows[1:]

    @staticmethod
    def _profile(header: list[str], rows: list[list[str]]) -> dict[str, list[int]]:
        """Guess column roles from values (used when headers are localized)."""
        sample = rows[:200]
        roles: dict[str, list[int]] = {}
        for i in range(len(header)):
            vals = [r[i].strip() for r in sample if i < len(r) and r[i].strip()]
            if not vals:
                continue

            def frac(pred: Callable[[str], bool]) -> float:
                return sum(1 for v in vals if pred(v)) / len(vals)

            if frac(lambda v: v.startswith("{")) > 0.9:
                roles.setdefault("json_text", []).append(i)
            elif frac(lambda v: bool(ISO_TS_RE.match(v))) > 0.9:
                roles.setdefault("timestamp", []).append(i)
            elif frac(lambda v: bool(CHANNEL_ID_RE.match(v))) > 0.9:
                roles.setdefault("channel_id", []).append(i)
            elif frac(lambda v: "youtube.com/channel/" in v) > 0.9:
                roles.setdefault("channel_url", []).append(i)
            elif frac(lambda v: bool(VIDEO_ID_RE.match(v))) > 0.9:
                roles.setdefault("video_id", []).append(i)
            elif frac(lambda v: bool(LONG_ID_RE.match(v))) > 0.9:
                roles.setdefault("ug_id", []).append(i)  # comment / parent / post ids
        return roles

    def parse_csv(self, tf: TakeoutFile) -> tuple[str, int]:
        header, rows = self._rows(tf)
        if not header:
            return "empty", 0
        nh = [norm_header(h) for h in header]

        def col(*names: str, contains: tuple[str, ...] = ()) -> int | None:
            for n in names:
                if n in nh:
                    return nh.index(n)
            if contains:
                for i, h in enumerate(nh):
                    if all(c in h for c in contains):
                        return i
            return None

        label = source_label(tf.rel)
        roles = self._profile(header, rows)
        # --- live chats ---
        if col("livechatid") is not None or label == "live_chats":
            return self._csv_live_chats(header, rows, nh, label)
        # --- comments.csv (English header, or localized: JSON text + id columns) ---
        if col("commentid") is not None or label == "comments" or (
            roles.get("json_text") and roles.get("ug_id") and (roles.get("video_id") or len(roles["ug_id"]) > 1)
        ):
            return self._csv_comments(tf, header, rows, nh, "comments")
        # --- subscriptions ---
        ch_url = col("channelurl")
        if label == "subscriptions" or (ch_url is not None and col("channeltitle") is not None) or (
            roles.get("channel_id") and roles.get("channel_url") and not roles.get("video_id")
        ):
            return self._csv_subscriptions(header, rows, nh, label)
        # --- own posts ---
        p_id = col("postid", contains=("post", "id"))
        if p_id is not None and (col("posttext") is not None or label == "own_posts"):
            n = 0
            for r in rows:
                if p_id < len(r) and r[p_id].strip():
                    it = self.inv.get("post", r[p_id].strip(), "own_posts")
                    it.takeout_extra.append({h: v for h, v in zip(header, r) if v})
                    n += 1
            return "own_posts", n
        # --- generic: anything with a video id column (playlists, uploads, music...) ---
        v_id = col("videoid", contains=("video", "id"))
        if v_id is None:
            if roles.get("video_id"):
                v_id = roles["video_id"][0]
        n = 0
        if v_id is not None:
            ts = col(contains=("timestamp",))
            if ts is None and roles.get("timestamp"):
                ts = roles["timestamp"][0]
            title_col = col("videotitleoriginal", "videotitle")
            for r in rows:
                if v_id < len(r) and VIDEO_ID_RE.match(r[v_id].strip()):
                    it = self.inv.get("video", r[v_id].strip(), label)
                    if title_col is not None and title_col < len(r) and r[title_col].strip() and not it.title:
                        it.title = r[title_col].strip()
                    if ts is not None and ts < len(r):
                        it.activity_times.append(r[ts])
                    n += 1
        if p_id is not None:
            for r in rows:
                if p_id < len(r) and r[p_id].strip():
                    self.inv.get("post", r[p_id].strip(), label)
                    n += 1
        return ("generic_ids" if n else "no_ids"), n

    def _csv_comments(self, tf: TakeoutFile, header: list[str], rows: list[list[str]], nh: list[str], label: str) -> tuple[str, int]:
        def idx(*names: str) -> int | None:
            for n in names:
                if n in nh:
                    return nh.index(n)
            return None

        m = {
            "cid": idx("commentid"),
            "chan": idx("channelid"),
            "ts": idx("commentcreatetimestamp", "creationtimestamp", "commentcreatetime"),
            "price": idx("price"),
            "parent": idx("parentcommentid"),
            "post": idx("postid"),
            "video": idx("videoid"),
            "text": idx("commenttext", "text"),
        }
        if m["cid"] is None or m["text"] is None:  # localized header -> infer by values
            roles = self._profile(header, rows)
            m["text"] = m["text"] if m["text"] is not None else first(roles.get("json_text", []))
            m["ts"] = m["ts"] if m["ts"] is not None else first(roles.get("timestamp", []))
            m["chan"] = m["chan"] if m["chan"] is not None else first(roles.get("channel_id", []))
            m["video"] = m["video"] if m["video"] is not None else first(roles.get("video_id", []))
            ug = roles.get("ug_id", [])
            # the most populated Ug-column is the comment id, the others parent/post
            ug.sort(key=lambda i: -sum(1 for r in rows if i < len(r) and r[i].strip()))
            if ug and m["cid"] is None:
                m["cid"] = ug[0]
            vcol = m["video"]
            for i in ug[1:]:
                filled = [r for r in rows if i < len(r) and r[i].strip()]
                # post comments have no video id; parent ids sit next to one
                no_video = sum(1 for r in filled if vcol is None or vcol >= len(r) or not r[vcol].strip())
                if filled and (no_video / len(filled) > 0.9 or all(r[i].startswith("Ugkx") for r in filled)):
                    m["post"] = i if m["post"] is None else m["post"]
                elif m["parent"] is None:
                    m["parent"] = i
        if m["cid"] is None:
            return "comments(unrecognized)", 0

        def g(r: list[str], k: str) -> str:
            i = m[k]
            return r[i].strip() if i is not None and i < len(r) else ""

        n = 0
        for r in rows:
            cid, parent = g(r, "cid"), g(r, "parent") or None
            if parent and "." not in cid:
                cid = f"{parent}.{cid}"
            if not parent and "." in cid:
                parent = cid.split(".", 1)[0]
            text, links = parse_takeout_text(r[m["text"]] if m["text"] is not None and m["text"] < len(r) else "")
            uc = UserComment(
                comment_id=cid,
                parent_id=parent,
                video_id=g(r, "video") or None,
                post_id=g(r, "post") or None,
                text=text,
                links=links,
                created=g(r, "ts") or None,
                channel_id=g(r, "chan") or None,
                price=g(r, "price") or None,
                source="comments",
            )
            if uc.video_id or uc.post_id:
                self.inv.add_comment(uc)
                n += 1
        return "comments", n

    def _csv_live_chats(self, header: list[str], rows: list[list[str]], nh: list[str], label: str) -> tuple[str, int]:
        roles = self._profile(header, rows)

        def idx(names: tuple[str, ...], role: str) -> int | None:
            for n in names:
                if n in nh:
                    return nh.index(n)
            return first(roles.get(role, []))

        v = idx(("videoid",), "video_id")
        t = idx(("livechattext", "messagetext", "text"), "json_text")
        ts = idx(("livechatcreatetimestamp", "creationtimestamp"), "timestamp")
        if v is None:
            return "live_chats(unrecognized)", 0
        n = 0
        for r in rows:
            if v >= len(r) or not VIDEO_ID_RE.match(r[v].strip()):
                continue
            it = self.inv.get("video", r[v].strip(), "live_chats")
            text, _ = parse_takeout_text(r[t]) if t is not None and t < len(r) else ("", [])
            it.live_chat_messages.append({"text": text, "created": r[ts] if ts is not None and ts < len(r) else None})
            n += 1
        return "live_chats", n

    def _csv_subscriptions(self, header: list[str], rows: list[list[str]], nh: list[str], label: str) -> tuple[str, int]:
        roles = self._profile(header, rows)
        cid = nh.index("channelid") if "channelid" in nh else first(roles.get("channel_id", []))
        curl = nh.index("channelurl") if "channelurl" in nh else first(roles.get("channel_url", []))
        title = nh.index("channeltitle") if "channeltitle" in nh else None
        if title is None:  # the remaining column is the title
            rest = [i for i in range(len(header)) if i not in (cid, curl)]
            title = rest[0] if rest else None
        n = 0
        for r in rows:
            ch = r[cid].strip() if cid is not None and cid < len(r) else ""
            if not CHANNEL_ID_RE.match(ch) and curl is not None and curl < len(r):
                m = URL_CHANNEL_RE.search(r[curl])
                ch = m.group(1) if m else ""
            if not ch:
                continue
            it = self.inv.get("channel", ch, "subscriptions")
            if title is not None and title < len(r):
                it.title = r[title].strip() or it.title
            n += 1
        return "subscriptions", n

    # ---------------- JSON ----------------

    def parse_json(self, tf: TakeoutFile) -> tuple[str, int]:
        raw = tf.read_text()
        try:
            data = json.loads(raw)
        except ValueError:
            return "invalid_json", 0
        label = source_label(tf.rel)
        if label == "search_history":
            return "search_history(ignored)", 0
        n = 0
        # watch-history.json / MyActivity.json : list of activity records
        if isinstance(data, list) and data and isinstance(data[0], dict) and ("titleUrl" in data[0] or "header" in data[0]):
            for e in data:
                if not isinstance(e, dict):
                    continue
                if any(_is_ad_marker(d.get("name") or "") for d in e.get("details", []) if isinstance(d, dict)):
                    continue  # ads that were played, not something you watched
                n += self._activity(e.get("titleUrl") or "", e.get("title") or "", e.get("time"), e.get("subtitles") or [], label)
            return "activity", n
        # old playlists/likes.json etc: generic url / id scan
        if isinstance(data, list):
            for e in data:
                vid = (((e or {}).get("contentDetails") or {}).get("videoId")) if isinstance(e, dict) else None
                if vid and VIDEO_ID_RE.match(vid):
                    self.inv.get("video", vid, label)
                    n += 1
        return self._scan_urls(raw, label, n)

    _ACTIVITY_PREFIX = re.compile(
        r"^(Watched|Viewed|Liked|Disliked|Commented on|Answered|Voted on|Shared"
        r"|Вы посмотрели(?: видео)?|Вы просмотрели(?: видео)?|Просмотрено|Просмотрен[оа]?"
        r"|Вам понравилось(?: видео)?|Вы оценили(?: видео)?|Понравилось)\s*:?\s+",
        re.I,
    )

    def _activity(self, url: str, title: str, when: str | None, subtitles: list, label: str) -> int:
        url = url.replace("\\u003d", "=")
        m = URL_VIDEO_RE.search(url)
        kind, id_ = (("video", m.group(1)) if m else (None, None))
        if not kind:
            m = URL_POST_RE.search(url)
            if m:
                kind, id_ = "post", m.group(1)
        if not kind:
            return 0
        it = self.inv.get(kind, id_, label)
        t = self._ACTIVITY_PREFIX.sub("", title).strip().strip("«»\"")
        if t and not t.startswith("http") and not it.title:
            it.title = t
        if when:
            it.activity_times.append(when)
        if subtitles and isinstance(subtitles[0], dict):
            it.channel_name = it.channel_name or subtitles[0].get("name")
            it.channel_url = it.channel_url or subtitles[0].get("url")
        return 1

    def _scan_urls(self, raw: str, label: str, n: int = 0) -> tuple[str, int]:
        for vid in set(URL_VIDEO_RE.findall(raw)):
            self.inv.get("video", vid, label)
            n += 1
        for pid in set(URL_POST_RE.findall(raw)):
            self.inv.get("post", pid, label)
            n += 1
        return ("url_scan" if n else "no_ids"), n

    # ---------------- HTML (older exports) ----------------

    def parse_html(self, tf: TakeoutFile) -> tuple[str, int]:
        raw = tf.read_text()
        label = source_label(tf.rel)
        if label == "search_history":
            return "search_history(ignored)", 0
        if not _installed("bs4"):
            log.warning("beautifulsoup4 not installed - falling back to URL scan for %s", tf.rel)
            return self._scan_urls(raw, label)
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(raw, "lxml" if _installed("lxml") else "html.parser")
        if label in ("comments", "live_chats"):
            return self._html_comments(soup, label)
        cells = soup.select("div.content-cell") or []
        n = 0
        for c in cells:
            txt = c.get_text(" ", strip=True)
            if not txt or _is_ad_marker(txt):
                continue
            links = [a for a in c.find_all("a") if a.get("href")]
            if not links:
                continue
            href = links[0]["href"]
            when = None
            strings = list(c.stripped_strings)
            if strings:
                when = strings[-1]
            subs = [{"name": a.get_text(strip=True), "url": a["href"]} for a in links[1:2]]
            n += self._activity(href, links[0].get_text(strip=True), when, subs, label)
        if n:
            return "activity_html", n
        return self._scan_urls(raw, label)

    def _html_comments(self, soup: Any, label: str) -> tuple[str, int]:
        n = 0
        date_re = re.compile(r"(\d{4})-(\d{2})-(\d{2})(?:\s*|T)(\d{2}):(\d{2}):(\d{2})")
        for li in soup.select("li"):
            hrefs = [a["href"] for a in li.find_all("a") if a.get("href")]
            vid = post = cid = None
            for h in hrefs:
                h = h.replace("&amp;", "&")
                vid = vid or _group1(URL_VIDEO_RE, h)
                post = post or _group1(URL_POST_RE, h)
                cid = cid or _group1(URL_LC_RE, h)
            if not vid and not post:
                continue
            # text after the first <br>
            br = li.find("br")
            text = ""
            if br is not None:
                text = "".join(s if isinstance(s, str) else s.get_text() for s in br.next_siblings).strip()
            dm = date_re.search(li.get_text(" "))
            created = f"{dm.group(1)}-{dm.group(2)}-{dm.group(3)}T{dm.group(4)}:{dm.group(5)}:{dm.group(6)}+00:00" if dm else None
            if label == "live_chats":
                if vid:
                    self.inv.get("video", vid, "live_chats").live_chat_messages.append({"text": text, "created": created})
                    n += 1
                continue
            parent = cid.split(".", 1)[0] if cid and "." in cid else None
            self.inv.add_comment(
                UserComment(
                    comment_id=cid or "",
                    parent_id=parent,
                    video_id=None if post else vid,
                    post_id=post,
                    text=text,
                    links=[],
                    created=created,
                    channel_id=None,
                    price=None,
                    source="comments",
                )
            )
            n += 1
        return "comments_html", n


def _is_ad_marker(text: str) -> bool:
    """'From Google Ads', 'Von Google Ads', 'Из Google Рекламы', ..."""
    low = text.lower()
    return "google ads" in low or "google реклам" in low or "google рекламы" in low


def _group1(rx: re.Pattern, s: str) -> str | None:
    m = rx.search(s)
    return m.group(1) if m else None


def _installed(module: str) -> bool:
    return importlib.util.find_spec(module) is not None


# --------------------------------------------------------------------------------------
# HTTP + InnerTube client
# --------------------------------------------------------------------------------------


BOT_CHECK_RE = re.compile(r"confirm you.{0,3}re not a bot|не бот", re.I)
# temporary failures: the item is saved as "error" and retried on the next run
TRANSIENT_RE = re.compile(
    r"try again later|not a bot|не бот|please sign in|HTTP Error (429|5\d\d)|timed? ?out|failed to extract any player response"
    r"|unable to download (api page|webpage)|connection|temporar",
    re.I,
)
# reasons that will not change by retrying (without cookies): saved as "unavailable"
PERMANENT_RE = re.compile(
    r"video unavailable|video is unavailable|private video|been removed|terminated|does not exist|no longer available"
    r"|copyright|been deleted|members.only|join this channel|confirm your age|age.restricted|not available in your country"
    r"|not made this video available|content isn.t available|недоступно|удалено",
    re.I,
)


def classify_failure(msg: str | None) -> str:
    """'unavailable' (permanent, not retried) or 'error' (retried next run)."""
    msg = msg or ""
    if TRANSIENT_RE.search(msg):
        return "error"
    return "unavailable" if PERMANENT_RE.search(msg) else "error"


class RateLimited(RuntimeError):
    pass


# --------------------------------------------------------------------------------------
# Adaptive concurrency
#
# Every outgoing request belongs to a *class* (InnerTube API, yt-dlp extraction,
# media downloads, Return YouTube Dislike). Each class has an AdaptiveLimiter
# that decides how many requests of that class may be in flight, and the
# WorkerGovernor decides how many items are processed in parallel. Both adapt at
# run time and persist what they learned in OUT/.adaptive_state.json, so the
# next run starts from the measured values instead of guesses.
# --------------------------------------------------------------------------------------


class Ewma:
    def __init__(self, alpha: float):
        self.alpha = alpha
        self.value: float | None = None

    def add(self, x: float) -> float:
        self.value = x if self.value is None else self.value + self.alpha * (x - self.value)
        return self.value


class Ticket:
    """Handed to the code inside AdaptiveLimiter.slot() to report the outcome."""

    __slots__ = ("outcome", "nbytes", "inflight_at_start", "epoch", "key")

    def __init__(self, inflight_at_start: int, epoch: int, key: str = ""):
        self.key = key  # request kind; each kind has its own latency baseline
        self.outcome = "ok"  # ok | error | throttle
        self.nbytes = 0
        self.inflight_at_start = inflight_at_start
        self.epoch = epoch  # throttle reactions seen when the request started

    def fail(self) -> None:
        self.outcome = "error"

    def throttle(self) -> None:
        self.outcome = "throttle"


class AdaptiveLimiter:
    """Concurrency limit for one class of requests (gradient/AIMD scheme, like
    TCP Vegas or Netflix's concurrency-limits):

    * latency stays within 1.5x of the baseline (a slowly decaying minimum, as in
      TCP Vegas) -> the limit grows by ~1 per round of requests, but only while
      it is actually used (no growth when the caller is the bottleneck)
    * latency inflates (server busy or the link saturated) -> it shrinks in
      proportion to the inflation
    * HTTP 429 / bot check -> it halves (once per event, not once per request
      that was already in flight), the whole class pauses with exponential
      back-off, and the limit at which this happened becomes a *ceiling* that is
      remembered across runs
    * timeouts / 5xx -> small multiplicative decrease

    Latency of large downloads is normalized per 256 KiB, so the same rule
    detects bandwidth saturation: once more parallel downloads stop adding
    throughput, per-connection speed drops and the limit stops growing.
    """

    TOLERANCE = 1.5  # latency may grow by 50% over the baseline before shrinking
    WARMUP = 5
    BASELINE_DRIFT = 1.001  # per second: the minimum creeps up ~6%/min so lasting changes are followed

    def __init__(self, name: str, initial: float, min_limit: int = 1, max_limit: int = 16,
                 ceiling: float | None = None, min_interval: float = 0.0, fixed: bool = False):
        self.name = name
        self.min_limit = min_limit
        self.max_limit = max(min_limit, max_limit)
        self.ceiling = ceiling
        self.fixed = fixed
        self.limit = float(min(max(initial, min_limit), self.cap()))
        self.min_interval = max(0.0, min_interval)
        self.inflight = 0
        self._next_start = 0.0
        self._cond = threading.Condition()
        # per request kind (endpoint): a comment page and a lyrics page have
        # different natural latencies, only their *inflation* matters
        self._short: dict[str, Ewma] = {}
        self._baseline: dict[str, tuple[float, float]] = {}  # key -> (value, updated_at)
        self._epoch = 0
        self.cooldown_until = 0.0
        self._consecutive_throttles = 0
        self.started = time.monotonic()
        # statistics
        self.requests = self.errors = self.throttles = 0
        self.nbytes = 0
        self.latency_sum = 0.0
        self.peak_limit = self.limit

    def cap(self) -> float:
        cap = float(self.max_limit)
        if self.ceiling and not self.fixed:
            cap = min(cap, max(self.min_limit, self.ceiling * 0.9))
        return cap

    def width(self) -> int:
        """Current limit as an int - used to size sub-task parallelism."""
        return max(1, int(self.limit))

    @contextlib.contextmanager
    def slot(self, key: str = "") -> Iterator[Ticket]:
        ticket = self._acquire()
        ticket.key = key
        t0 = time.monotonic()
        try:
            yield ticket
        except BaseException:
            if ticket.outcome == "ok":
                ticket.fail()
            raise
        finally:
            self._release(time.monotonic() - t0, ticket)

    def _acquire(self) -> Ticket:
        with self._cond:
            while True:
                now = time.monotonic()
                if now < self.cooldown_until:
                    self._cond.wait(self.cooldown_until - now)
                    continue
                if self.inflight >= max(1, int(self.limit)):
                    self._cond.wait(1.0)
                    continue
                if self.min_interval and now < self._next_start:
                    self._cond.wait(self._next_start - now)
                    continue
                self._next_start = now + self.min_interval
                ticket = Ticket(self.inflight, self._epoch)
                self.inflight += 1
                return ticket

    def _release(self, latency: float, t: Ticket) -> None:
        with self._cond:
            self.inflight -= 1
            self.requests += 1
            self.latency_sum += latency
            self.nbytes += t.nbytes
            if t.outcome == "throttle":
                self._on_throttle(t)
            elif t.outcome == "error":
                self.errors += 1
                if not self.fixed:
                    self.limit = max(self.min_limit, self.limit * 0.9)
            else:
                self._consecutive_throttles = 0
                self._on_success(latency, t)
            self._cond.notify_all()

    def _on_success(self, latency: float, t: Ticket) -> None:
        sample = latency / max(1.0, t.nbytes / 262144)
        short = self._short.setdefault(t.key, Ewma(0.3)).add(sample)
        now = time.monotonic()
        prev = self._baseline.get(t.key)
        baseline = short if prev is None else min(short, prev[0] * self.BASELINE_DRIFT ** (now - prev[1]))
        self._baseline[t.key] = (baseline, now)
        if self.fixed or self.requests < self.WARMUP:
            return
        gradient = min(1.0, max(0.5, baseline * self.TOLERANCE / max(short, 1e-6)))
        if gradient < 1.0:  # latency inflated: shrink proportionally (smoothed)
            self.limit = max(self.min_limit, self.limit * (0.9 + 0.1 * gradient))
            return
        # only grow while the limit is the bottleneck (most of the usable slots busy)
        if t.inflight_at_start + 1 >= max(1, int(self.limit)) * 0.75:
            self.limit = min(self.cap(), self.limit + 1.0 / self.limit)
            self.peak_limit = max(self.peak_limit, self.limit)

    def _on_throttle(self, t: Ticket) -> None:
        self.throttles += 1
        if t.epoch < self._epoch:
            return  # started before the last reaction: same event, already handled
        self._epoch += 1
        self._consecutive_throttles += 1
        if not self.fixed:
            self.ceiling = min(self.ceiling or float("inf"), max(float(self.min_limit), self.limit))
            self.limit = max(self.min_limit, self.limit * 0.5)
        pause = min(300.0, 10.0 * 2 ** (self._consecutive_throttles - 1))
        self.cooldown_until = max(self.cooldown_until, time.monotonic() + pause)
        log.warning("adaptive[%s]: throttled by the server -> limit %.1f, pausing %ds", self.name, self.limit, pause)

    def snapshot(self) -> dict:
        with self._cond:
            elapsed = max(1e-6, time.monotonic() - self.started)
            return {
                "limit": round(self.limit, 2),
                "peak_limit": round(self.peak_limit, 2),
                "ceiling": round(self.ceiling, 2) if self.ceiling else None,
                "requests": self.requests,
                "errors": self.errors,
                "throttles": self.throttles,
                "avg_latency_s": round(self.latency_sum / self.requests, 3) if self.requests else None,
                "baseline_latency_s": {k: round(v[0], 3) for k, v in sorted(self._baseline.items())},
                "bytes": self.nbytes,
                "throughput_bps": round(self.nbytes / elapsed),
            }


class Gate:
    """Semaphore whose size can change while threads wait on it."""

    def __init__(self, size: int):
        self.size = max(1, size)
        self.active = 0
        self._cond = threading.Condition()

    def resize(self, size: int) -> None:
        with self._cond:
            self.size = max(1, size)
            self._cond.notify_all()

    @contextlib.contextmanager
    def enter(self) -> Iterator[None]:
        with self._cond:
            while self.active >= self.size:
                self._cond.wait()
            self.active += 1
        try:
            yield
        finally:
            with self._cond:
                self.active -= 1
                self._cond.notify_all()


class WorkerGovernor:
    """Number of items processed in parallel, tuned by hill-climbing on the
    measured throughput (items per minute):

    * every window (>= 30 s and enough finished items) the throughput is compared
      with the previous window; while it improves the governor keeps moving in the
      same direction, when it gets worse it turns around, on a plateau it stays
    * any throttling in the window cuts the worker count by 30%
    * the worker count is capped by what the request limiters can serve
    * the throughput measured for each worker count is remembered across runs, and
      the next run starts with the best one
    """

    def __init__(self, initial: int, max_workers: int, limiters: dict[str, AdaptiveLimiter],
                 history: dict[int, float] | None = None, fixed: bool = False, window_s: float = 30.0):
        self.max_workers = max(1, max_workers)
        self.workers = min(max(1, initial), self.max_workers)
        self.fixed = fixed
        self.limiters = limiters
        self.history: dict[int, float] = dict(history or {})
        self.window_s = window_s
        self.gate = Gate(self.workers)
        self.remaining = 0
        self._lock = threading.Lock()
        self._direction = 1
        self._prev_tput: float | None = None
        self._reset_window()
        self.started = time.monotonic()
        self.done = 0
        self.worker_seconds = 0.0  # integral of workers over time (for the average)
        self._last_tick = self.started

    def _reset_window(self) -> None:
        self._win_start = time.monotonic()
        self._win_done = 0
        self._win_throttles = sum(lim.throttles for lim in self.limiters.values())

    def item_finished(self) -> None:
        with self._lock:
            now = time.monotonic()
            self.worker_seconds += self.workers * (now - self._last_tick)
            self._last_tick = now
            self.done += 1
            self.remaining = max(0, self.remaining - 1)
            self._win_done += 1
            elapsed = now - self._win_start
            if elapsed < self.window_s or self._win_done < max(3, self.workers):
                return
            tput = self._win_done * 60.0 / elapsed
            old = self.history.get(self.workers)
            self.history[self.workers] = tput if old is None else old + 0.5 * (tput - old)
            throttled = sum(lim.throttles for lim in self.limiters.values()) > self._win_throttles
            new = self.workers
            if not self.fixed:
                if throttled:
                    new, self._direction = max(1, int(self.workers * 0.7)), -1
                elif self._prev_tput is None:
                    new = self.workers + self._direction
                elif tput >= self._prev_tput * 1.05:
                    new = self.workers + self._direction
                elif tput <= self._prev_tput * 0.95:
                    self._direction = -self._direction
                    new = self.workers + self._direction
                # every worker needs about one request slot to make progress
                slots = sum(lim.width() for lim in self.limiters.values())
                cap = max(1, min(self.max_workers, slots, self.remaining or self.max_workers))
                new = min(max(1, new), cap)
            log.debug(
                "adaptive: %.1f items/min with %d workers; %s",
                tput, self.workers, ", ".join(f"{k} {v.limit:.1f}" for k, v in self.limiters.items()),
            )
            if new != self.workers:
                log.info("adaptive: workers %d -> %d (%.1f items/min)", self.workers, new, tput)
                self.workers = new
                self.gate.resize(new)
            self._prev_tput = tput
            self._reset_window()

    def close(self) -> None:
        """Record the unfinished last window (short runs still teach something)."""
        with self._lock:
            elapsed = time.monotonic() - self._win_start
            if self._win_done >= 3 and elapsed > 1:
                tput = self._win_done * 60.0 / elapsed
                old = self.history.get(self.workers)
                self.history[self.workers] = tput if old is None else old + 0.25 * (tput - old)

    def snapshot(self) -> dict:
        with self._lock:
            elapsed = max(1e-6, time.monotonic() - self.started)
            return {
                "final": self.workers,
                "average": round(self.worker_seconds / elapsed, 2) if self.done else self.workers,
                "items": self.done,
                "items_per_min": round(self.done * 60.0 / elapsed, 2),
                "history": {str(k): round(v, 2) for k, v in sorted(self.history.items())},
            }

    def best_known(self) -> int | None:
        if not self.history:
            return None
        return max(self.history.items(), key=lambda kv: kv[1])[0]


class AdaptiveState:
    """What previous runs learned (OUT/.adaptive_state.json)."""

    VERSION = 1
    FILE = ".adaptive_state.json"

    def __init__(self, path: Path):
        self.path = path
        self.reset()
        try:
            loaded = json.loads(path.read_text("utf-8"))
            if loaded.get("version") == self.VERSION:
                self.data.update(loaded)
        except (OSError, ValueError):
            pass

    def reset(self) -> None:
        self.data: dict[str, Any] = {"version": self.VERSION, "limiters": {}, "workers": {}, "network": {}, "runs": []}

    def limiter_start(self, name: str, default: float) -> tuple[float, float | None]:
        """(initial limit, ceiling) for a limiter. The previous final limit is
        reused with a small safety margin; a learned ceiling is forgiven slowly
        (+25% after 6 h, dropped after 3 days) because YouTube's limits change."""
        s = self.data["limiters"].get(name) or {}
        initial = s.get("limit")
        # a learned limit above the default is reused with a safety margin; lower
        # values are not (short runs never grow the limit, and a throttling server
        # is remembered by the ceiling, not by the limit)
        initial = max(default, initial * 0.8) if isinstance(initial, (int, float)) else default
        ceiling = s.get("ceiling")
        if ceiling and s.get("ceiling_at"):
            try:
                age_h = (datetime.now(timezone.utc) - datetime.fromisoformat(s["ceiling_at"])).total_seconds() / 3600
            except ValueError:
                age_h = 0.0
            if age_h > 72:
                ceiling = None
            elif age_h > 6:
                ceiling *= 1.25
        return initial, ceiling

    def worker_history(self) -> dict[int, float]:
        h = (self.data.get("workers") or {}).get("history") or {}
        out = {}
        for k, v in h.items():
            try:
                out[int(k)] = float(v)
            except (TypeError, ValueError):
                pass
        return out

    def save(self, limiters: dict[str, AdaptiveLimiter], governor: WorkerGovernor | None, network: dict | None,
             run: dict | None = None) -> None:
        prev = self.data["limiters"]
        for name, lim in limiters.items():
            snap = lim.snapshot()
            old = prev.get(name) or {}
            entry = {
                "limit": snap["limit"],
                "peak_limit": snap["peak_limit"],
                "ceiling": snap["ceiling"],
                "ceiling_at": old.get("ceiling_at"),
                "baseline_latency_s": snap["baseline_latency_s"],
                "total_requests": (old.get("total_requests") or 0) + snap["requests"],
                "total_throttles": (old.get("total_throttles") or 0) + snap["throttles"],
            }
            if snap["throttles"]:
                entry["ceiling_at"] = now_iso()
            if lim.fixed or snap["requests"] < 20:
                # a user-forced value says nothing about the server, and a few
                # requests are not enough evidence to replace the learned limit
                entry["limit"] = old.get("limit", entry["limit"])
                entry["peak_limit"] = old.get("peak_limit", entry["peak_limit"])
            prev[name] = entry
        if governor is not None and governor.done:
            g = governor.snapshot()
            self.data["workers"] = {"history": g["history"], "last": g["final"], "best": governor.best_known()}
        if network:
            self.data["network"] = network
        if run:
            self.data["runs"] = (self.data.get("runs") or [])[-19:] + [run]
        self.data["updated_at"] = now_iso()
        tmp = self.path.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(self.data, ensure_ascii=False, indent=1), "utf-8")
            tmp.replace(self.path)
        except OSError as e:
            log.debug("could not save adaptive state: %s", e)


def probe_network(session: requests.Session) -> dict:
    """Rough latency/bandwidth measurement used to seed the limiters."""
    rtts = []
    for _ in range(3):
        t0 = time.monotonic()
        try:
            session.get("https://www.youtube.com/generate_204", timeout=10)
            rtts.append(time.monotonic() - t0)
        except requests.RequestException:
            pass
    rtt = sorted(rtts)[len(rtts) // 2] if rtts else None
    bw = None
    try:
        session.get("https://i.ytimg.com/generate_204", timeout=10)  # open the CDN connection first
        size, dt = 0, 0.0
        for vid in ("dQw4w9WgXcQ", "jNQXAC9IVRw", "9bZkp7q19f0"):
            t0 = time.monotonic()
            r = session.get(f"https://i.ytimg.com/vi/{vid}/maxresdefault.jpg", timeout=20)
            if r.ok:
                size, dt = size + len(r.content), dt + time.monotonic() - t0
        if size:
            bw = size / max(dt, 1e-3)
    except requests.RequestException:
        pass
    return {
        "rtt_ms": round(rtt * 1000) if rtt else None,
        "bandwidth_mbps": round(bw * 8 / 1e6, 1) if bw else None,
        "measured_at": now_iso(),
    }


# Request classes and their bounds. YouTube's own endpoints are kept on a short
# leash: flooding them gets the IP flagged ("confirm you're not a bot") for hours.
LIMITER_BOUNDS = {
    #          (default start, max)
    "api": (2, 8),      # InnerTube (comments, posts, lyrics) + subtitle files
    "ytdlp": (2, 4),    # one yt-dlp extraction = several requests
    "media": (4, 16),   # thumbnails, banners, post images (CDN)
    "ryd": (2, 4),      # Return YouTube Dislike (third party, be polite)
}


def build_limiters(state: AdaptiveState, network: dict | None, fixed_api: int | None, min_interval: float) -> dict[str, AdaptiveLimiter]:
    rtt = (network or {}).get("rtt_ms")
    bw = (network or {}).get("bandwidth_mbps")
    defaults = {k: float(v[0]) for k, v in LIMITER_BOUNDS.items()}
    if rtt:  # high latency links need more requests in flight for the same rate
        defaults["api"] = 2.0 if rtt < 150 else 3.0 if rtt < 400 else 4.0
    max_media = LIMITER_BOUNDS["media"][1]
    if bw:  # ~1 parallel download per 10 Mbit/s, at least 2
        defaults["media"] = float(min(8, max(2, round(bw / 10))))
        max_media = int(min(16, max(4, round(bw / 5))))
    limiters = {}
    for name, (_, max_limit) in LIMITER_BOUNDS.items():
        initial, ceiling = state.limiter_start(name, defaults[name])
        if name == "media":
            max_limit = max_media
        fixed = name == "api" and fixed_api is not None
        if fixed:
            initial, max_limit = float(fixed_api), max(1, fixed_api)
        limiters[name] = AdaptiveLimiter(name, initial, max_limit=max_limit, ceiling=ceiling,
                                         min_interval=min_interval if name in ("api", "ytdlp") else 0.0, fixed=fixed)
    return limiters


class Http:
    """Shared, thread-safe HTTP layer: one session for YouTube (headers, cookies),
    one plain session for third parties/CDNs; every request goes through the
    limiter of its class and reports latency, size and throttling to it."""

    def __init__(self, limiters: dict[str, AdaptiveLimiter], hl: str, cookies: http.cookiejar.CookieJar | None):
        self.limiters = limiters
        self.yt = self._session()
        self.yt.headers.update({"Accept-Language": f"{hl},en;q=0.8"})
        self.yt.cookies.set("SOCS", "CAI", domain=".youtube.com")  # skip EU consent
        if cookies is not None:
            for c in cookies:
                self.yt.cookies.set_cookie(c)
        self.plain = self._session()

    @staticmethod
    def _session() -> requests.Session:
        s = requests.Session()
        s.headers["User-Agent"] = WEB_UA
        adapter = requests.adapters.HTTPAdapter(pool_connections=8, pool_maxsize=32, max_retries=0)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        return s

    def request(self, cls: str, method: str, url: str, *, youtube: bool = False, **kw: Any) -> requests.Response:
        kw.setdefault("timeout", 45)
        session = self.yt if youtube else self.plain
        key = urllib.parse.urlparse(url).path.rsplit("/", 1)[-1] if cls == "api" else ""
        with self.limiters[cls].slot(key) as t:
            r = session.request(method, url, **kw)
            t.nbytes = len(r.content)
            if r.status_code == 429 or "google.com/sorry" in r.url:
                t.throttle()
            elif r.status_code >= 500:
                t.fail()
        return r


def parallel_map(fn: Callable[[Any], Any], items: Iterable[Any], width: int) -> list[Any]:
    """Order-preserving map over a small thread pool (inline for <= 1 item).
    The first exception is re-raised after all started calls finished."""
    items = list(items)
    if len(items) <= 1 or width <= 1:
        return [fn(x) for x in items]
    with cf.ThreadPoolExecutor(max_workers=min(width, len(items))) as pool:
        return list(pool.map(fn, items))


def run_tasks(tasks: dict[str, Callable[[], Any]], width: int) -> dict[str, tuple[Any, Exception | None]]:
    """Run independent callables in parallel; returns name -> (result, error).
    RateLimited is re-raised so the run can stop."""

    def call(name: str) -> tuple[Any, Exception | None]:
        try:
            return tasks[name](), None
        except RateLimited:
            raise
        except Exception as e:  # recorded per task, the item still gets saved
            log.debug("task %s failed", name, exc_info=True)
            return None, e

    names = list(tasks)
    return dict(zip(names, parallel_map(call, names, width)))


class InnerTube:
    CLIENTS = {
        "web": {
            "base": "https://www.youtube.com/youtubei/v1/",
            "name": "WEB", "id": "1", "version": "2.20251101.00.00", "origin": "https://www.youtube.com",
        },
        "music": {
            "base": "https://music.youtube.com/youtubei/v1/",
            "name": "WEB_REMIX", "id": "67", "version": "1.20251103.01.00", "origin": "https://music.youtube.com",
        },
    }

    def __init__(self, http_: Http, hl: str = "en", gl: str = "US", retries: int = 5):
        self.http = http_
        self.hl, self.gl = hl, gl
        self.retries = retries
        self.sapisid = next((c.value for c in http_.yt.cookies if c.name in ("SAPISID", "__Secure-3PAPISID")), None)

    def _headers(self, client: dict) -> dict:
        h = {
            "Origin": client["origin"],
            "Referer": client["origin"] + "/",
            "X-Youtube-Client-Name": client["id"],
            "X-Youtube-Client-Version": client["version"],
        }
        if self.sapisid:
            ts = str(int(time.time()))
            digest = hashlib.sha1(f"{ts} {self.sapisid} {client['origin']}".encode()).hexdigest()
            h.update({"Authorization": f"SAPISIDHASH {ts}_{digest}", "X-Goog-AuthUser": "0", "X-Origin": client["origin"]})
        return h

    def call(self, endpoint: str, body: dict, client: str = "web") -> dict:
        c = self.CLIENTS[client]
        endpoint = endpoint.lstrip("/").replace("youtubei/v1/", "")
        payload = {"context": {"client": {"clientName": c["name"], "clientVersion": c["version"], "hl": self.hl, "gl": self.gl}}, **body}
        last_err: Exception | None = None
        for attempt in range(self.retries):
            try:
                r = self.http.request("api", "POST", c["base"] + endpoint, youtube=True, params={"prettyPrint": "false"},
                                      json=payload, headers=self._headers(c))
            except requests.RequestException as e:
                last_err = e
                time.sleep(2 * 2**attempt)
                continue
            if r.status_code == 200:
                try:
                    return r.json()
                except ValueError as e:  # e.g. an HTML interstitial
                    last_err = e
                    continue
            if r.status_code in (400, 404):
                try:
                    return {"__error__": r.status_code, **r.json()}
                except ValueError:
                    return {"__error__": r.status_code}
            if r.status_code == 429 or "google.com/sorry" in r.url:
                # the limiter already paused the whole class; just try again
                last_err = RateLimited("YouTube rate limit (HTTP 429)")
                continue
            last_err = RuntimeError(f"HTTP {r.status_code} from {endpoint}")
            time.sleep(3 * 2**attempt)
        raise last_err or RuntimeError("request failed")

    # ---------------- lyrics (YouTube Music) ----------------

    def lyrics(self, video_id: str) -> dict:
        nxt = self.call("next", {"videoId": video_id, "isAudioOnly": True}, client="music")
        if "__error__" in nxt:
            return {"status": "unavailable", "error": f"HTTP {nxt['__error__']}"}
        tab = next(
            (t for t in walk(nxt, "tabRenderer")
             if (((t.get("endpoint") or {}).get("browseEndpoint") or {}).get("browseId") or "").startswith("MPLY")),
            None,
        )
        if tab is None or tab.get("unselectable"):
            return {"status": "unavailable"}
        data = self.call("browse", {"browseId": tab["endpoint"]["browseEndpoint"]["browseId"]}, client="music")
        shelf = first(walk(data, "musicDescriptionShelfRenderer"))
        if not shelf:
            msg = first(walk(data, "messageRenderer"))
            return {"status": "unavailable", "message": text_of((msg or {}).get("text")) or None}
        source = text_of(shelf.get("footer"))
        return {
            "status": "ok",
            "text": text_of(shelf.get("description")),
            "source": re.sub(r"^(Source|Источник):\s*", "", source) or None,
        }

    # ---------------- comments ----------------

    def _comment(self, vm: dict, ents: dict, states: dict) -> dict | None:
        if "commentKey" not in vm and "commentViewModel" in vm:
            vm = vm["commentViewModel"]
        ent = ents.get(vm.get("commentKey")) or ents.get(("id", vm.get("commentId")))
        if not ent:
            return None
        p, a, tb = ent.get("properties", {}), ent.get("author", {}), ent.get("toolbar", {})
        st = states.get(p.get("toolbarStateKey") or vm.get("toolbarStateKey")) or {}
        cid = p.get("commentId") or vm.get("commentId")
        published = p.get("publishedTime") or ""
        likes_txt = (tb.get("likeCountNotliked") or "").strip() or "0"
        likes = parse_count(tb.get("likeCountA11y")) if tb.get("likeCountA11y") else parse_count(likes_txt)
        out = {
            "id": cid,
            "parent_id": cid.split(".", 1)[0] if cid and "." in cid else None,
            "text": (p.get("content") or {}).get("content", ""),
            "author": {
                "name": a.get("displayName"),
                "channel_id": a.get("channelId"),
                "avatar": a.get("avatarThumbnailUrl"),
                "is_verified": a.get("isVerified", False),
                "is_creator": a.get("isCreator", False),
                "is_artist": a.get("isArtist", False),
            },
            "published": re.sub(r"\s*\((edited|изменено)\)\s*$", "", published),
            "edited": bool(re.search(r"\((edited|изменено)\)", published)),
            "likes": likes,
            "likes_text": likes_txt,
            "reply_count": parse_count(tb.get("replyCount")) or 0,
            "hearted_by_creator": st.get("heartState") == "TOOLBAR_HEART_STATE_HEARTED",
        }
        if vm.get("pinnedText"):
            out["pinned"] = True
        if vm.get("linkedCommentText"):
            out["highlight_label"] = vm["linkedCommentText"]
        return out

    @staticmethod
    def _token_from(node: dict) -> tuple[str | None, str | None]:
        cc = first(walk(node, "continuationCommand"))
        if not cc:
            return None, None
        api = first(walk(node, "apiUrl"))
        return cc.get("token"), api

    def comments_page(self, endpoint: str, token: str) -> dict:
        """One page of comments or replies, parsed."""
        resp = self.call(endpoint, {"continuation": token})
        if "__error__" in resp:
            raise RuntimeError(f"comments request failed: HTTP {resp['__error__']} {first(walk(resp, 'message'), '')}")
        ents: dict[Any, dict] = {}
        for e in walk(resp, "commentEntityPayload"):
            ents[e.get("key")] = e
            ents[("id", (e.get("properties") or {}).get("commentId"))] = e
        states = {s.get("key"): s for s in walk(resp, "engagementToolbarStateEntityPayload")}
        page: dict[str, Any] = {"threads": [], "replies": [], "next": None, "count_text": None}
        hdr = first(walk(resp, "commentsHeaderRenderer"))
        if hdr:
            page["count_text"] = text_of(hdr.get("countText")) or None
        for _, action in walk_any(resp, {"reloadContinuationItemsCommand", "appendContinuationItemsAction"}):
            for item in action.get("continuationItems") or []:
                if "commentThreadRenderer" in item:
                    tr = item["commentThreadRenderer"]
                    c = self._comment(tr.get("commentViewModel") or {}, ents, states)
                    if not c:
                        continue
                    rr = (tr.get("replies") or {}).get("commentRepliesRenderer") or {}
                    rtok, rapi = self._token_from({"c": rr.get("contents")}) if rr else (None, None)
                    teasers = [x for x in (self._comment(t.get("commentViewModel") or t, ents, states) for t in rr.get("teaserContents") or []) if x]
                    page["threads"].append({"comment": c, "replies_token": rtok, "replies_api": rapi, "teasers": teasers})
                elif "commentViewModel" in item:
                    c = self._comment(item["commentViewModel"], ents, states)
                    if c:
                        page["replies"].append(c)
                elif "continuationItemRenderer" in item:
                    tok, api = self._token_from(item["continuationItemRenderer"])
                    if tok:
                        page["next"] = (tok, api)
        return page

    def iter_threads(self, endpoint: str, token: str, max_pages: int = 1000) -> Iterator[tuple[dict, dict]]:
        """Yield (thread, page) over successive pages of top-level comments."""
        nxt: tuple[str, str | None] | None = (token, None)
        pages = 0
        while nxt and pages < max_pages:
            tok, api = nxt
            page = self.comments_page(api or endpoint, tok)
            pages += 1
            for t in page["threads"]:
                yield t, page
            nxt = page["next"]
            if not page["threads"]:
                break

    def replies(self, endpoint: str, thread: dict, limit: int) -> list[dict]:
        """All (or `limit`) replies of a thread, in YouTube's order. limit<0 = all."""
        out: list[dict] = []
        tok, api = thread.get("replies_token"), thread.get("replies_api")
        seen = set()
        while tok and (limit < 0 or len(out) < limit):
            page = self.comments_page(api or endpoint, tok)
            new = [r for r in page["replies"] if r["id"] not in seen]
            for r in new:
                seen.add(r["id"])
            out.extend(new)
            if not new or not page["next"]:
                break
            tok, api = page["next"]
        return out if limit < 0 else out[:limit]

    # ---------------- posts ----------------

    def post(self, post_id: str) -> dict:
        res = self.call("navigation/resolve_url", {"url": f"https://www.youtube.com/post/{post_id}"})
        be = ((res.get("endpoint") or {}).get("browseEndpoint")) if "__error__" not in res else None
        if not be:
            return {"status": "unavailable", "error": "post URL did not resolve (deleted or private?)"}
        data = self.call("browse", {"browseId": be["browseId"], "params": be.get("params")})
        if "__error__" in data:
            return {"status": "unavailable", "error": f"HTTP {data['__error__']}"}
        r = first(walk(data, "backstagePostRenderer")) or first(walk(data, "sharedPostRenderer"))
        if not r:
            alert = first(walk(data, "alertRenderer")) or first(walk(data, "messageRenderer"))
            msg = text_of((alert or {}).get("text")) if alert else "post renderer not found"
            return {"status": "unavailable", "error": msg or "post renderer not found"}
        return {"status": "ok", "post": self._parse_post(r)}

    def _parse_post(self, r: dict) -> dict:
        disp, exp, links = runs_to_text(r.get("contentText") or r.get("content"))
        author_ep = ((r.get("authorEndpoint") or {}).get("browseEndpoint")) or {}
        if not author_ep:
            author_ep = first(walk(r.get("displayName") or r.get("authorText") or {}, "browseEndpoint")) or {}
        like_label = first(walk(r.get("actionButtons") or {}, "label")) or ""
        m = re.search(r"along with ([\d,.\s]+) other", like_label)
        post: dict[str, Any] = {
            "id": r.get("postId"),
            "url": f"https://www.youtube.com/post/{r.get('postId')}",
            "author": {
                "name": text_of(r.get("authorText") or r.get("displayName")),
                "channel_id": author_ep.get("browseId"),
                "handle": (author_ep.get("canonicalBaseUrl") or "").lstrip("/") or None,
                "avatar": best_thumb((r.get("authorThumbnail") or r.get("thumbnail") or {}).get("thumbnails")),
            },
            "published": text_of(r.get("publishedTimeText")),
            "text": disp,
            "text_expanded": exp,
            "links": links,
            "likes": parse_count(m.group(1)) if m else parse_count(text_of(r.get("voteCount"))),
            "likes_text": text_of(r.get("voteCount")) or None,
            "attachments": [],
            "images": [],
        }
        reply_btn = first(walk(r.get("actionButtons") or {}, "replyButton"))
        if reply_btn:
            post["comment_count_text"] = text_of(first(walk(reply_btn, "text"))) or None
        att = r.get("backstageAttachment") or {}
        for img in walk(att, "backstageImageRenderer"):
            u = best_thumb((img.get("image") or {}).get("thumbnails"))
            if u:
                post["images"].append(u)
        if "pollRenderer" in att:
            pr = att["pollRenderer"]
            post["attachments"].append(
                {
                    "type": "poll",
                    "total_votes": text_of(pr.get("totalVotes")),
                    "choices": [
                        {"text": text_of(c.get("text")), "image": best_thumb((c.get("image") or {}).get("thumbnails"))}
                        for c in pr.get("choices", [])
                    ],
                }
            )
            for c in pr.get("choices", []):
                u = best_thumb((c.get("image") or {}).get("thumbnails"))
                if u:
                    post["images"].append(u)
        if "quizRenderer" in att:
            post["attachments"].append({"type": "quiz", "choices": [text_of(c.get("text")) for c in att["quizRenderer"].get("choices", [])]})
        vr = first(walk(att, "videoRenderer"))
        if vr:
            post["attachments"].append(
                {"type": "video", "video_id": vr.get("videoId"), "title": text_of(vr.get("title")), "url": f"https://www.youtube.com/watch?v={vr.get('videoId')}"}
            )
        pl = first(walk(att, "playlistRenderer"))
        if pl:
            post["attachments"].append({"type": "playlist", "playlist_id": pl.get("playlistId"), "title": text_of(pl.get("title"))})
        if "originalPost" in r:  # sharedPostRenderer
            orig = first(walk(r["originalPost"], "backstagePostRenderer"))
            if orig:
                post["shared_post"] = self._parse_post(orig)
        return post


# --------------------------------------------------------------------------------------
# Enricher
# --------------------------------------------------------------------------------------


# --------------------------------------------------------------------------------------
# Parts: what is fetched for each item, and how completion is tracked
# --------------------------------------------------------------------------------------

PARTS: dict[str, tuple[str, ...]] = {
    "video": ("metadata", "thumbnail", "dislikes", "top-comments", "my-comments", "subtitles", "lyrics"),
    "post": ("metadata", "images", "top-comments", "my-comments"),
    "channel": ("metadata", "banner", "avatar"),
}
ALL_PARTS: tuple[str, ...] = tuple(dict.fromkeys(p for ps in PARTS.values() for p in ps))
PART_GROUPS: dict[str, tuple[str, ...]] = {
    "all": ALL_PARTS,
    "comments": ("top-comments", "my-comments"),
    "media": ("thumbnail", "images", "banner", "avatar"),
    "top": ("top-comments",),
    "my": ("my-comments",),
    "mine": ("my-comments",),
    "user-comments": ("my-comments",),
    "info": ("metadata",),
    "subs": ("subtitles",),
    "captions": ("subtitles",),
    "rydislikes": ("dislikes",),
    "post-images": ("images",),
}
# a part is complete with one of these statuses (and options that cover the current ones)
PART_DONE = ("ok", "none", "skipped")  # none = nothing to fetch, skipped = not applicable (lyrics of non-music)


def parse_parts(spec: str) -> set[str]:
    """'subtitle,comment' -> {'subtitles', 'top-comments', 'my-comments'}; singular/plural and _/- are accepted."""
    out: set[str] = set()
    for raw in spec.split(","):
        name = raw.strip().lower().replace("_", "-").replace(" ", "-")
        if not name:
            continue
        for cand in (name, name + "s", name[:-1] if name.endswith("s") else name):
            if cand in PART_GROUPS:
                out.update(PART_GROUPS[cand])
                break
            if cand in ALL_PARTS:
                out.add(cand)
                break
        else:
            raise argparse.ArgumentTypeError(
                f"unknown part {raw.strip()!r}; valid: {', '.join(ALL_PARTS)} (groups: {', '.join(PART_GROUPS)})"
            )
    return out


@dataclass
class Options:
    out: Path
    parts: frozenset[str] = frozenset(ALL_PARTS)
    top_comments: int = 20
    top_comment_replies: int = 0
    max_replies: int = 100
    original_images: bool = True
    subs: str = "auto"  # manual | auto (manual + auto-generated in the original language)
    sub_langs: tuple[str, ...] = ()  # empty = all
    lyrics: str = "auto"  # auto (music-like videos) | all
    refresh: bool = False
    retry_unavailable: bool = False
    retry_unavailable_videos: bool = False
    hl: str = "en"
    gl: str = "US"
    cookies_file: str | None = None

    def selected(self, kind: str) -> list[str]:
        return [p for p in PARTS[kind] if p in self.parts]

    def part_params(self, part: str) -> dict:
        """Options a part's result depends on; stored with the part."""
        if part == "top-comments":
            return {"n": self.top_comments, "replies": self.top_comment_replies}
        if part == "my-comments":
            return {"max_replies": self.max_replies}
        if part == "subtitles":
            return {"mode": self.subs, "langs": sorted(self.sub_langs)}
        if part == "lyrics":
            return {"mode": self.lyrics}
        if part == "images":
            return {"original": self.original_images}
        return {}


def _covers(part: str, saved: dict | None, wanted: dict) -> bool:
    """Do results fetched with `saved` options satisfy the `wanted` options?"""
    if saved is None:  # record from an older version: options unknown, accept it
        return True
    if part == "top-comments":
        return saved.get("n", 0) >= wanted["n"] and saved.get("replies", 0) >= wanted["replies"]
    if part == "my-comments":
        s, w = saved.get("max_replies", 0), wanted["max_replies"]
        return s == -1 or (w != -1 and s >= w)
    if part == "subtitles":
        mode_ok = saved.get("mode") == wanted["mode"] or saved.get("mode") == "auto"
        langs_ok = not saved.get("langs") or (wanted["langs"] and set(wanted["langs"]) <= set(saved["langs"]))
        return bool(mode_ok and langs_ok)
    if part == "lyrics":
        return saved.get("mode") == wanted["mode"] or saved.get("mode") == "all"
    return saved == wanted


def legacy_parts(res: dict) -> dict[str, dict]:
    """Infer part records for an info.json written before parts were tracked."""
    parts: dict[str, dict] = {}
    if res.get("status") not in ("ok", "partial"):
        return parts

    def rec(status: str, **extra: Any) -> dict:
        return {"status": status, "at": res.get("fetched_at"), "params": None, **extra}

    if res.get("metadata") or res.get("post"):
        parts["metadata"] = rec("ok")
    if res.get("thumbnail_file"):
        parts["thumbnail"] = rec("ok")
    if res.get("return_youtube_dislike"):
        parts["dislikes"] = rec("ok")
    comments = res.get("comments") or {}
    if "error" not in comments:
        if "top" in comments:  # options inferred from what was saved
            top = comments["top"]
            parts["top-comments"] = rec("ok")
            parts["top-comments"]["params"] = {
                "n": len(top), "replies": max((len(c.get("replies") or []) for c in top), default=0),
            }
        threads = comments.get("user_threads")
        if threads is not None and not any(t.get("status") == "error" for t in threads):
            parts["my-comments"] = rec("ok", ids=sorted(t.get("comment_id", "") for t in threads))
    subs = res.get("subtitles")
    if isinstance(subs, dict) and "error" not in subs:
        parts["subtitles"] = rec("ok" if subs.get("tracks") else "none")
    lyr = res.get("lyrics")
    if isinstance(lyr, dict) and "error" not in lyr:
        parts["lyrics"] = rec("ok" if lyr.get("file") else "none")
    if res.get("post") is not None and "image_files" in res and all(res["image_files"]):
        parts["images"] = rec("ok")
    for k in ("banner", "avatar"):
        if res.get(k + "_file"):
            parts[k] = rec("ok")
        elif res.get("metadata") is not None and (k + "_url") in res and not res.get(k + "_url"):
            parts[k] = rec("none")
    return parts


BOT_CHECKS_BEFORE_STOP = 5
_MUSIC_TITLE_RE = re.compile(
    r"official (music )?video|official audio|\baudio\b|lyrics?|\bft\.|\bfeat\.|music video|\bmv\b|клип|караоке",
    re.I,
)
_FINAL_THREAD_STATUSES = ("ok", "not_found_online", "reply_not_found_online", "no_comment_id_in_takeout")


def write_atomic(path: Path, data: bytes | str) -> None:
    tmp = path.with_name(path.name + ".part")
    if isinstance(data, str):
        tmp.write_text(data, "utf-8")
    else:
        tmp.write_bytes(data)
    tmp.replace(path)


def srt_to_text(srt: str) -> str:
    """Plain transcript from SRT: drops cue numbers/timings/tags, merges the
    repeated lines that auto-generated captions produce."""
    out: list[str] = []
    for block in re.split(r"\r?\n\s*\r?\n", srt.strip()):
        for line in block.splitlines():
            line = line.strip()
            if not line or line.isdigit() or "-->" in line:
                continue
            line = re.sub(r"<[^>]+>", "", line).strip()
            if line and (not out or out[-1] != line):
                out.append(line)
    return "\n".join(out) + ("\n" if out else "")


@dataclass
class Plan:
    """What has to be done for one item in this run."""

    item: Item
    prev: dict | None  # saved info.json (None = first time / --refresh)
    todo: list[str]  # parts to fetch


class Enricher:
    def __init__(self, opt: Options, limiters: dict[str, AdaptiveLimiter]):
        self.opt = opt
        self.limiters = limiters
        cookiejar: http.cookiejar.MozillaCookieJar | None = None
        if opt.cookies_file:
            cookiejar = http.cookiejar.MozillaCookieJar(opt.cookies_file)
            cookiejar.load(ignore_discard=True, ignore_expires=True)
        self.http = Http(limiters, opt.hl, cookiejar)
        self.yt = InnerTube(self.http, opt.hl, opt.gl)  # stateless, shared by all threads
        self._local = threading.local()  # YoutubeDL objects are not thread-safe
        self._bot_checks = 0  # consecutive "confirm you're not a bot" answers
        self._bot_lock = threading.Lock()

    def width(self, cls: str = "api") -> int:
        return self.limiters[cls].width()

    # ---------------- yt-dlp ----------------

    def ydl(self, flat: bool = False) -> Any:
        key = "ydl_flat" if flat else "ydl"
        if not hasattr(self._local, key):
            import yt_dlp

            params: dict[str, Any] = {
                "quiet": True,
                "no_warnings": True,
                "skip_download": True,
                "ignore_no_formats_error": True,
                "noprogress": True,
                # metadata is always fetched in English: yt-dlp parses counts
                # from English labels ('19M likes'); --lang only affects comments
                "logger": _YdlLogger(),
            }
            if flat:
                params.update({"extract_flat": True, "playlist_items": "0"})
            if self.opt.cookies_file:
                params["cookiefile"] = self.opt.cookies_file
            setattr(self._local, key, yt_dlp.YoutubeDL(params))
        return getattr(self._local, key)

    def extract(self, url: str, flat: bool = False) -> tuple[dict | None, str | None]:
        """yt-dlp extraction through the 'ytdlp' limiter -> (info, error)."""
        ydl = self.ydl(flat)
        logger: _YdlLogger = ydl.params["logger"]
        logger.messages.clear()
        info: dict | None = None
        error: str | None = None
        with self.limiters["ytdlp"].slot() as t:
            try:
                info = ydl.extract_info(url, download=False, process=False)
            except Exception as e:  # yt_dlp.utils.DownloadError
                error = _clean_ydl_msg(str(e))
            if info is not None and not (info.get("title") or info.get("channel")):
                # playability errors (removed/private/terminated) are downgraded to
                # warnings by ignore_no_formats_error -> recover the reason
                info = None
            if info is None and (not error or error == "video unavailable"):
                reasons = [m for m in logger.messages if "429" not in m and "JavaScript" not in m]
                error = _clean_ydl_msg(reasons[-1]) if reasons else "unavailable"
            if info is None and (BOT_CHECK_RE.search(error or "") or "HTTP Error 429" in (error or "")):
                t.throttle()
        return info, error

    def _note_bot_check(self, hit: bool) -> None:
        with self._bot_lock:
            self._bot_checks = self._bot_checks + 1 if hit else 0
            if self._bot_checks >= BOT_CHECKS_BEFORE_STOP:
                raise RateLimited("YouTube keeps asking to confirm you're not a bot")

    # ---------------- planning: which parts still have to be done ----------------

    def item_dir(self, item: Item) -> Path:
        return self.opt.out / f"{item.kind}s" / item.id

    def load(self, item: Item) -> dict | None:
        try:
            return json.loads((self.item_dir(item) / "info.json").read_text("utf-8"))
        except (OSError, ValueError):
            return None

    def part_done(self, item: Item, part: str, rec: dict | None) -> bool:
        if not rec or rec.get("status") not in PART_DONE:
            return False
        if not _covers(part, rec.get("params"), self.opt.part_params(part)):
            return False
        if part == "my-comments":  # a newer Takeout may contain comments not fetched yet
            wanted = {uc.comment_id for uc in item.user_comments}
            return wanted <= set(rec.get("ids") or [])
        return True

    def plan(self, item: Item) -> Plan:
        selected = self.opt.selected(item.kind)
        prev = None if self.opt.refresh else self.load(item)
        if not selected:
            return Plan(item, prev, [])
        if prev is None:
            return Plan(item, None, selected)
        status = prev.get("status")
        if status == "unavailable":
            retry = self.opt.retry_unavailable or (self.opt.retry_unavailable_videos and item.kind == "video")
            return Plan(item, prev, selected if retry else [])
        if status not in ("ok", "partial"):  # error: base data never fetched
            return Plan(item, prev, selected)
        parts = prev.get("parts") or legacy_parts(prev)
        return Plan(item, prev, [p for p in selected if not self.part_done(item, p, parts.get(p))])

    def missing_parts(self, item: Item, res: dict) -> list[str]:
        if res.get("status") == "unavailable":
            return []
        parts = res.get("parts") or legacy_parts(res)
        return [p for p in self.opt.selected(item.kind) if not self.part_done(item, p, parts.get(p))]

    # ---------------- processing ----------------

    def process(self, plan: Plan) -> tuple[dict, dict[str, str]]:
        """Fetch the planned parts, merge them into the saved info.json.
        Returns (result, {part: status} for the parts handled in this run)."""
        item = plan.item
        d = self.item_dir(item)
        d.mkdir(parents=True, exist_ok=True)
        res: dict[str, Any] = dict(plan.prev or {})
        res.update({"kind": item.kind, "id": item.id, "fetched_at": now_iso(), "tool_version": __version__})
        parts: dict[str, dict] = dict(res.get("parts") or (legacy_parts(res) if plan.prev else {}))
        before = {k: dict(v) for k, v in parts.items()}
        handler = {"video": self._video, "post": self._post, "channel": self._channel}[item.kind]
        try:
            handler(item, d, res, parts, plan.todo)
        except RateLimited:
            raise
        except Exception as e:
            log.debug("failure on %s %s", item.kind, item.id, exc_info=True)
            res["status"], res["error"] = "error", f"{type(e).__name__}: {e}"
        res["parts"] = parts
        if res.get("status") not in ("unavailable", "error"):
            failed = [p for p in self.opt.selected(item.kind) if (parts.get(p) or {}).get("status") == "error"]
            res["status"] = "partial" if failed else "ok"
            if not failed:
                res.pop("error", None)
        res["takeout"] = item.to_json()
        write_atomic(d / "info.json", json.dumps(res, ensure_ascii=False, indent=2))
        changed = {p: parts[p]["status"] for p in plan.todo if p in parts and parts[p] != before.get(p)}
        return res, changed

    def _record(self, parts: dict, part: str, status: str, error: Exception | str | None = None, **extra: Any) -> None:
        rec: dict[str, Any] = {"status": status, "at": now_iso(), "params": self.opt.part_params(part), **extra}
        if error is not None:
            rec["error"] = error if isinstance(error, str) else f"{type(error).__name__}: {error}"
        parts[part] = rec

    def _run_parts(self, tasks: dict[str, Callable[[], Any]], parts: dict,
                   apply: Callable[[str, Any], tuple[str, dict]]) -> None:
        """Run part fetchers in parallel; `apply(part, value)` stores the value and
        returns (status, extra record fields). Exceptions -> status 'error'."""
        for part, (value, err) in run_tasks(tasks, width=max(1, len(tasks))).items():
            if err is not None:
                self._record(parts, part, "error", err)
                continue
            status, extra = apply(part, value)
            self._record(parts, part, status, **extra)

    # ---------------- downloads ----------------

    _EXT = {"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp", "image/gif": ".gif", "image/avif": ".avif"}

    def download(self, url: str | None, dest_stem: Path) -> str | None:
        """Download to dest_stem.<ext from content-type>; existing complete files are
        reused. None if the server has no such file; network errors raise."""
        if not url:
            return None
        if not self.opt.refresh:
            for existing in dest_stem.parent.glob(dest_stem.name + ".*"):
                if existing.suffix != ".part":
                    return existing.name
        r = self.http.request("media", "GET", url, timeout=60)
        if r.status_code >= 500 or r.status_code == 429:
            raise RuntimeError(f"HTTP {r.status_code} for {url}")
        if r.status_code != 200 or not r.content:
            return None
        ctype = r.headers.get("content-type", "").split(";")[0].strip()
        ext = self._EXT.get(ctype) or Path(urllib.parse.urlparse(url).path).suffix or ".bin"
        path = dest_stem.with_suffix(ext)
        write_atomic(path, r.content)
        return path.name

    # ---------------- comments (shared by videos and posts) ----------------

    def _top_comments(self, endpoint: str, token_fn: Callable[..., str]) -> dict:
        out: dict[str, Any] = {"sort": "top", "top": []}
        if self.opt.top_comments <= 0:
            return out
        threads: list[dict] = []
        for thread, page in self.yt.iter_threads(endpoint, token_fn(SORT_TOP)):
            out.setdefault("total_count_text", page.get("count_text"))
            threads.append(thread)
            if len(threads) >= self.opt.top_comments:
                break
        top = [dict(t["comment"]) for t in threads]
        if self.opt.top_comment_replies:
            need = [i for i, c in enumerate(top) if c.get("reply_count")]

            def fetch(i: int) -> list[dict]:
                return self.yt.replies(endpoint, threads[i], self.opt.top_comment_replies)

            for i, replies in zip(need, parallel_map(fetch, need, self.width())):
                top[i]["replies"] = replies
        out["top"] = top
        return out

    def _my_comments(self, endpoint: str, token_fn: Callable[..., str], item: Item, prev_threads: list[dict],
                     reuse: bool) -> list[dict]:
        """Your comment threads; threads already resolved earlier (with options that
        still apply) are reused, only new or failed ones are fetched."""
        known = {t.get("comment_id"): t for t in prev_threads if reuse and t.get("status") in _FINAL_THREAD_STATUSES}
        todo = [uc for uc in item.user_comments if uc.comment_id not in known]
        fetched = {t["comment_id"]: t for t in parallel_map(lambda uc: self._user_thread(endpoint, token_fn, uc), todo, self.width())}
        return [fetched.get(uc.comment_id) or known[uc.comment_id] for uc in item.user_comments]

    def _apply_comments(self, part: str, value: Any, res: dict) -> tuple[str, dict]:
        comments = res.get("comments") if isinstance(res.get("comments"), dict) else {}
        comments.pop("error", None)
        if part == "top-comments":
            comments.update(value)
            res["comments"] = comments
            return "ok", {}
        comments["user_threads"] = value
        res["comments"] = comments
        status = "error" if any(t.get("status") == "error" for t in value) else ("ok" if value else "none")
        done_ids = sorted(t["comment_id"] for t in value if t.get("status") in _FINAL_THREAD_STATUSES)
        return status, {"ids": done_ids}

    def _comment_tasks(self, todo: list[str], endpoint: str, token_fn: Callable[..., str], item: Item, res: dict) -> dict:
        tasks: dict[str, Callable[[], Any]] = {}
        if "top-comments" in todo:
            tasks["top-comments"] = lambda: self._top_comments(endpoint, token_fn)
        if "my-comments" in todo:
            prev_threads = ((res.get("comments") or {}).get("user_threads")) or []
            rec = (res.get("parts") or {}).get("my-comments") or {}
            reuse = _covers("my-comments", rec.get("params"), self.opt.part_params("my-comments")) if rec else True
            tasks["my-comments"] = lambda: self._my_comments(endpoint, token_fn, item, prev_threads, reuse)
        return tasks

    def _user_thread(self, endpoint: str, token_fn: Callable[..., str], uc: UserComment) -> dict:
        res: dict[str, Any] = {"comment_id": uc.comment_id, "is_reply": uc.is_reply, "takeout_text": uc.text, "created": uc.created}
        try:
            self._find_user_thread(endpoint, token_fn, uc, res)
        except RateLimited:
            raise
        except Exception as e:  # one broken thread must not lose the others
            res["status"] = "error"
            res["error"] = f"{type(e).__name__}: {e}"
        return res

    def _find_user_thread(self, endpoint: str, token_fn: Callable[..., str], uc: UserComment, res: dict) -> None:
        if not uc.comment_id:
            res["status"] = "no_comment_id_in_takeout"
            return
        top_id = uc.parent_id or uc.comment_id
        first_thread = first(t for t, _ in self.yt.iter_threads(endpoint, token_fn(SORT_TOP, uc.comment_id), max_pages=1))
        if first_thread is None or first_thread["comment"]["id"] != top_id:
            res["status"] = "not_found_online"  # deleted, held for review, or hidden
            return
        thread = first_thread
        if not uc.is_reply:
            res["status"] = "ok"
            res["comment"] = thread["comment"]
            res["replies"] = (
                self.yt.replies(endpoint, thread, self.opt.max_replies)
                if thread["comment"]["reply_count"] and self.opt.max_replies != 0 else []
            )
            return
        # it's a reply: parent = the comment it replied to
        res["replying_to"] = thread["comment"]
        own = next((c for c in thread["teasers"] if c["id"] == uc.comment_id), None)
        siblings: list[dict] = []
        if self.opt.max_replies != 0:
            siblings = self.yt.replies(endpoint, thread, self.opt.max_replies)
            own = own or next((c for c in siblings if c["id"] == uc.comment_id), None)
        if own is None:
            res["status"] = "reply_not_found_online"
            return
        res["status"] = "ok"
        res["comment"] = own
        # YouTube threads are flat: "replies to a reply" are later replies in the
        # same thread that @mention you.
        handle = (own["author"].get("name") or "").lstrip("@")
        ids = [c["id"] for c in siblings]
        after = siblings[ids.index(own["id"]) + 1:] if own["id"] in ids else []
        res["replies"] = [c for c in after if handle and re.search(r"@" + re.escape(handle) + r"\b", c["text"], re.I)]

    # ---------------- video ----------------

    VIDEO_FIELDS = (
        "id title description channel channel_id channel_url uploader uploader_id uploader_url "
        "channel_follower_count channel_is_verified upload_date timestamp release_timestamp release_date "
        "duration duration_string view_count concurrent_view_count like_count comment_count age_limit "
        "categories tags chapters live_status was_live availability playable_in_embed language "
        "media_type webpage_url thumbnail track artist artists album album_artist alt_title creators release_year"
    ).split()

    def _video(self, item: Item, d: Path, res: dict, parts: dict, todo: list[str]) -> None:
        vid = item.id
        res["url"] = f"https://www.youtube.com/watch?v={vid}"
        had_data = bool(res.get("metadata", {}).get("title")) and res.get("status") in ("ok", "partial")
        info: dict | None = None
        # subtitle URLs expire, so subtitles always need a fresh extraction
        if "metadata" in todo or "subtitles" in todo or not had_data:
            info, error = self.extract(res["url"])
            self._note_bot_check(info is None and bool(BOT_CHECK_RE.search(error or "")))
            if info is None:
                failure = classify_failure(error)
                if failure == "error" and had_data:
                    # temporary: keep the saved data, retry the parts that needed it
                    for p in ("metadata", "subtitles"):
                        if p in todo:
                            self._record(parts, p, "error", error)
                    todo = [p for p in todo if p not in ("metadata", "subtitles")]
                else:
                    res["status"], res["error"] = failure, error or "video unavailable"
                    if failure == "error":
                        return  # nothing saved yet: retry everything next run
                    # unavailable: old data (if any) is kept; RYD/thumbnails may still exist
                    todo = [p for p in todo if p in ("thumbnail", "dislikes")]
            else:
                meta = {k: info.get(k) for k in self.VIDEO_FIELDS if info.get(k) is not None}
                if meta.get("media_type") is None and "/shorts/" in (info.get("webpage_url") or ""):
                    meta["media_type"] = "short"
                old = res.get("metadata") or {}
                if "dislike_count_estimate" in old and "dislikes" not in todo:
                    meta["dislike_count_estimate"] = old["dislike_count_estimate"]
                res["metadata"] = meta
                res["status"] = "ok"
                res.pop("error", None)
                self._record(parts, "metadata", "ok")
        meta = res.get("metadata") or {}
        exists = bool(meta.get("title")) and res.get("status") != "unavailable"

        token_fn = lambda sort, hl=None: video_comments_token(vid, sort, hl)  # noqa: E731
        tasks: dict[str, Callable[[], Any]] = {}
        if "dislikes" in todo:
            tasks["dislikes"] = lambda: self._ryd(vid)
        if "thumbnail" in todo:
            tasks["thumbnail"] = lambda: self._thumbnail(vid, meta, d)
        if exists:
            tasks.update(self._comment_tasks(todo, "next", token_fn, item, res))
            if "subtitles" in todo and info is not None:
                tasks["subtitles"] = lambda: self._subtitles(info, d)
            if "lyrics" in todo:
                tasks["lyrics"] = lambda: self._lyrics(vid, d) if self._wants_lyrics(meta, item) else None

        def apply(part: str, value: Any) -> tuple[str, dict]:
            if part == "dislikes":
                if not value or not (exists or value.get("dislikes") or value.get("likes")):
                    return "none", {}
                res.setdefault("metadata", {})["dislike_count_estimate"] = value.get("dislikes")
                res["return_youtube_dislike"] = value
                return "ok", {}
            if part == "thumbnail":
                if not value:
                    return "none", {}
                res["thumbnail_file"], res["thumbnail_url"] = value
                return "ok", {}
            if part == "subtitles":
                res["subtitles"] = value
                if any(str(t.get("error", "")).startswith("HTTP") for t in value["tracks"]):
                    return "error", {"error": "some subtitle downloads failed"}
                return ("ok" if any(t.get("file") for t in value["tracks"]) else "none"), {}
            if part == "lyrics":
                if value is None:
                    return "skipped", {}  # not a music video (--lyrics auto)
                res["lyrics"] = value
                return ("ok" if value.get("file") else "none"), {}
            return self._apply_comments(part, value, res)

        self._run_parts(tasks, parts, apply)

    def _thumbnail(self, vid: str, meta: dict, d: Path) -> tuple[str, str] | None:
        cands = [meta.get("thumbnail")] if meta.get("thumbnail") else []
        cands += [f"https://i.ytimg.com/vi/{vid}/maxresdefault.jpg", f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"]
        for u in cands:
            name = self.download(u, d / "thumbnail")
            if name:
                return name, u
        return None

    def _ryd(self, vid: str) -> dict | None:
        r = self.http.request("ryd", "GET", "https://returnyoutubedislikeapi.com/votes", params={"videoId": vid}, timeout=20)
        if r.status_code >= 500 or r.status_code == 429:
            raise RuntimeError(f"Return YouTube Dislike: HTTP {r.status_code}")
        if r.status_code != 200:
            return None
        j = r.json()
        return {k: j.get(k) for k in ("likes", "dislikes", "rating", "viewCount", "deleted", "dateCreated")}

    # ---------------- subtitles ----------------

    def _lang_wanted(self, lang: str) -> bool:
        if not self.opt.sub_langs:
            return True
        low = lang.lower()
        return any(low == w or low.startswith(w + "-") for w in self.opt.sub_langs)

    def _subtitles(self, info: dict, d: Path) -> dict:
        """Creator subtitles (all or --sub-langs) and, with --subs auto, the
        auto-generated captions in the video's original language. Saved as
        subtitles/<lang>[.auto].srt plus a plain-text transcript .txt."""
        manual = info.get("subtitles") or {}
        auto = info.get("automatic_captions") or {}
        tracks: list[tuple[str, bool, list[dict]]] = [
            (lang, False, fmts) for lang, fmts in manual.items() if lang != "live_chat" and self._lang_wanted(lang)
        ]
        if self.opt.subs == "auto":
            orig = next((k for k in auto if k.endswith("-orig")), None)
            if orig:
                lang = orig[: -len("-orig")]
                if lang not in manual and self._lang_wanted(lang):
                    tracks.append((lang, True, auto[orig]))
        out: dict[str, Any] = {
            "available_manual": sorted(k for k in manual if k != "live_chat"),
            "auto_original_language": next((k[:-5] for k in auto if k.endswith("-orig")), None),
            "tracks": [],
        }
        if not tracks:
            return out
        sub_dir = d / "subtitles"
        sub_dir.mkdir(exist_ok=True)

        def fetch(track: tuple[str, bool, list[dict]]) -> dict:
            lang, is_auto, fmts = track
            fmt = next((f for f in fmts if f.get("ext") == "srt"), None) or next((f for f in fmts if f.get("ext") == "vtt"), None)
            entry: dict[str, Any] = {"lang": lang, "name": (fmt or {}).get("name"), "auto_generated": is_auto}
            if not fmt or not fmt.get("url"):
                entry["error"] = "no srt/vtt format offered"
                return entry
            stem = f"{lang}.auto" if is_auto else lang
            path = sub_dir / f"{stem}.{fmt['ext']}"
            if not path.exists() or self.opt.refresh:
                r = self.http.request("api", "GET", fmt["url"], youtube=True, timeout=45)
                if r.status_code != 200 or not r.text.strip():
                    entry["error"] = f"HTTP {r.status_code}"
                    return entry
                write_atomic(path, r.text)
            text = srt_to_text(path.read_text("utf-8"))
            write_atomic(sub_dir / f"{stem}.txt", text)
            entry.update({"file": f"subtitles/{path.name}", "text_file": f"subtitles/{stem}.txt", "chars": len(text)})
            return entry

        out["tracks"] = parallel_map(fetch, tracks, self.width())
        return out

    # ---------------- lyrics ----------------

    def _wants_lyrics(self, meta: dict, item: Item) -> bool:
        if self.opt.lyrics == "all":
            return True
        cats = [c.lower() for c in meta.get("categories") or []]
        channel = meta.get("channel") or meta.get("uploader") or ""
        return bool(
            "music" in cats
            or meta.get("track") or meta.get("artist") or meta.get("artists")
            or channel.endswith(" - Topic") or "vevo" in channel.lower()
            or _MUSIC_TITLE_RE.search(meta.get("title") or "")
            or any("music" in s.lower() for s in item.sources)
        )

    def _lyrics(self, vid: str, d: Path) -> dict:
        res = self.yt.lyrics(vid)
        if res.get("status") == "ok" and res.get("text"):
            write_atomic(d / "lyrics.txt", res["text"].rstrip() + "\n")
            res["file"] = "lyrics.txt"
        return res

    # ---------------- post ----------------

    def _download_post_image(self, i: int, u: str, d: Path) -> str | None:
        name = None
        if self.opt.original_images and "=" in u and ("ggpht.com" in u or "googleusercontent.com" in u):
            name = self.download(u.split("=")[0] + "=s0", d / f"image_{i}")
        return name or self.download(u, d / f"image_{i}")

    def _post(self, item: Item, d: Path, res: dict, parts: dict, todo: list[str]) -> None:
        res["url"] = f"https://www.youtube.com/post/{item.id}"
        if "metadata" in todo or not res.get("post") or res.get("status") not in ("ok", "partial"):
            got = self.yt.post(item.id)
            if got["status"] != "ok":
                # a previously saved copy of a since-deleted post is kept as is
                res["status"], res["error"] = got["status"], got.get("error")
                return
            res["post"] = got["post"]
            res["status"] = "ok"
            res.pop("error", None)
            self._record(parts, "metadata", "ok")
        post = res["post"]
        tasks: dict[str, Callable[[], Any]] = {}
        if "images" in todo:
            tasks["images"] = lambda: parallel_map(
                lambda iu: self._download_post_image(iu[0], iu[1], d), list(enumerate(post.get("images") or [], 1)), self.width("media")
            )
        ucid = (post.get("author") or {}).get("channel_id")
        if ucid:
            token_fn = lambda sort, hl=None: post_comments_token(item.id, ucid, sort, hl)  # noqa: E731
            tasks.update(self._comment_tasks(todo, "browse", token_fn, item, res))

        def apply(part: str, value: Any) -> tuple[str, dict]:
            if part == "images":
                res["image_files"] = value
                if not value:
                    return "none", {}
                return ("ok" if all(value) else "error"), ({} if all(value) else {"error": "some images missing"})
            return self._apply_comments(part, value, res)

        self._run_parts(tasks, parts, apply)

    # ---------------- channel ----------------

    CHANNEL_FIELDS = (
        "id channel channel_id title description channel_follower_count uploader_id uploader_url "
        "channel_url channel_is_verified tags availability"
    ).split()

    def _channel(self, item: Item, d: Path, res: dict, parts: dict, todo: list[str]) -> None:
        url = f"https://www.youtube.com/channel/{item.id}"
        res["url"] = url
        if "metadata" in todo or "banner_url" not in res or res.get("status") not in ("ok", "partial"):
            info, error = self.extract(url, flat=True)
            if info is None:
                res["status"], res["error"] = classify_failure(error), error or "channel unavailable"
                return
            res["metadata"] = {k: info.get(k) for k in self.CHANNEL_FIELDS if info.get(k) is not None}
            thumbs = {t.get("id"): t.get("url") for t in info.get("thumbnails") or []}
            res["banner_url"] = thumbs.get("banner_uncropped")
            res["avatar_url"] = thumbs.get("avatar_uncropped")
            res["status"] = "ok"
            res.pop("error", None)
            self._record(parts, "metadata", "ok")
        tasks = {k: (lambda k=k: self.download(res.get(k + "_url"), d / k)) for k in ("banner", "avatar") if k in todo}

        def apply(part: str, value: Any) -> tuple[str, dict]:
            res[part + "_file"] = value
            return ("ok" if value else "none"), {}

        self._run_parts(tasks, parts, apply)


class _YdlLogger:
    """Quiet logger that remembers warnings/errors of the current extraction."""

    def __init__(self) -> None:
        self.messages: list[str] = []

    def debug(self, msg: str) -> None:
        pass

    def info(self, msg: str) -> None:
        pass

    def warning(self, msg: str) -> None:
        self.messages.append(msg)
        log.debug("yt-dlp: %s", msg)

    def error(self, msg: str) -> None:
        self.messages.append(msg)
        log.debug("yt-dlp: %s", msg)


def _clean_ydl_msg(msg: str) -> str:
    msg = re.sub(r"^(ERROR|WARNING):\s*", "", msg.strip())
    msg = re.sub(r"^\[[\w:]+\]\s*(?:[\w-]+:\s*)?", "", msg)
    msg = re.split(r"\.?\s+Use --cookies|;?\s+please report this issue", msg)[0]  # drop yt-dlp's CLI advice
    msg = " ".join(msg.split())
    return msg.strip()


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------


def write_summary(out: Path, inv: Inventory, enr: Enricher) -> None:
    rows = []
    for kind in ("video", "post", "channel"):
        for id_, item in inv.items[kind].items():
            info: dict[str, Any] = {}
            try:
                info = json.loads((out / f"{kind}s" / id_ / "info.json").read_text("utf-8"))
            except (OSError, ValueError):
                pass
            meta = info.get("metadata") or {}
            post = info.get("post") or {}
            title = meta.get("title") or post.get("text", "")[:120] or item.title or ""
            ut = (info.get("comments") or {}).get("user_threads") or []
            tracks = (info.get("subtitles") or {}).get("tracks") or []
            rows.append(
                {
                    "kind": kind,
                    "id": id_,
                    "status": info.get("status", "pending"),
                    "title": title,
                    "channel": meta.get("channel") or (post.get("author") or {}).get("name") or item.channel_name or "",
                    "sources": ";".join(sorted(item.sources)),
                    "user_comments": len(item.user_comments),
                    "user_comments_found_online": sum(1 for t in ut if t.get("status") == "ok"),
                    "views": meta.get("view_count", ""),
                    "likes": meta.get("like_count", post.get("likes", "")),
                    "dislikes_estimate": meta.get("dislike_count_estimate", ""),
                    "subtitles": ";".join(t["lang"] + (".auto" if t.get("auto_generated") else "") for t in tracks if t.get("file")),
                    "lyrics": "yes" if (info.get("lyrics") or {}).get("file") else "",
                    "missing_parts": ";".join(enr.missing_parts(item, info)) if info else ";".join(enr.opt.selected(kind)),
                    "error": info.get("error", ""),
                }
            )
    fields = list(rows[0].keys()) if rows else ["kind", "id"]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fields)
    w.writeheader()
    w.writerows(rows)
    write_atomic(out / "summary.csv", buf.getvalue())


def _auto_int(value: str) -> int | None:
    if value.strip().lower() == "auto":
        return None
    n = int(value)
    if n < 1:
        raise argparse.ArgumentTypeError("must be >= 1 or 'auto'")
    return n


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Enrich a YouTube Google Takeout export with video/post/channel details, thumbnails, comments, subtitles and lyrics.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("takeout", nargs="+", type=Path, help="Takeout .zip/.tgz archive(s) and/or extracted folder(s)")
    ap.add_argument("-o", "--out", type=Path, default=Path("takeout_enriched"), help="output directory")
    ap.add_argument("-n", "--top-comments", type=int, default=20, help="top comments to fetch per video/post (0 = none)")
    ap.add_argument("--top-comment-replies", type=int, default=0, help="replies to fetch for each top comment")
    ap.add_argument("--max-replies", type=int, default=100, help="max replies fetched for each of YOUR comments (-1 = all, 0 = none)")
    ap.add_argument("--kinds", default="video,post,channel", help="which item kinds to enrich")
    ap.add_argument("--only-commented", action="store_true", help="only videos/posts you commented on (channels unaffected)")
    ap.add_argument("--exclude-files", action="append", default=[], metavar="SUBSTR",
                    help="ignore Takeout files whose path contains SUBSTR, e.g. 'watch-history' (repeatable)")
    ap.add_argument("--limit", type=int, default=0, help="process at most N items per kind (0 = no limit)")
    g = ap.add_argument_group("speed (adaptive by default)")
    g.add_argument("--workers", type=_auto_int, default="auto", metavar="N|auto",
                   help="items processed in parallel; 'auto' tunes it from measured throughput")
    g.add_argument("--max-workers", type=int, default=12, help="upper bound for --workers auto")
    g.add_argument("--concurrency", "--parallel-requests", type=_auto_int, default="auto", metavar="N|auto",
                   help="concurrent YouTube API requests; 'auto' adapts to latency and throttling")
    g.add_argument("--sleep", type=float, default=0.0,
                   help="minimum seconds between the starts of two YouTube requests (API and yt-dlp), on top of the adaptive limits")
    g.add_argument("--no-network-probe", action="store_true", help="skip the startup latency/bandwidth measurement")
    g.add_argument("--reset-stats", action="store_true", help="ignore what previous runs learned (OUT/.adaptive_state.json)")
    m = ap.add_argument_group("content (parts)")
    m.add_argument("--include", type=parse_parts, default="all", metavar="PARTS",
                   help="only these parts, comma-separated: " + ", ".join(ALL_PARTS)
                   + "; groups: comments, media, all. Singular/plural both work")
    m.add_argument("--exclude", type=parse_parts, default="", metavar="PARTS",
                   help="leave out these parts, e.g. 'banner,thumbnail'")
    m.add_argument("--subs", choices=("manual", "auto"), default="auto",
                   help="subtitles: creator-made only, or creator-made + auto-generated in the original language")
    m.add_argument("--sub-langs", default="", metavar="LANGS",
                   help="comma-separated subtitle languages to keep, e.g. 'en,ru' (prefix match: en = en-US, en-GB); empty = all")
    m.add_argument("--lyrics", choices=("auto", "all"), default="auto",
                   help="song lyrics from YouTube Music: auto = only for music-like videos, all = try every video")
    m.add_argument("--resized-images", action="store_true", help="save post images as served (webp) instead of originals")
    ap.add_argument("--cookies", help="Netscape cookies.txt for youtube.com (age-restricted/members content, fewer rate limits)")
    ap.add_argument("--lang", default="en", help="interface language for relative dates/labels")
    ap.add_argument("--region", default="US", help="content region")
    ap.add_argument("--refresh", action="store_true", help="re-fetch items that were already saved")
    ap.add_argument("--retry-unavailable", "--refresh-unavailable", action="store_true",
                    help="re-parse all items previously marked as unavailable (videos, posts, channels)")
    ap.add_argument("--retry-unavailable-videos", action="store_true",
                    help="re-parse only videos previously marked as unavailable (e.g. to retry with cookies)")
    ap.add_argument("--dry-run", action="store_true", help="only parse Takeout and write inventory.json")
    ap.add_argument("-v", "--verbose", action="count", default=0, help="-v: reasons, item details and debug logs; -vv: also HTTP library logs")
    args = ap.parse_args(argv)
    missing = [str(p) for p in args.takeout if not p.expanduser().exists()]
    if missing:
        ap.error("input not found: " + ", ".join(missing))

    logging.basicConfig(
        level=logging.DEBUG if args.verbose > 0 else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    if args.verbose < 2:
        logging.getLogger("urllib3").setLevel(logging.WARNING)

    out: Path = args.out
    out.mkdir(parents=True, exist_ok=True)

    log.info("Reading Takeout ...")
    inv = Inventory()
    TakeoutParser(inv, args.exclude_files).run(args.takeout)
    write_atomic(
        out / "inventory.json",
        json.dumps(
            {
                "generated_at": now_iso(),
                "files": inv.files,
                "items": {k: {i: it.to_json() for i, it in v.items()} for k, v in inv.items.items()},
            },
            ensure_ascii=False,
            indent=1,
        ),
    )
    counts = {k: len(v) for k, v in inv.items.items()}
    n_comments = sum(len(i.user_comments) for k in ("video", "post") for i in inv.items[k].values())
    log.info("Found %d videos, %d posts, %d channels, %d of your comments", counts["video"], counts["post"], counts["channel"], n_comments)
    if not any(counts.values()):
        log.error("Nothing found. Is this a YouTube Takeout? Check %s", out / "inventory.json")
        return 1
    if args.dry_run:
        return 0

    kinds = [k.strip().rstrip("s") for k in args.kinds.split(",") if k.strip()]
    todo: list[Item] = []
    for kind in ("channel", "post", "video"):
        if kind not in kinds:
            continue
        items = list(inv.items[kind].values())
        if args.only_commented and kind != "channel":
            items = [i for i in items if i.user_comments]
        # commented items first - they are the most valuable
        items.sort(key=lambda i: (not i.user_comments, i.id))
        if args.limit:
            items = items[: args.limit]
        todo += items

    if any(i.kind in ("video", "channel") for i in todo):
        if not _installed("yt_dlp"):
            log.error("yt-dlp is required for video/channel metadata: pip install -U yt-dlp")
            return 1

    selected_parts = args.include - args.exclude
    if not selected_parts:
        log.error("--include/--exclude leave no parts to fetch")
        return 1
    opt = Options(
        out=out,
        parts=frozenset(selected_parts),
        top_comments=args.top_comments,
        top_comment_replies=args.top_comment_replies,
        max_replies=args.max_replies,
        original_images=not args.resized_images,
        subs=args.subs,
        sub_langs=tuple(x.strip().lower() for x in args.sub_langs.split(",") if x.strip()),
        lyrics=args.lyrics,
        refresh=args.refresh,
        retry_unavailable=args.retry_unavailable,
        retry_unavailable_videos=args.retry_unavailable_videos,
        hl=args.lang,
        gl=args.region,
        cookies_file=args.cookies,
    )

    # ---- adaptive concurrency: learned state + network probe ----
    state = AdaptiveState(out / AdaptiveState.FILE)
    if args.reset_stats:
        state.reset()
    network = None if args.no_network_probe else probe_network(Http._session())
    limiters = build_limiters(state, network, args.concurrency, args.sleep)
    enr = Enricher(opt, limiters)

    plans = [enr.plan(i) for i in todo]
    pending = [pl for pl in plans if pl.todo]
    retrying = sum(1 for pl in pending if (pl.prev or {}).get("status") == "unavailable")
    if retrying:
        log.info("Retrying %d items previously marked as unavailable.", retrying)
    part_todo: dict[str, int] = {}
    for pl in pending:
        for p in pl.todo:
            key = f"{pl.item.kind} metadata" if p == "metadata" else p
            part_todo[key] = part_todo.get(key, 0) + 1
    log.info("Parts: %s", ", ".join(sorted(selected_parts, key=ALL_PARTS.index)))
    if part_todo:
        log.info("Missing: %s", ", ".join(f"{k} x{v}" for k, v in part_todo.items()))

    history = state.worker_history()
    if args.workers:
        initial_workers = args.workers
    else:
        best = max(history.items(), key=lambda kv: kv[1])[0] if history else None
        initial_workers = best or 2
    governor = WorkerGovernor(initial_workers, args.workers or args.max_workers, limiters, history, fixed=args.workers is not None)
    governor.remaining = len(pending)

    if network and network.get("rtt_ms"):
        log.info("Network: %s ms to YouTube, ~%s Mbit/s", network["rtt_ms"], network.get("bandwidth_mbps") or "?")
    log.info(
        "%d items need work (%d complete). Starting with %d workers (%s); request limits: %s%s",
        len(pending),
        len(todo) - len(pending),
        governor.workers,
        "fixed" if args.workers else ("learned" if history else "adaptive"),
        ", ".join(f"{k} {v.limit:.0f}{' fixed' if v.fixed else ''}" for k, v in limiters.items()),
        " (from previous runs)" if state.data["limiters"] else "",
    )

    stats: dict[str, int] = {}
    part_stats: dict[str, dict[str, int]] = {}
    unavailable_reasons: dict[str, int] = {}
    stats_lock = threading.Lock()
    stop = threading.Event()
    started_at, t0 = now_iso(), time.monotonic()
    last_save = [time.monotonic()]

    def report(item: Item, r: dict, changed: dict[str, str]) -> None:
        st = r.get("status", "?")
        clean_err = " ".join(str(r.get("error") or "").split())
        with stats_lock:
            stats[st] = stats.get(st, 0) + 1
            if st == "unavailable" and clean_err:
                unavailable_reasons[clean_err] = unavailable_reasons.get(clean_err, 0) + 1
            for p, pst in changed.items():
                part_stats.setdefault(p, {})[pst] = part_stats.setdefault(p, {}).get(pst, 0) + 1
        title = (r.get("metadata") or {}).get("title") or ((r.get("post") or {}).get("text") or "")[:60] or item.title or ""
        extra = ""
        ut = (r.get("comments") or {}).get("user_threads")
        if ut:
            extra += f" | your comments found: {sum(1 for t in ut if t.get('status') == 'ok')}/{len(ut)}"
        subs = [t for t in (r.get("subtitles") or {}).get("tracks") or [] if t.get("file")]
        if subs:
            extra += f" | subs: {len(subs)}"
        if (r.get("lyrics") or {}).get("file"):
            extra += " | lyrics"
        if changed:
            extra += " | parts: " + " ".join(f"{p}:{pst}" for p, pst in changed.items())
        if st == "partial":
            failed = [p for p, rec in (r.get("parts") or {}).items() if rec.get("status") == "error" and p in opt.parts]
            clean_err = clean_err or "; ".join(f"{p}: {r['parts'][p].get('error', 'failed')}" for p in failed)
        reason = f" | reason: {clean_err}" if clean_err and (st in ("unavailable", "error", "partial") or args.verbose) else ""
        log.info("[%s] %-7s %s %s%s%s", st, item.kind, item.id, title[:60].replace("\n", " "), extra, reason)
        if args.verbose and st in ("ok", "partial"):
            meta = r.get("metadata") or {}
            details = []
            if meta.get("duration_string"):
                details.append(meta["duration_string"])
            for key, label in (("view_count", "views"), ("like_count", "likes")):
                if isinstance(meta.get(key), int):
                    details.append(f"{meta[key]:,} {label}")
            if isinstance(meta.get("dislike_count_estimate"), int):
                details.append(f"~{meta['dislike_count_estimate']:,} dislikes")
            comm_cnt = len((r.get("comments") or {}).get("top", []))
            if comm_cnt:
                details.append(f"{comm_cnt} top comments")
            if details:
                log.debug("       ↳ %s", ", ".join(details))

    def work(plan: Plan) -> None:
        item = plan.item
        with governor.gate.enter():
            if stop.is_set():
                return
            try:
                r, changed = enr.process(plan)
            except RateLimited as e:
                if not stop.is_set():
                    log.error("Rate limited by YouTube (%s). Stopping; re-run later to resume (or use --cookies).", e)
                stop.set()
                return
            except Exception:  # e.g. disk full while saving - keep the other workers going
                log.exception("Unexpected failure on %s %s", item.kind, item.id)
                return
            finally:
                governor.item_finished()
            report(item, r, changed)
            if time.monotonic() - last_save[0] > 60:  # keep learned stats even if the run gets killed
                last_save[0] = time.monotonic()
                state.save(limiters, governor, network)

    ex = cf.ThreadPoolExecutor(max_workers=governor.max_workers)
    try:
        futures = [ex.submit(work, pl) for pl in pending]
        for f in cf.as_completed(futures):
            f.result()
            if stop.is_set():
                break
    except KeyboardInterrupt:
        stop.set()
        log.warning("Interrupted - finishing items in progress; re-run to resume.")
    finally:
        ex.shutdown(wait=True, cancel_futures=True)

    governor.close()
    duration = time.monotonic() - t0
    run_info = {
        "started_at": started_at,
        "duration_s": round(duration, 1),
        "statuses": stats,
        "workers": governor.snapshot(),
        "limiters": {k: v.snapshot() for k, v in limiters.items()},
        "network": network,
        "stopped_early": stop.is_set(),
    }
    state.save(limiters, governor, network, run_info if governor.done else None)
    write_summary(out, inv, enr)
    if not pending:
        log.info("Done: all selected parts are complete, nothing to fetch. Output in %s", out.resolve())
    else:
        log.info("Done: %s. Output in %s", ", ".join(f"{k}={v}" for k, v in sorted(stats.items())) or "nothing processed", out.resolve())
    if part_stats:
        log.info("Parts fetched: %s", "; ".join(
            f"{p} " + ", ".join(f"{k}={v}" for k, v in sorted(c.items())) for p, c in sorted(part_stats.items(), key=lambda kv: ALL_PARTS.index(kv[0]))
        ))
    if governor.done:
        w = run_info["workers"]
        log.info(
            "Speed: %.1f items/min, %.1f workers on average (final %d); %s",
            w["items_per_min"], w["average"], w["final"],
            ", ".join(
                f"{k} limit {s['limit']:.1f}" + (f" ({s['throttles']} throttled)" if s["throttles"] else "")
                for k, s in run_info["limiters"].items() if s["requests"]
            ),
        )
    if unavailable_reasons:
        sorted_reasons = sorted(unavailable_reasons.items(), key=lambda x: -x[1])
        breakdown = "; ".join(f"{count}x '{reason}'" for reason, count in sorted_reasons[:5])
        if len(sorted_reasons) > 5:
            breakdown += f"; and {len(sorted_reasons) - 5} more"
        log.info("Unavailable breakdown: %s", breakdown)
    return 2 if stop.is_set() else 0


if __name__ == "__main__":
    sys.exit(main())
