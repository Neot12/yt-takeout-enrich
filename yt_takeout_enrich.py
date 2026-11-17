#!/usr/bin/env python3
"""
yt_takeout_enrich.py - enrich a YouTube Google Takeout export with live data.

Reads one or more Takeout archives (.zip/.tgz) or extracted folders, collects
every video, community post and subscribed channel referenced in them, and
fetches for each:

  video   : metadata (title, description, likes, estimated dislikes, views, tags,
            chapters, ...), thumbnail, top-N comments, and every comment *you*
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

Takeout parsing follows the formats documented by
github.com/purarue/google_takeout_parser and Google's Data Portability schema
(comments.csv with "Post ID"/"Parent Comment ID" columns, the old pre-2024
'takeoutSegments' comment text, legacy my-comments.html, watch-history
json/html, subscriptions.csv, playlist CSVs, own posts CSV).

Output layout (re-running resumes; finished items are skipped):

  OUT/inventory.json              everything extracted from Takeout
  OUT/summary.csv                 one row per enriched item
  OUT/videos/<id>/info.json       + thumbnail.<ext>
  OUT/posts/<id>/info.json        + image_<n>.<ext>
  OUT/channels/<id>/info.json     + banner.<ext>, avatar.<ext>
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures as cf
import csv
import hashlib
import http.cookiejar
import io
import json
import logging
import queue
import re
import sys
import tarfile
import threading
import time
import urllib.parse
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator

import requests

__version__ = "1.0.0"
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
            d["user_comments"] = [uc.__dict__ for uc in self.user_comments]
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


def iter_takeout_files(paths: list[Path]) -> Iterator[TakeoutFile]:
    """Yield files of Takeout archives and/or extracted folders. A folder may be
    the whole Takeout, the 'YouTube and YouTube Music' folder itself, any folder
    above them, or a folder holding takeout-*.zip / *.tgz parts."""
    seen: set[str] = set()
    for p in paths:
        p = p.expanduser()
        if p.is_dir():
            for f in sorted(p.rglob("*")):
                if not f.is_file():
                    continue
                if ARCHIVE_RE.search(f.name) and "takeout" in f.name.lower():
                    yield from iter_takeout_files([f])
                    continue
                rel = rel_takeout_path(str(f.relative_to(p)))
                if rel not in seen:
                    seen.add(rel)
                    yield TakeoutFile(rel, lambda f=f: open(f, "rb"))
        elif zipfile.is_zipfile(p):
            zf = zipfile.ZipFile(p)
            for info in zf.infolist():
                if info.is_dir():
                    continue
                rel = rel_takeout_path(info.filename)
                if rel not in seen:
                    seen.add(rel)
                    yield TakeoutFile(rel, lambda info=info, zf=zf: zf.open(info))
        elif tarfile.is_tarfile(p):
            tf = tarfile.open(p)
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
            files = list(iter_takeout_files([p]))
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
                source=label if label == "comments" else "comments",
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
        try:
            import bs4  # noqa: F401
        except ImportError:
            log.warning("beautifulsoup4 not installed - falling back to URL scan for %s", tf.rel)
            return self._scan_urls(raw, label)
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(raw, "lxml" if _has_lxml() else "html.parser")
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
                vid = vid or (URL_VIDEO_RE.search(h).group(1) if URL_VIDEO_RE.search(h) else None)
                post = post or (URL_POST_RE.search(h).group(1) if URL_POST_RE.search(h) else None)
                cid = cid or (URL_LC_RE.search(h).group(1) if URL_LC_RE.search(h) else None)
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


def _has_lxml() -> bool:
    try:
        import lxml  # noqa: F401

        return True
    except ImportError:
        return False


# --------------------------------------------------------------------------------------
# HTTP + InnerTube client
# --------------------------------------------------------------------------------------


BOT_CHECK_RE = re.compile(r"confirm you.{0,3}re not a bot|не бот", re.I)


class RateLimited(RuntimeError):
    pass


class Throttle:
    """Global concurrency and rate limiter shared across worker threads."""

    def __init__(self, interval: float, concurrency: int = 4):
        self.interval = max(0.0, interval)
        self.concurrency = max(1, concurrency)
        self.slots: queue.Queue[float] = queue.Queue()
        for _ in range(self.concurrency):
            self.slots.put(0.0)

    def __enter__(self) -> Throttle:
        now = time.monotonic()
        ready_at = self.slots.get()
        delay = ready_at - now
        if delay > 0:
            time.sleep(delay)
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.slots.put(time.monotonic() + self.interval)

    def wait(self) -> None:
        """Backwards compatibility: acquire and release."""
        with self:
            pass


class InnerTube:
    BASE = "https://www.youtube.com/youtubei/v1/"
    CLIENT_VERSION = "2.20251101.00.00"

    def __init__(self, throttle: Throttle, hl: str = "en", gl: str = "US", cookies: http.cookiejar.CookieJar | None = None, retries: int = 5):
        self.s = requests.Session()
        self.s.headers.update(
            {
                "User-Agent": WEB_UA,
                "Accept-Language": f"{hl},en;q=0.8",
                "Origin": "https://www.youtube.com",
                "Referer": "https://www.youtube.com/",
                "X-Youtube-Client-Name": "1",
                "X-Youtube-Client-Version": self.CLIENT_VERSION,
            }
        )
        self.s.cookies.set("SOCS", "CAI", domain=".youtube.com")  # skip EU consent
        if cookies is not None:
            for c in cookies:
                self.s.cookies.set_cookie(c)
        self.sapisid = next((c.value for c in self.s.cookies if c.name in ("SAPISID", "__Secure-3PAPISID")), None)
        self.context = {"client": {"clientName": "WEB", "clientVersion": self.CLIENT_VERSION, "hl": hl, "gl": gl}}
        self.throttle = throttle
        self.retries = retries

    def _auth_headers(self) -> dict:
        if not self.sapisid:
            return {}
        ts = str(int(time.time()))
        h = hashlib.sha1(f"{ts} {self.sapisid} https://www.youtube.com".encode()).hexdigest()
        return {"Authorization": f"SAPISIDHASH {ts}_{h}", "X-Goog-AuthUser": "0", "X-Origin": "https://www.youtube.com"}

    def call(self, endpoint: str, body: dict) -> dict:
        endpoint = endpoint.lstrip("/").replace("youtubei/v1/", "")
        payload = {"context": self.context, **body}
        last_err: Exception | None = None
        for attempt in range(self.retries):
            try:
                with self.throttle:
                    r = self.s.post(self.BASE + endpoint, params={"prettyPrint": "false"}, json=payload, headers=self._auth_headers(), timeout=45)
            except requests.RequestException as e:
                last_err = e
                time.sleep(2 * 2**attempt)
                continue
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                try:
                    return {"__error__": r.status_code, **r.json()}
                except ValueError:
                    return {"__error__": r.status_code}
            if r.status_code == 429 or "google.com/sorry" in r.url:
                last_err = RateLimited("YouTube rate limit (HTTP 429)")
                time.sleep(min(300, 15 * 2**attempt))
                continue
            last_err = RuntimeError(f"HTTP {r.status_code} from {endpoint}")
            time.sleep(3 * 2**attempt)
        raise last_err or RuntimeError("request failed")

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


@dataclass
class Options:
    out: Path
    top_comments: int = 20
    top_comment_replies: int = 0
    max_replies: int = 100
    thumbnails: bool = True
    dislikes: bool = True
    original_images: bool = True
    refresh: bool = False
    hl: str = "en"
    gl: str = "US"
    cookies_file: str | None = None
    sleep: float = 1.0
    concurrency: int = 4


class Enricher:
    def __init__(self, opt: Options):
        self.opt = opt
        self.throttle = Throttle(opt.sleep, opt.concurrency)
        self.cookiejar: http.cookiejar.MozillaCookieJar | None = None
        if opt.cookies_file:
            self.cookiejar = http.cookiejar.MozillaCookieJar(opt.cookies_file)
            self.cookiejar.load(ignore_discard=True, ignore_expires=True)
        self.local = threading.local()
        self.bot_checks = 0  # consecutive "confirm you're not a bot" answers
        self.bot_lock = threading.Lock()

    # per-thread clients (requests.Session / YoutubeDL are not shared)
    @property
    def yt(self) -> InnerTube:
        if not hasattr(self.local, "yt"):
            self.local.yt = InnerTube(self.throttle, self.opt.hl, self.opt.gl, self.cookiejar)
        return self.local.yt

    def ydl(self, flat: bool = False) -> Any:
        key = "ydl_flat" if flat else "ydl"
        if not hasattr(self.local, key):
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
            setattr(self.local, key, yt_dlp.YoutubeDL(params))
        return getattr(self.local, key)

    def item_dir(self, item: Item) -> Path:
        return self.opt.out / f"{item.kind}s" / item.id

    def done(self, item: Item) -> bool:
        f = self.item_dir(item) / "info.json"
        if self.opt.refresh or not f.exists():
            return False
        try:
            return json.loads(f.read_text("utf-8")).get("status") in ("ok", "unavailable")
        except ValueError:
            return False

    def process(self, item: Item) -> dict:
        d = self.item_dir(item)
        d.mkdir(parents=True, exist_ok=True)
        res: dict[str, Any] = {"kind": item.kind, "id": item.id, "fetched_at": now_iso(), "tool_version": __version__}
        try:
            if item.kind == "video":
                self._video(item, d, res)
            elif item.kind == "post":
                self._post(item, d, res)
            else:
                self._channel(item, d, res)
            res.setdefault("status", "ok")
        except RateLimited:
            raise
        except Exception as e:
            log.debug("failure on %s %s", item.kind, item.id, exc_info=True)
            res["status"] = "error"
            res["error"] = f"{type(e).__name__}: {e}"
        res["takeout"] = item.to_json()
        tmp = d / "info.json.tmp"
        tmp.write_text(json.dumps(res, ensure_ascii=False, indent=2), "utf-8")
        tmp.replace(d / "info.json")
        return res

    # ---------------- downloads ----------------

    def download(self, url: str | None, dest_stem: Path) -> str | None:
        if not url:
            return None
        for existing in dest_stem.parent.glob(dest_stem.name + ".*"):
            if existing.suffix != ".part" and not self.opt.refresh:
                return existing.name
        try:
            with self.throttle:
                r = self.yt.s.get(url, timeout=60)
            if r.status_code != 200 or not r.content:
                return None
            ctype = r.headers.get("content-type", "").split(";")[0].strip()
            ext = {"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp", "image/gif": ".gif", "image/avif": ".avif"}.get(ctype)
            if not ext:
                ext = Path(urllib.parse.urlparse(url).path).suffix or ".bin"
            path = dest_stem.with_suffix(ext)
            path.write_bytes(r.content)
            return path.name
        except requests.RequestException as e:
            log.debug("download failed %s: %s", url, e)
            return None

    # ---------------- comments (shared by videos and posts) ----------------

    def _comments(self, endpoint: str, token_fn: Callable[..., str], item: Item) -> dict:
        yt = self.yt
        out: dict[str, Any] = {"sort": "top", "top": [], "user_threads": []}
        # 1. top-N
        if self.opt.top_comments > 0:
            top_threads: list[tuple[dict, dict]] = []
            for thread, page in yt.iter_threads(endpoint, token_fn(SORT_TOP)):
                out.setdefault("total_count_text", page.get("count_text"))
                c = dict(thread["comment"])
                top_threads.append((c, thread))
                if len(top_threads) >= self.opt.top_comments:
                    break

            if self.opt.top_comment_replies:
                need_replies = [(idx, t) for idx, (c, t) in enumerate(top_threads) if c.get("reply_count")]

                def fetch_reply(t: dict) -> list[dict]:
                    try:
                        return self.yt.replies(endpoint, t, self.opt.top_comment_replies)
                    except RateLimited:
                        raise
                    except Exception as e:
                        log.debug("failed to fetch replies: %s", e)
                        return []

                if len(need_replies) == 1:
                    idx, t = need_replies[0]
                    top_threads[idx][0]["replies"] = fetch_reply(t)
                elif len(need_replies) > 1:
                    workers = min(len(need_replies), self.opt.concurrency)
                    with cf.ThreadPoolExecutor(max_workers=workers) as pool:
                        futs = {pool.submit(fetch_reply, t): idx for idx, t in need_replies}
                        for f in cf.as_completed(futs):
                            idx = futs[f]
                            top_threads[idx][0]["replies"] = f.result()

            out["top"] = [c for c, _ in top_threads]

        # 2. the user's own comments, located via the "highlighted comment" mechanism
        if item.user_comments:
            if len(item.user_comments) == 1:
                out["user_threads"].append(self._user_thread(endpoint, token_fn, item.user_comments[0]))
            else:
                workers = min(len(item.user_comments), self.opt.concurrency)
                with cf.ThreadPoolExecutor(max_workers=workers) as pool:
                    futs = [pool.submit(self._user_thread, endpoint, token_fn, uc) for uc in item.user_comments]
                    out["user_threads"] = [f.result() for f in futs]

        if not out["user_threads"]:
            del out["user_threads"]
        return out

    def _user_thread(self, endpoint: str, token_fn: Callable[..., str], uc: UserComment) -> dict:
        res: dict[str, Any] = {"comment_id": uc.comment_id, "is_reply": uc.is_reply, "takeout_text": uc.text, "created": uc.created}
        if not uc.comment_id:
            res["status"] = "no_comment_id_in_takeout"
            return res
        top_id = uc.parent_id or uc.comment_id
        thread = None
        for t, page in self.yt.iter_threads(endpoint, token_fn(SORT_TOP, uc.comment_id), max_pages=1):
            if t["comment"]["id"] == top_id:
                thread = t
            break
        if thread is None:
            res["status"] = "not_found_online"  # deleted, held for review, or hidden
            return res
        yt = self.yt
        if not uc.is_reply:
            res["status"] = "ok"
            res["comment"] = thread["comment"]
            if thread["comment"]["reply_count"] and self.opt.max_replies != 0:
                res["replies"] = yt.replies(endpoint, thread, self.opt.max_replies)
            else:
                res["replies"] = []
            return res
        # it's a reply: parent = the comment it replied to
        res["replying_to"] = thread["comment"]
        own = next((c for c in thread["teasers"] if c["id"] == uc.comment_id), None)
        siblings: list[dict] = []
        if self.opt.max_replies != 0:
            siblings = yt.replies(endpoint, thread, self.opt.max_replies)
            own = own or next((c for c in siblings if c["id"] == uc.comment_id), None)
        if own is None:
            res["status"] = "reply_not_found_online"
            return res
        res["status"] = "ok"
        res["comment"] = own
        # YouTube threads are flat: "replies to a reply" are later replies in the
        # same thread that @mention you.
        handle = (own["author"].get("name") or "").lstrip("@")
        if handle and siblings:
            ids = [c["id"] for c in siblings]
            after = siblings[ids.index(own["id"]) + 1 :] if own["id"] in ids else []
            res["replies"] = [c for c in after if re.search(r"@" + re.escape(handle) + r"\b", c["text"], re.I)]
        return res

    # ---------------- video ----------------

    VIDEO_FIELDS = (
        "id title description channel channel_id channel_url uploader uploader_id uploader_url "
        "channel_follower_count channel_is_verified upload_date timestamp release_timestamp release_date "
        "duration duration_string view_count concurrent_view_count like_count comment_count age_limit "
        "categories tags chapters live_status was_live availability playable_in_embed language "
        "media_type webpage_url thumbnail"
    ).split()

    def _video(self, item: Item, d: Path, res: dict) -> None:
        vid = item.id
        res["url"] = f"https://www.youtube.com/watch?v={vid}"
        info = None
        ydl = self.ydl()
        ydl.params["logger"].messages.clear()
        try:
            info = ydl.extract_info(res["url"], download=False, process=False)
        except Exception as e:  # yt_dlp.utils.DownloadError
            info = None
            res["error"] = _clean_ydl_msg(str(e))
        if info is not None and not info.get("title"):
            # playability errors (removed/private/terminated) are downgraded to
            # warnings by ignore_no_formats_error -> recover the reason
            reasons = [m for m in ydl.params["logger"].messages if "429" not in m and "JavaScript" not in m]
            res["error"] = _clean_ydl_msg(reasons[-1]) if reasons else "video unavailable"
            info = None
        if info is None:
            res["status"] = "unavailable"
            res.setdefault("error", "video unavailable")
            if BOT_CHECK_RE.search(res["error"]):
                # IP flagged by YouTube: retry this item on the next run, and
                # stop the run if it keeps happening
                res["status"] = "error"
                with self.bot_lock:
                    self.bot_checks += 1
                    if self.bot_checks >= 5:
                        raise RateLimited(res["error"])
            else:
                if re.match(r"Please sign in\b", res["error"]):
                    res["status"] = "error"  # usually a temporary IP flag -> retry next run
                with self.bot_lock:
                    self.bot_checks = 0
        else:
            with self.bot_lock:
                self.bot_checks = 0
        if info:
            meta = {k: info.get(k) for k in self.VIDEO_FIELDS if info.get(k) is not None}
            if meta.get("media_type") is None and "/shorts/" in (info.get("webpage_url") or ""):
                meta["media_type"] = "short"
            res["metadata"] = meta
        # Fetch estimated dislikes, thumbnail, and comments concurrently
        def fetch_ryd() -> dict | None:
            if self.opt.dislikes:
                return self._ryd(vid)
            return None

        def fetch_thumb() -> tuple[str | None, str | None]:
            if not self.opt.thumbnails:
                return None, None
            cands = [info.get("thumbnail")] if info else []
            cands += [f"https://i.ytimg.com/vi/{vid}/maxresdefault.jpg", f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"]
            for u in cands:
                name = self.download(u, d / "thumbnail")
                if name:
                    return name, u
            return None, None

        def fetch_comm() -> dict | None:
            if info and (self.opt.top_comments > 0 or item.user_comments):
                return self._comments("next", lambda sort, hl=None: video_comments_token(vid, sort, hl), item)
            return None

        with cf.ThreadPoolExecutor(max_workers=3) as pool:
            f_ryd = pool.submit(fetch_ryd)
            f_thumb = pool.submit(fetch_thumb)
            f_comm = pool.submit(fetch_comm)

            ryd = f_ryd.result()
            thumb_name, thumb_url = f_thumb.result()
            comm = None
            comm_err: Exception | None = None
            try:
                comm = f_comm.result()
            except RateLimited:
                raise
            except Exception as e:
                comm_err = e

        if ryd and (info or ryd.get("dislikes") or ryd.get("likes")):
            res.setdefault("metadata", {})["dislike_count_estimate"] = ryd.get("dislikes")
            res["return_youtube_dislike"] = ryd

        if thumb_name:
            res["thumbnail_file"] = thumb_name
            res["thumbnail_url"] = thumb_url

        if comm is not None:
            res["comments"] = comm
        elif comm_err is not None:
            res["comments"] = {"error": f"{type(comm_err).__name__}: {comm_err}"}

    def _ryd(self, vid: str) -> dict | None:
        try:
            with self.throttle:
                r = self.yt.s.get("https://returnyoutubedislikeapi.com/votes", params={"videoId": vid}, timeout=20)
            if r.status_code == 200:
                j = r.json()
                return {k: j.get(k) for k in ("likes", "dislikes", "rating", "viewCount", "deleted", "dateCreated")}
        except (requests.RequestException, ValueError):
            pass
        return None

    # ---------------- post ----------------

    def _download_post_image(self, i: int, u: str, d: Path) -> str | None:
        name = None
        if self.opt.original_images and "=" in u and ("ggpht.com" in u or "googleusercontent.com" in u):
            name = self.download(u.split("=")[0] + "=s0", d / f"image_{i}")
        return name or self.download(u, d / f"image_{i}")

    def _post(self, item: Item, d: Path, res: dict) -> None:
        res["url"] = f"https://www.youtube.com/post/{item.id}"
        got = self.yt.post(item.id)
        if got["status"] != "ok":
            res["status"] = got["status"]
            res["error"] = got.get("error")
            return
        post = got["post"]
        res["post"] = post
        if self.opt.thumbnails and post["images"]:
            if len(post["images"]) == 1:
                res["image_files"] = [self._download_post_image(1, post["images"][0], d)]
            else:
                workers = min(len(post["images"]), self.opt.concurrency)
                with cf.ThreadPoolExecutor(max_workers=workers) as pool:
                    futs = [pool.submit(self._download_post_image, i, u, d) for i, u in enumerate(post["images"], 1)]
                    res["image_files"] = [f.result() for f in futs]
        ucid = post["author"].get("channel_id")
        if ucid and (self.opt.top_comments > 0 or item.user_comments):
            try:
                res["comments"] = self._comments("browse", lambda sort, hl=None: post_comments_token(item.id, ucid, sort, hl), item)
            except RateLimited:
                raise
            except Exception as e:
                res["comments"] = {"error": f"{type(e).__name__}: {e}"}

    # ---------------- channel ----------------

    CHANNEL_FIELDS = (
        "id channel channel_id title description channel_follower_count uploader_id uploader_url "
        "channel_url channel_is_verified tags availability"
    ).split()

    def _channel(self, item: Item, d: Path, res: dict) -> None:
        url = f"https://www.youtube.com/channel/{item.id}"
        res["url"] = url
        try:
            info = self.ydl(flat=True).extract_info(url, download=False, process=False)
        except Exception as e:
            msg = _clean_ydl_msg(str(e))
            res["status"] = "unavailable"
            res["error"] = msg
            if "429" in msg:
                raise RateLimited(msg)
            return
        res["metadata"] = {k: info.get(k) for k in self.CHANNEL_FIELDS if info.get(k) is not None}
        thumbs = {t.get("id"): t.get("url") for t in info.get("thumbnails") or []}
        res["banner_url"] = thumbs.get("banner_uncropped")
        res["avatar_url"] = thumbs.get("avatar_uncropped")
        if self.opt.thumbnails:
            downloads: list[tuple[str, str, Path]] = []
            if res["banner_url"]:
                downloads.append(("banner_file", res["banner_url"], d / "banner"))
            if res["avatar_url"]:
                downloads.append(("avatar_file", res["avatar_url"], d / "avatar"))
            if len(downloads) == 1:
                key, u, dest = downloads[0]
                res[key] = self.download(u, dest)
            elif len(downloads) > 1:
                with cf.ThreadPoolExecutor(max_workers=len(downloads)) as pool:
                    futs = {pool.submit(self.download, u, dest): key for key, u, dest in downloads}
                    for f in cf.as_completed(futs):
                        res[futs[f]] = f.result()


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
    msg = re.split(r"\.?\s+Use --cookies", msg)[0]  # drop yt-dlp's CLI advice
    return msg.strip()


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------


def write_summary(out: Path, inv: Inventory) -> None:
    rows = []
    for kind in ("video", "post", "channel"):
        for id_, item in inv.items[kind].items():
            f = out / f"{kind}s" / id_ / "info.json"
            info: dict[str, Any] = {}
            if f.exists():
                try:
                    info = json.loads(f.read_text("utf-8"))
                except ValueError:
                    pass
            meta = info.get("metadata") or {}
            post = info.get("post") or {}
            title = meta.get("title") or post.get("text", "")[:120] or item.title or ""
            ut = (info.get("comments") or {}).get("user_threads") or []
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
                    "error": info.get("error", ""),
                }
            )
    with open(out / "summary.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()) if rows else ["kind", "id"])
        w.writeheader()
        w.writerows(rows)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Enrich a YouTube Google Takeout export with video/post/channel details, thumbnails and comments.",
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
    ap.add_argument("--workers", type=int, default=2, help="parallel workers (items processed in parallel)")
    ap.add_argument(
        "--concurrency",
        "--parallel-requests",
        type=int,
        default=None,
        help="maximum concurrent in-flight HTTP requests across all workers (default: max(4, workers * 2))",
    )
    ap.add_argument("--sleep", type=float, default=1.0, help="minimum seconds between requests per slot (0 = no delay)")
    ap.add_argument("--no-thumbnails", action="store_true", help="skip downloading thumbnails/banners/post images")
    ap.add_argument("--resized-images", action="store_true", help="save post images as served (webp) instead of originals")
    ap.add_argument("--no-dislikes", action="store_true", help="skip the Return YouTube Dislike lookup")
    ap.add_argument("--cookies", help="Netscape cookies.txt for youtube.com (age-restricted/members content, fewer rate limits)")
    ap.add_argument("--lang", default="en", help="interface language for relative dates/labels")
    ap.add_argument("--region", default="US", help="content region")
    ap.add_argument("--refresh", action="store_true", help="re-fetch items that were already saved")
    ap.add_argument("--dry-run", action="store_true", help="only parse Takeout and write inventory.json")
    ap.add_argument("-v", "--verbose", action="count", default=0)
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose > 1 else logging.INFO,
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
    (out / "inventory.json").write_text(
        json.dumps(
            {
                "generated_at": now_iso(),
                "files": inv.files,
                "items": {k: {i: it.to_json() for i, it in v.items()} for k, v in inv.items.items()},
            },
            ensure_ascii=False,
            indent=1,
        ),
        "utf-8",
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

    if "video" in kinds or "channel" in kinds:
        try:
            import yt_dlp  # noqa: F401
        except ImportError:
            log.error("yt-dlp is required for video/channel metadata: pip install -U yt-dlp")
            return 1

    concurrency = args.concurrency if args.concurrency is not None else max(4, args.workers * 2)

    enr = Enricher(
        Options(
            out=out,
            top_comments=args.top_comments,
            top_comment_replies=args.top_comment_replies,
            max_replies=args.max_replies,
            thumbnails=not args.no_thumbnails,
            dislikes=not args.no_dislikes,
            original_images=not args.resized_images,
            refresh=args.refresh,
            hl=args.lang,
            gl=args.region,
            cookies_file=args.cookies,
            sleep=args.sleep,
            concurrency=concurrency,
        )
    )
    pending = [i for i in todo if not enr.done(i)]
    log.info(
        "%d items to process (%d already done) with %d item workers, max %d concurrent requests (sleep=%.1fs)",
        len(pending),
        len(todo) - len(pending),
        args.workers,
        concurrency,
        args.sleep,
    )

    stats: dict[str, int] = {}
    stop = threading.Event()

    def work(item: Item) -> None:
        if stop.is_set():
            return
        try:
            r = enr.process(item)
        except RateLimited as e:
            if not stop.is_set():
                log.error("Rate limited by YouTube (%s). Stopping; re-run later to resume (try --sleep 3 or --cookies).", e)
            stop.set()
            return
        st = r.get("status", "?")
        stats[st] = stats.get(st, 0) + 1
        title = (r.get("metadata") or {}).get("title") or ((r.get("post") or {}).get("text") or "")[:60] or item.title or ""
        extra = ""
        ut = (r.get("comments") or {}).get("user_threads")
        if ut:
            extra = f" | your comments found: {sum(1 for t in ut if t.get('status') == 'ok')}/{len(ut)}"
        log.info("[%s] %-7s %s %s%s", st, item.kind, item.id, title[:60].replace("\n", " "), extra)

    try:
        with cf.ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
            futures = [ex.submit(work, item) for item in pending]
            for f in cf.as_completed(futures):
                if stop.is_set():
                    for remaining in futures:
                        remaining.cancel()
                    break
                f.result()
    except KeyboardInterrupt:
        stop.set()
        log.warning("Interrupted - progress is saved, re-run to resume.")
    write_summary(out, inv)
    log.info("Done: %s. Output in %s", ", ".join(f"{k}={v}" for k, v in sorted(stats.items())) or "nothing new", out.resolve())
    return 2 if stop.is_set() else 0


if __name__ == "__main__":
    sys.exit(main())
