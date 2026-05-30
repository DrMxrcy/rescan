import argparse as _argparse
import signal
import os
import sys
import requests
import configparser
import xml.etree.ElementTree as ET
from urllib.parse import quote
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from plexapi.server import PlexServer
import logging
from datetime import datetime
import schedule
import discord
from discord import Webhook, Embed, Color
import asyncio
import aiohttp
from state_cache import StateCache

# === CONFIG ===

_parser = _argparse.ArgumentParser(
    description="Rescan media library scanner", add_help=False
)
_parser.add_argument("--config", default="config/config.ini", help="Path to config.ini")
_args, _ = _parser.parse_known_args()
CONFIG_PATH = _args.config

if not os.path.exists(CONFIG_PATH):
    logging.basicConfig(
        level=logging.ERROR,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger(__name__).error("[FAIL] config file not found: %s", CONFIG_PATH)
    sys.exit(1)

config = configparser.ConfigParser()
config.read(CONFIG_PATH)

try:
    LOG_LEVEL = config["logs"]["loglevel"]
    SCAN_INTERVAL = int(config["behaviour"]["scan_interval"])
    RUN_INTERVAL = int(config["behaviour"]["run_interval"])
    SYMLINK_CHECK = config.getboolean("behaviour", "symlink_check", fallback=False)
    NOTIFICATIONS_ENABLED = config.getboolean("notifications", "enabled", fallback=True)
    STATE_CACHE_ENABLED = config.getboolean("behaviour", "state_cache", fallback=True)
    STATE_DB_RAW = config.get("behaviour", "state_db", fallback="rescan.db").strip()
    REPAIR_SCAN_COOLDOWN_HOURS = config.getfloat(
        "behaviour", "repair_scan_cooldown_hours", fallback=24.0
    )
    LIBRARY_WORKERS = config.getint("behaviour", "library_workers", fallback=2)
    METADATA_REPAIR_ENABLED = config.getboolean(
        "behaviour", "metadata_repair", fallback=False
    )
    CACHE_TIMEOUT = config.getint("behaviour", "cache_timeout_seconds", fallback=60)
    CACHE_RETRY_WAIT = config.getint(
        "behaviour", "cache_retry_wait_seconds", fallback=60
    )
    CACHE_RETRY_ATTEMPTS = config.getint(
        "behaviour", "cache_retry_attempts", fallback=0
    )
    CACHE_PAGE_MAX_RETRIES = config.getint(
        "behaviour", "cache_page_max_retries", fallback=5
    )
    BATCH_SIZE = config.getint("behaviour", "batch_size", fallback=25)
    BATCH_DELAY = config.getint("behaviour", "batch_delay_seconds", fallback=10)
    CACHE_PAGE_SIZE = config.getint("behaviour", "cache_page_size", fallback=100)
    directories_raw = config["scan"]["directories"]
except (KeyError, configparser.NoSectionError) as e:
    print(
        f"ERROR: Missing required config section/key: {e}. Please check your config.ini against config-example.ini."
    )
    sys.exit(1)

DISCORD_WEBHOOK_URL = config.get("notifications", "discord_webhook_url", fallback="")
STATE_DB_PATH = STATE_DB_RAW or "rescan.db"
if not os.path.isabs(STATE_DB_PATH):
    STATE_DB_PATH = os.path.join(
        os.path.dirname(os.path.abspath(CONFIG_PATH)), STATE_DB_PATH
    )
# Environment variable overrides (take precedence over config.ini)
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", DISCORD_WEBHOOK_URL)
DISCORD_AVATAR_URL = (
    "https://raw.githubusercontent.com/pukabyte/rescan/master/assets/logo.png"
)
DISCORD_WEBHOOK_NAME = "Rescan"

# Support both comma-separated or line-separated values
SCAN_PATHS = [
    path.strip()
    for path in directories_raw.replace("\n", ",").split(",")
    if path.strip()
]

# Media file extensions to look for
MEDIA_EXTENSIONS = {
    ".mp4",
    ".mkv",
    ".avi",
    ".mov",
    ".wmv",
    ".flv",
    ".webm",
    ".m4v",
    ".m4p",
    ".m4b",
    ".m4r",
    ".3gp",
    ".mpg",
    ".mpeg",
    ".m2v",
    ".m2ts",
    ".ts",
    ".vob",
    ".iso",
}

# Global library IDs and path mappings (per server)
library_ids = {}
library_paths = {}
# Cache for directory-level searches to minimize API calls
directory_cache = {}
directory_cache_lock = threading.Lock()
# Per-server library cache to avoid redundant API calls
_library_cache = {}
# Bulk path cache for Jellyfin/Emby (built once per scan cycle)
_server_path_caches: dict = {}  # {server_url: set of normalized file paths}
_server_item_caches: dict = {}  # {server_url: {normalized_path: item}}
_failed_cache_servers: set = set()  # server URLs whose cache build failed this cycle

# ANSI escape codes for text formatting
BOLD = "\033[1m"
RESET = "\033[0m"

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper()),
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Graceful shutdown support
_shutdown_requested = False
state_cache = StateCache(
    STATE_CACHE_ENABLED, STATE_DB_PATH, REPAIR_SCAN_COOLDOWN_HOURS, logger
)


def _handle_shutdown(signum, frame):
    global _shutdown_requested
    logger.info(
        "[SHUTDOWN] Signal received — will exit after current operation completes"
    )
    _shutdown_requested = True


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)


def _request_with_retry(method, url, retries=2, **kwargs):
    """Wrap requests calls with simple retry logic on transient errors."""
    transient_statuses = {429, 500, 502, 503, 504}
    transient_exceptions = (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
    )
    delay = 1
    last_exc = None
    for attempt in range(retries + 1):
        try:
            response = method(url, **kwargs)
            if response.status_code not in transient_statuses or attempt == retries:
                return response
            logger.warning(
                f"[RETRY] HTTP {response.status_code} from {url} retrying in {delay}s"
            )
        except transient_exceptions as e:
            last_exc = e
            if attempt == retries:
                raise
            logger.warning(f"[RETRY] Connection error: {url} retrying in {delay}s")
        time.sleep(delay)
        delay *= 2
    if last_exc:
        raise last_exc


def _embed_content_length(embed):
    """Return the actual character count of an embed's content fields."""
    total = len(embed.title or "") + len(embed.description or "")
    for f in embed.fields:
        total += len(f.name) + len(f.value)
    return total


# Initialize media servers (support Plex, Jellyfin, and Emby)
media_servers = []


# Helper function to parse server entries
def parse_server_entry(entry, default_type="plex"):
    """Parse a server entry in format: type:url:token or url:token (for backward compat)

    Handles URLs with ports (e.g., http://host:port:token) by checking if first part is a known type.
    """
    entry = entry.strip()

    # Check if entry starts with a known server type
    known_types = ["plex", "jellyfin", "emby"]
    for server_type in known_types:
        if entry.startswith(f"{server_type}:"):
            # Format: type:url:token (where url may contain :port)
            # Remove the type prefix, then split on last colon
            remaining = entry[len(server_type) + 1 :]  # Remove "type:"
            last_colon_idx = remaining.rfind(":")
            if last_colon_idx > 0:
                url_part = remaining[:last_colon_idx]
                token_part = remaining[last_colon_idx + 1 :]
                return {
                    "type": server_type.lower(),
                    "url": url_part,
                    "token": token_part,
                }

    # Otherwise, treat as url:token format (backward compatible)
    # Find the last colon to split url from token (handles URLs with ports)
    last_colon_idx = entry.rfind(":")
    if last_colon_idx > 0:
        url_part = entry[:last_colon_idx]
        token_part = entry[last_colon_idx + 1 :]
        return {"type": default_type, "url": url_part, "token": token_part}

    return None


# Load Plex servers (backward compatible)
if "plex" in config:
    if "servers" in config["plex"]:
        # New format: multiple servers
        servers_raw = config["plex"]["servers"]
        servers_list = [
            s.strip() for s in servers_raw.replace("\n", ",").split(",") if s.strip()
        ]
        for server_entry in servers_list:
            server_info = parse_server_entry(server_entry, default_type="plex")
            if server_info and server_info["type"] == "plex":
                try:
                    plex_server = PlexServer(server_info["url"], server_info["token"])
                    media_servers.append(
                        {
                            "type": "plex",
                            "url": server_info["url"],
                            "token": server_info["token"],
                            "server": plex_server,
                        }
                    )
                    logger.info(f"[OK] Connected to Plex: {server_info['url']}")
                except Exception as e:
                    logger.error(
                        f"[FAIL] Could not connect to Plex: {server_info['url']} - {str(e)}"
                    )
            elif server_info:
                logger.warning(
                    f"[WARN] Invalid server type in plex section: {server_info['type']}"
                )
    elif "server" in config["plex"] and "token" in config["plex"]:
        # Old format: single server (backward compatible)
        PLEX_URL = config["plex"]["server"]
        TOKEN = os.environ.get("PLEX_TOKEN", config["plex"]["token"])
        try:
            plex_server = PlexServer(PLEX_URL, TOKEN)
            media_servers.append(
                {"type": "plex", "url": PLEX_URL, "token": TOKEN, "server": plex_server}
            )
            logger.info(f"[OK] Connected to Plex: {PLEX_URL}")
        except Exception as e:
            logger.error(f"[FAIL] Could not connect to Plex: {PLEX_URL} - {str(e)}")

# Load Jellyfin servers
if "jellyfin" in config:
    if "servers" in config["jellyfin"]:
        servers_raw = config["jellyfin"]["servers"]
        servers_list = [
            s.strip() for s in servers_raw.replace("\n", ",").split(",") if s.strip()
        ]
        for server_entry in servers_list:
            server_info = parse_server_entry(server_entry, default_type="jellyfin")
            if server_info and server_info["type"] == "jellyfin":
                media_servers.append(
                    {
                        "type": "jellyfin",
                        "url": server_info["url"],
                        "token": server_info["token"],
                    }
                )
                logger.info(f"[OK] Connected to Jellyfin: {server_info['url']}")
    elif "server" in config["jellyfin"] and "token" in config["jellyfin"]:
        media_servers.append(
            {
                "type": "jellyfin",
                "url": config["jellyfin"]["server"],
                "token": os.environ.get("JELLYFIN_TOKEN", config["jellyfin"]["token"]),
            }
        )
        logger.info(f"[OK] Connected to Jellyfin: {config['jellyfin']['server']}")

# Load Emby servers
if "emby" in config:
    if "servers" in config["emby"]:
        servers_raw = config["emby"]["servers"]
        servers_list = [
            s.strip() for s in servers_raw.replace("\n", ",").split(",") if s.strip()
        ]
        for server_entry in servers_list:
            server_info = parse_server_entry(server_entry, default_type="emby")
            if server_info and server_info["type"] == "emby":
                media_servers.append(
                    {
                        "type": "emby",
                        "url": server_info["url"],
                        "token": server_info["token"],
                    }
                )
                logger.info(f"[OK] Connected to Emby: {server_info['url']}")
    elif "server" in config["emby"] and "token" in config["emby"]:
        media_servers.append(
            {
                "type": "emby",
                "url": config["emby"]["server"],
                "token": config["emby"]["token"],
            }
        )
        logger.info(f"[OK] Connected to Emby: {config['emby']['server']}")

if not media_servers:
    logger.error("[FAIL] No media servers configured or all connections failed")
    exit(1)


class RunStats:
    def __init__(self):
        self.start_time = datetime.now()
        self.missing_items = defaultdict(list)
        self.errors = []
        self.warnings = []
        self.total_scanned = 0
        self.total_missing = 0
        self.broken_symlinks = 0

    def add_missing_item(self, library_name, file_path):
        self.missing_items[library_name].append(file_path)
        self.total_missing += 1

    def add_error(self, error):
        self.errors.append(error)

    def add_warning(self, warning):
        self.warnings.append(warning)

    def increment_scanned(self):
        self.total_scanned += 1

    def increment_broken_symlinks(self):
        self.broken_symlinks += 1

    def get_run_time(self):
        return datetime.now() - self.start_time

    async def send_discord_summary(self):
        if not NOTIFICATIONS_ENABLED:
            logger.info("[NOTIFY] Notifications disabled")
            return

        if not DISCORD_WEBHOOK_URL:
            logger.warning("[NOTIFY] Discord webhook URL not configured")
            return

        try:
            # Create webhook client with aiohttp session
            _timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=_timeout) as session:
                webhook = Webhook.from_url(DISCORD_WEBHOOK_URL, session=session)

                # Create embed
                embed = Embed(
                    title="Rescan Summary", color=Color.blue(), timestamp=datetime.now()
                )

                # Add overview
                embed.add_field(
                    name="📊 Overview",
                    value=f"Found **{self.total_missing}** items from **{self.total_scanned}** scanned files",
                    inline=False,
                )

                # Add broken symlinks summary if any
                if self.broken_symlinks > 0:
                    embed.add_field(
                        name="⚠️ Issues",
                        value=f"Broken Symlinks Skipped: **{self.broken_symlinks}**",
                        inline=False,
                    )

                # Add library-specific stats
                for library, items in self.missing_items.items():
                    embed.add_field(
                        name=f"📁 {library}",
                        value=f"Found: **{len(items)}** items",
                        inline=True,
                    )

                # Add other errors and warnings if any
                if self.errors or self.warnings:
                    error_text = "\n".join([f"❌ {e}" for e in self.errors])
                    warning_text = "\n".join([f"⚠️ {w}" for w in self.warnings])
                    if error_text or warning_text:
                        embed.add_field(
                            name="⚠️ Other Issues",
                            value=f"{error_text}\n{warning_text}",
                            inline=False,
                        )

                # Add footer
                embed.set_footer(text=f"Run Time: {self.get_run_time()}")

                # Send webhook
                await send_discord_webhook(webhook, embed)
                logger.info("[NOTIFY] Discord summary sent")

        except discord.HTTPException as e:
            logger.error(f"[FAIL] Discord API error: {str(e)}")
        except Exception as e:
            logger.error(f"[FAIL] Discord notification failed: {str(e)}")


async def _send_discord_alert(message, color=None):
    """Send a single-line Discord notification without a full stats summary."""
    if not DISCORD_WEBHOOK_URL:
        return
    try:
        embed = Embed(
            description=message,
            color=color or Color.orange(),
            timestamp=datetime.now(),
        )
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            webhook = Webhook.from_url(DISCORD_WEBHOOK_URL, session=session)
            await webhook.send(
                embed=embed,
                avatar_url=DISCORD_AVATAR_URL,
                username=DISCORD_WEBHOOK_NAME,
            )
    except Exception as e:
        logger.debug(f"[WARN] Discord alert failed: {e}")


async def send_discord_webhook(webhook, embed):
    """Send a Discord webhook message."""
    try:
        # Check if embed exceeds Discord's limits
        if _embed_content_length(embed) > 6000:
            # Split into multiple embeds
            base_embed = Embed(
                title=embed.title, color=embed.color, timestamp=embed.timestamp
            )

            # Add overview field
            if embed.fields and embed.fields[0].name == "📊 Overview":
                base_embed.add_field(
                    name=embed.fields[0].name, value=embed.fields[0].value, inline=False
                )

            # Send base embed
            await webhook.send(
                embed=base_embed,
                avatar_url=DISCORD_AVATAR_URL,
                username=DISCORD_WEBHOOK_NAME,
                wait=True,
            )

            # Create additional embeds for libraries
            current_embed = Embed(
                title="📁 Library Details", color=embed.color, timestamp=embed.timestamp
            )

            # Add library fields
            for field in embed.fields[1:]:
                if field.name.startswith("📁"):
                    if (
                        _embed_content_length(current_embed)
                        + len(field.name)
                        + len(field.value)
                        > 6000
                    ):
                        # Send current embed and create new one
                        await webhook.send(
                            embed=current_embed,
                            avatar_url=DISCORD_AVATAR_URL,
                            username=DISCORD_WEBHOOK_NAME,
                            wait=True,
                        )
                        current_embed = Embed(
                            title="📁 Library Details (continued)",
                            color=embed.color,
                            timestamp=embed.timestamp,
                        )
                    current_embed.add_field(
                        name=field.name, value=field.value, inline=field.inline
                    )

            # Send final library embed if it has fields
            if current_embed.fields:
                await webhook.send(
                    embed=current_embed,
                    avatar_url=DISCORD_AVATAR_URL,
                    username=DISCORD_WEBHOOK_NAME,
                    wait=True,
                )

            # Send issues in separate embed if they exist
            if embed.fields and embed.fields[-1].name == "⚠️ Issues":
                issues_embed = Embed(
                    title="⚠️ Issues", color=Color.red(), timestamp=embed.timestamp
                )
                issues_embed.add_field(
                    name=embed.fields[-1].name,
                    value=embed.fields[-1].value,
                    inline=False,
                )
                await webhook.send(
                    embed=issues_embed,
                    avatar_url=DISCORD_AVATAR_URL,
                    username=DISCORD_WEBHOOK_NAME,
                    wait=True,
                )
        else:
            # Send single embed if within limits
            await webhook.send(
                embed=embed,
                avatar_url=DISCORD_AVATAR_URL,
                username=DISCORD_WEBHOOK_NAME,
                wait=True,
            )
    except discord.HTTPException as e:
        logger.error(f"[FAIL] Discord API error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"[FAIL] Discord webhook failed: {str(e)}")
        raise


# Server-specific helper functions
def get_libraries_plex(server_info):
    """Get libraries from a Plex server."""
    libraries = []
    try:
        plex = server_info["server"]
        for section in plex.library.sections():
            lib_type = section.type
            lib_key = section.key
            lib_title = section.title
            lib_locations = []
            for location in section.locations:
                lib_locations.append(location)
            libraries.append(
                {
                    "type": lib_type,
                    "key": lib_key,
                    "title": lib_title,
                    "locations": lib_locations,
                }
            )
    except Exception as e:
        logger.error(f"[FAIL] Plex | Could not fetch libraries: {str(e)}")
    return libraries


def get_libraries_jellyfin(server_info):
    """Get libraries from a Jellyfin server."""
    libraries = []
    try:
        url = f"{server_info['url']}/Library/VirtualFolders"
        headers = {"X-Emby-Token": server_info["token"]}
        response = _request_with_retry(requests.get, url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Jellyfin API returns a list directly, not a dict with 'Items'
        library_list = data if isinstance(data, list) else data.get("Items", [])

        for library in library_list:
            lib_key = library.get("ItemId")
            lib_title = library.get("Name")
            lib_locations = library.get("Locations", [])
            libraries.append(
                {
                    "type": library.get("CollectionType", "unknown"),
                    "key": lib_key,
                    "title": lib_title,
                    "locations": lib_locations,
                }
            )
    except Exception as e:
        logger.error(f"[FAIL] Jellyfin | Could not fetch libraries: {str(e)}")
    return libraries


def get_libraries_emby(server_info):
    """Get libraries from an Emby server."""
    libraries = []
    try:
        url = f"{server_info['url']}/Library/VirtualFolders"
        headers = {"X-Emby-Token": server_info["token"]}
        response = _request_with_retry(requests.get, url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Emby API may return a list or a dict with 'Items'
        library_list = data if isinstance(data, list) else data.get("Items", [])

        for library in library_list:
            lib_key = library.get("ItemId")
            lib_title = library.get("Name")
            lib_locations = library.get("Locations", [])
            libraries.append(
                {
                    "type": library.get("CollectionType", "unknown"),
                    "key": lib_key,
                    "title": lib_title,
                    "locations": lib_locations,
                }
            )
    except Exception as e:
        logger.error(f"[FAIL] Emby | Could not fetch libraries: {str(e)}")
    return libraries


def _ping_server(server_info):
    """Quick reachability check before attempting an expensive cache build."""
    server_type = server_info["type"]
    if server_type == "plex":
        url = f"{server_info['url']}/identity"
        headers = {"X-Plex-Token": server_info["token"]}
    else:
        url = f"{server_info['url']}/System/Info"
        headers = {"X-Emby-Token": server_info["token"]}
    try:
        response = requests.get(url, headers=headers, timeout=5)
        return response.status_code < 500
    except Exception:
        return False


def _diagnose_stuck_page(
    url, headers, base_params, start_index, page_size, server_label
):
    """Binary-search a stuck page range to find the first item that causes a hang."""
    logger.info(
        f"[CACHE] {server_label} | Diagnosing stuck range {start_index:,}–{start_index + page_size - 1:,} ..."
    )
    lo, hi = start_index, start_index + page_size - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        try:
            r = requests.get(
                url,
                headers=headers,
                params={**base_params, "startIndex": mid, "limit": 1},
                timeout=CACHE_TIMEOUT,
            )
            r.raise_for_status()
            items = (
                r.json().get("Items", []) if isinstance(r.json(), dict) else r.json()
            )
            if items:
                item = items[0]
                logger.debug(
                    f"[CACHE] {server_label} | offset {mid:,} ok — "
                    f"Id={item.get('Id')} Path={item.get('Path')}"
                )
            lo = mid + 1
        except Exception:
            logger.warning(
                f"[CACHE] {server_label} | Stuck item at offset {mid:,} — "
                f"check Jellyfin item near this position (Id unknown, query timed out)"
            )
            hi = mid - 1

    logger.info(
        f"[CACHE] {server_label} | Diagnosis complete for range starting at {start_index:,}"
    )


def _build_server_path_cache(server_info, server_label):
    """Fetch all media item paths from a Jellyfin/Emby server in pages.

    Returns a set of normalised absolute paths.
    Called once per scan cycle; result stored in _server_path_caches and
    _server_item_caches.
    """
    url = f"{server_info['url']}/Items"
    headers = {"X-Emby-Token": server_info["token"]}
    page_size = CACHE_PAGE_SIZE
    base_params = {
        "recursive": "true",
        "includeItemTypes": "Movie,Episode",
        "fields": "Path,MediaSources"
        + (
            ",ProviderIds,PremiereDate,ProductionYear"
            if METADATA_REPAIR_ENABLED
            else ""
        ),
        "enableTotalRecordCount": "true",
    }
    paths: set = set()
    items_by_path = {}
    start_index = 0
    total_record_count = None

    logger.info(
        f"[CACHE] {server_label} | Fetching indexed paths from {server_info['url']} "
        f"in pages of {page_size}"
    )

    try:
        while True:
            params = {
                **base_params,
                "startIndex": start_index,
                "limit": page_size,
            }
            page_attempt = 0
            page_skipped = False
            while True:
                try:
                    response = _request_with_retry(
                        requests.get,
                        url,
                        headers=headers,
                        params=params,
                        timeout=CACHE_TIMEOUT,
                    )
                    response.raise_for_status()
                    break
                except Exception as page_exc:
                    if _shutdown_requested:
                        raise
                    page_attempt += 1
                    unlimited = CACHE_RETRY_ATTEMPTS == 0
                    if not unlimited and page_attempt >= CACHE_RETRY_ATTEMPTS:
                        raise
                    if (
                        CACHE_PAGE_MAX_RETRIES > 0
                        and page_attempt >= CACHE_PAGE_MAX_RETRIES
                    ):
                        logger.warning(
                            f"[CACHE] {server_label} | Page offset {start_index:,} skipped "
                            f"after {page_attempt} attempts — {page_size} items will not be cached"
                        )
                        page_skipped = True
                        break
                    attempt_label = (
                        f"{page_attempt}/∞"
                        if unlimited
                        else f"{page_attempt}/{CACHE_RETRY_ATTEMPTS}"
                    )
                    logger.warning(
                        f"[CACHE] {server_label} | Page offset {start_index:,} failed "
                        f"(attempt {attempt_label}), retrying in {CACHE_RETRY_WAIT}s: {page_exc}"
                    )
                    time.sleep(CACHE_RETRY_WAIT)
            if page_skipped:
                _diagnose_stuck_page(
                    url, headers, base_params, start_index, page_size, server_label
                )
                start_index += page_size
                if total_record_count is not None and start_index >= total_record_count:
                    break
                continue
            data = response.json()
            if isinstance(data, list):
                items = data
                total_record_count = len(items)
            else:
                items = data.get("Items", [])
                if total_record_count is None:
                    total_record_count = data.get("TotalRecordCount")

            for item in items:
                if "Path" in item:
                    normalized_path = os.path.normpath(item["Path"])
                    paths.add(normalized_path)
                    items_by_path[normalized_path] = item
                for ms in item.get("MediaSources", []):
                    if "Path" in ms:
                        normalized_path = os.path.normpath(ms["Path"])
                        paths.add(normalized_path)
                        items_by_path[normalized_path] = item

            page_count = len(items)
            start_index += page_count
            if total_record_count:
                logger.info(
                    f"[CACHE] {server_label} | Loaded {start_index:,}/{total_record_count:,} items"
                )

            if page_count == 0:
                break
            if total_record_count is not None and start_index >= total_record_count:
                break
            if page_count < page_size:
                break

        logger.info(
            f"[CACHE] {server_label} | Cached {len(paths):,} paths from {server_info['url']}"
        )
    except Exception as e:
        logger.error(f"[FAIL] {server_label} | Could not build path cache: {e}")
        _failed_cache_servers.add(server_info["url"])
    _server_item_caches[server_info["url"]] = items_by_path
    return paths


def get_library_ids():
    """Fetch library section IDs and paths dynamically from all media servers."""
    global library_ids, library_paths, _library_cache
    library_ids = {}
    library_paths = {}
    _library_cache = {}

    for server_info in media_servers:
        server_type = server_info["type"]
        server_url = server_info["url"]
        server_token = server_info["token"]

        libraries = []
        if server_type == "plex":
            libraries = get_libraries_plex(server_info)
        elif server_type == "jellyfin":
            libraries = get_libraries_jellyfin(server_info)
        elif server_type == "emby":
            libraries = get_libraries_emby(server_info)

        # Populate the library cache for this server
        _library_cache[server_url] = libraries

        for lib in libraries:
            lib_key_with_server = f"{server_url}:{lib['key']}"
            library_ids[lib_key_with_server] = {
                "type": lib["type"],
                "key": lib["key"],
                "title": lib["title"],
                "url": server_url,
                "token": server_token,
                "server_type": server_type,
            }

            for location in lib["locations"]:
                if location not in library_paths:
                    library_paths[location] = []
                library_paths[location].append(
                    {
                        "key": lib["key"],
                        "title": lib["title"],
                        "url": server_url,
                        "token": server_token,
                        "server_type": server_type,
                    }
                )
                logger.debug(
                    f'[LIB] {server_type.capitalize()} | "{lib["title"]}" (ID: {lib["key"]}) at {location}'
                )

    return library_ids


def _get_library_roots():
    roots = []
    for libraries in _library_cache.values():
        for library in libraries:
            for location in library.get("locations", []):
                roots.append(os.path.normpath(location))
    return roots


def get_library_id_for_path(file_path):
    """Get the library section ID and server info for a given file path."""
    best_match = None
    best_match_length = 0

    # Check all media servers
    for server_info in media_servers:
        server_type = server_info["type"]
        server_url = server_info["url"]
        server_token = server_info["token"]

        try:
            # Use cached libraries if available, otherwise fetch
            if server_url in _library_cache:
                libraries = _library_cache[server_url]
            else:
                libraries = []
                if server_type == "plex":
                    libraries = get_libraries_plex(server_info)
                elif server_type == "jellyfin":
                    libraries = get_libraries_jellyfin(server_info)
                elif server_type == "emby":
                    libraries = get_libraries_emby(server_info)

            # Find matching sections
            for lib in libraries:
                for location_path in lib["locations"]:
                    # Normalize paths for comparison
                    normalized_scan_path = os.path.normpath(file_path)
                    normalized_location = os.path.normpath(location_path)

                    # Check if the file path is inside the library location.
                    if _is_path_in_library(normalized_scan_path, normalized_location):
                        # Use the longest matching path (most specific)
                        if len(normalized_location) > best_match_length:
                            best_match = {
                                "section_id": lib["key"],
                                "section_title": lib["title"],
                                "server_url": server_url,
                                "token": server_token,
                                "server_type": server_type,
                            }
                            best_match_length = len(normalized_location)
        except Exception as e:
            logger.debug(
                f"[FAIL] Error checking {server_type} {server_url} for path {file_path}: {str(e)}"
            )
            continue

    if best_match:
        logger.debug(
            f"[LIB] Matched section: {best_match['section_title']} (ID: {best_match['section_id']}) on {best_match['server_type']} {best_match['server_url']}"
        )
        return best_match

    logger.warning(f"[WARN] No matching library for: {file_path}")
    return None


def _extract_title_from_path(file_path):
    """Extract the media title from the folder structure.

    Movies:  /library/Title (Year) {ids}/file.mkv -> parent folder
    TV:      /library/Title (Year)/Season XX/file.mkv -> grandparent folder

    Extracts text before the first '(Year)' pattern.
    Falls back to filename without extension if no match.
    """
    import re

    # Try parent folder first (movies)
    parent = os.path.basename(os.path.dirname(file_path))
    match = re.match(r"^(.*?)\s*\(\d{4}\)", parent)
    if match:
        return match.group(1).strip()

    # Try grandparent folder (TV: .../Show (Year)/Season XX/file.mkv)
    grandparent = os.path.basename(os.path.dirname(os.path.dirname(file_path)))
    match = re.match(r"^(.*?)\s*\(\d{4}\)", grandparent)
    if match:
        return match.group(1).strip()

    return os.path.splitext(os.path.basename(file_path))[0]


def _short_search_term(title, max_words=3):
    """Shorten a title to the first few words for fuzzy API searches.

    Jellyfin/Emby SearchTerm works best with short queries; the exact
    file-path match afterwards ensures correctness.
    """
    words = title.split()
    return " ".join(words[:max_words]) if len(words) > max_words else title


def _search_term_variants(term):
    """Return search term variants to handle common mismatches.

    Handles: and/&, stripped apostrophes (Youre->You're, Its->It's, etc.)
    """
    import re

    variants = [term]

    # and / & variants
    if " and " in term.lower():
        variants.append(term.replace(" and ", " & ").replace(" And ", " & "))
    elif " & " in term:
        variants.append(term.replace(" & ", " and "))

    # Apostrophe variants: restore common contractions stripped from folder names
    apostrophe_map = {
        r"\b(\w+)re\b": r"\1're",  # Youre -> You're
        r"\b(\w+)nt\b": r"\1n't",  # Doesnt -> Doesn't, Dont -> Don't
        r"\b(\w+)ts\b": r"\1t's",  # Its -> It's, Whats -> What's
        r"\bIts\b": "It's",
    }
    for pattern, replacement in apostrophe_map.items():
        result = re.sub(pattern, replacement, term)
        if result != term and result not in variants:
            variants.append(result)

    # Possessive: Antonias -> Antonia's, Writers -> Writer's
    poss = re.sub(r"\b(\w+[^s])s\b", r"\1's", term)
    if poss != term and poss not in variants:
        variants.append(poss)

    return variants


def _check_plex_parts(xml_content, file_path):
    """Check if a file path matches any Part in Plex XML response."""
    root = ET.fromstring(xml_content)
    normalized = os.path.normpath(file_path)
    for part in root.iter("Part"):
        part_file = part.get("file")
        if part_file and os.path.normpath(part_file) == normalized:
            return True
    return False


def check_file_plex(file_path, library_info, server_info):
    """Check if a file exists in a Plex server.

    Strategy:
    1. Try file= filter with parent folder path (works for movies, fast & exact)
    2. Fall back to title search with progressive broadening (needed for TV shows)
    """
    try:
        section_id = library_info["section_id"]
        server_url = server_info["url"]
        token = server_info["token"]
        headers = {"X-Plex-Token": token}
        url = f"{server_url}/library/sections/{section_id}/all"

        # 1. Try path-based search first (works for movies)
        parent_folder = os.path.dirname(file_path)
        response = _request_with_retry(
            requests.get,
            url,
            headers=headers,
            params={"file": parent_folder},
            timeout=15,
        )
        if response is not None and _check_plex_parts(response.content, file_path):
            return True

        # 2. Fall back to title search (needed for TV shows where file= doesn't work)
        full_title = _extract_title_from_path(file_path)
        skip_words = {"the", "a", "an", "of", "in", "on", "at", "to", "for", "is", "it"}
        words = full_title.split()
        search_attempts = []
        for length in [min(3, len(words)), min(2, len(words)), 1]:
            term = " ".join(words[:length])
            if length == 1 and term.lower() in skip_words:
                continue
            for variant in _search_term_variants(term):
                if variant not in search_attempts:
                    search_attempts.append(variant)

        for search_title in search_attempts:
            response = _request_with_retry(
                requests.get,
                url,
                headers=headers,
                params={"title": search_title},
                timeout=15,
            )
            if response is None:
                continue

            # Check Video entries (movies or episodes in some cases)
            if _check_plex_parts(response.content, file_path):
                return True

            # TV shows return Directory entries — fetch episodes via /allLeaves
            root = ET.fromstring(response.content)
            for directory in root.iter("Directory"):
                rating_key = directory.get("ratingKey")
                if not rating_key:
                    continue
                leaves_url = f"{server_url}/library/metadata/{rating_key}/allLeaves"
                leaves_response = _request_with_retry(
                    requests.get, leaves_url, headers=headers, timeout=15
                )
                if leaves_response and _check_plex_parts(
                    leaves_response.content, file_path
                ):
                    return True

        return False
    except Exception as e:
        logger.error(f"[FAIL] Plex | File check error: {str(e)}")
        return False


def _check_file_emby_api(file_path, library_info, server_info, server_label):
    """Shared file check for Jellyfin and Emby (same API)."""
    try:
        url = f"{server_info['url']}/Items"
        headers = {"X-Emby-Token": server_info["token"]}
        title = _extract_title_from_path(file_path)

        # Progressive broadening: 3 words, 2 words, 1 word (with &/and variants)
        skip_words = {"the", "a", "an", "of", "in", "on", "at", "to", "for", "is", "it"}
        words = title.split()
        search_attempts = []
        for length in [min(3, len(words)), min(2, len(words)), 1]:
            term = " ".join(words[:length])
            if length == 1 and term.lower() in skip_words:
                continue
            for variant in _search_term_variants(term):
                if variant not in search_attempts:
                    search_attempts.append(variant)

        section_id = library_info.get("section_id")
        normalized_file_path = os.path.normpath(file_path)

        for search_term in search_attempts:
            params = {
                "Recursive": "true",
                "IncludeItemTypes": "Movie,Episode",
                "Fields": "Path,MediaSources",
                "SearchTerm": search_term,
            }
            if section_id:
                params["ParentId"] = section_id

            response = _request_with_retry(
                requests.get, url, headers=headers, params=params, timeout=10
            )
            response.raise_for_status()
            data = response.json()

            for item in data.get("Items", []):
                if "Path" in item:
                    if os.path.normpath(item["Path"]) == normalized_file_path:
                        return True
                if "MediaSources" in item:
                    for media_source in item["MediaSources"]:
                        if "Path" in media_source:
                            if (
                                os.path.normpath(media_source["Path"])
                                == normalized_file_path
                            ):
                                return True

        return False
    except Exception as e:
        logger.error(f"[FAIL] {server_label} | File check error: {str(e)}")
        return False


def check_file_jellyfin(file_path, library_info, server_info):
    """Check if a file exists in a Jellyfin server using the bulk path cache."""
    server_url = server_info["url"]
    if server_url not in _server_path_caches:
        _server_path_caches[server_url] = _build_server_path_cache(
            server_info, "Jellyfin"
        )
    return os.path.normpath(file_path) in _server_path_caches[server_url]


def check_file_emby(file_path, library_info, server_info):
    """Check if a file exists in an Emby server using the bulk path cache."""
    server_url = server_info["url"]
    if server_url not in _server_path_caches:
        _server_path_caches[server_url] = _build_server_path_cache(server_info, "Emby")
    return os.path.normpath(file_path) in _server_path_caches[server_url]


def _get_cached_server_item(server_url, file_path):
    return _server_item_caches.get(server_url, {}).get(os.path.normpath(file_path))


def _item_has_incomplete_metadata(item):
    if not item:
        return False

    provider_ids = item.get("ProviderIds") or {}
    return (
        not provider_ids
        and not item.get("ProductionYear")
        and not item.get("PremiereDate")
    )


def _metadata_refresh_request(file_path, server_info, item):
    item_id = item.get("Id")
    if not item_id:
        return None

    signature = state_cache.file_signature(file_path)
    if not signature:
        return None

    size, mtime_ns = signature
    return {
        "server_type": server_info["type"],
        "server_url": server_info["url"],
        "token": server_info["token"],
        "item_id": item_id,
        "file_path": file_path,
        "size": size,
        "mtime_ns": mtime_ns,
    }


def _is_path_in_library(file_path, library_location):
    normalized_file_path = os.path.normcase(os.path.normpath(file_path))
    normalized_location = os.path.normcase(os.path.normpath(library_location))
    try:
        return (
            os.path.commonpath([normalized_file_path, normalized_location])
            == normalized_location
        )
    except ValueError:
        return False


def _is_path_parent_of(parent_path, child_path):
    normalized_parent = os.path.normcase(os.path.normpath(parent_path))
    normalized_child = os.path.normcase(os.path.normpath(child_path))
    try:
        return (
            os.path.commonpath([normalized_parent, normalized_child])
            == normalized_parent
        )
    except ValueError:
        return False


def _should_walk_path(path, library_roots):
    if not library_roots:
        return True

    return any(
        _is_path_parent_of(path, library_root) or _is_path_parent_of(library_root, path)
        for library_root in library_roots
    )


def _skip_unmatched_library_status(server_info, file_path):
    server_type = server_info["type"]
    server_url = server_info["url"]
    logger.debug(
        f"[SKIP] {server_type.capitalize()} | No matching library for: {file_path}"
    )
    return {
        "server_type": server_type,
        "server_url": server_url,
        "found": False,
        "skipped": True,
        "library_info": None,
        "token": server_info["token"],
        "metadata_refresh": None,
    }


def check_file_in_all_servers(file_path):
    """Check if a file exists in all media servers and return status for each server.

    Returns:
        dict: {
            'found_anywhere': bool,
            'server_status': [
                {
                    'server_type': str,
                    'server_url': str,
                    'found': bool,
                    'skipped': bool,
                    'library_info': dict or None  # Library info if found, or best matching library if not found
                },
                ...
            ]
        }
    """
    # Check cache first for this specific file
    cache_key = f"server_status:{file_path}"
    with directory_cache_lock:
        cached_result = directory_cache.get(cache_key)
    if cached_result:
        if isinstance(cached_result, dict) and "found_anywhere" in cached_result:
            logger.debug(f"[CACHE] Hit for: {os.path.basename(file_path)}")
            return cached_result

    server_status_list = []
    found_anywhere = False

    # Check ALL servers to see if file exists
    for server_info in media_servers:
        server_type = server_info["type"]
        server_url = server_info["url"]
        found_on_this_server = False
        library_info_for_scan = None
        metadata_refresh = None

        try:
            if server_type == "plex":
                time.sleep(0.05)  # rate-limit Plex API calls
                # Plex requires checking each library individually
                # First, find the best matching library based on path
                libraries = _library_cache.get(server_url) or get_libraries_plex(
                    server_info
                )
                normalized_file_path = os.path.normpath(file_path)
                best_match = None
                best_match_length = 0

                # Find best matching library based on path
                for lib in libraries:
                    for location in lib.get("locations", []):
                        if _is_path_in_library(normalized_file_path, location):
                            normalized_location = os.path.normpath(location)
                            normalized_location_key = os.path.normcase(
                                normalized_location
                            )
                            if len(normalized_location_key) > best_match_length:
                                best_match = {
                                    "section_id": lib["key"],
                                    "section_title": lib["title"],
                                    "server_url": server_url,
                                    "token": server_info["token"],
                                    "server_type": server_type,
                                }
                                best_match_length = len(normalized_location_key)

                if best_match:
                    library_info_for_scan = best_match
                else:
                    server_status_list.append(
                        _skip_unmatched_library_status(server_info, file_path)
                    )
                    continue

                # Check the best matching library first
                if library_info_for_scan:
                    filename = os.path.basename(file_path)
                    logger.debug(
                        f"[OK] {server_type.capitalize()} | {library_info_for_scan['section_title']} | {filename}"
                    )

                    search_start = time.time()
                    found_here = check_file_plex(
                        file_path, library_info_for_scan, server_info
                    )
                    search_duration = time.time() - search_start

                    if found_here:
                        found_on_this_server = True
                        found_anywhere = True
                        logger.debug(
                            f"[OK] {server_type.capitalize()} | {library_info_for_scan['section_title']} | {filename} ({search_duration:.2f}s)"
                        )
                    else:
                        logger.debug(
                            f"[MISS] {server_type.capitalize()} | {library_info_for_scan['section_title']} | {filename} ({search_duration:.2f}s)"
                        )
            elif server_type in ["jellyfin", "emby"]:
                # Jellyfin/Emby search globally, so we only need one call per server
                # First, get the best matching library for this path
                best_match = None
                if server_url in _library_cache:
                    libraries = _library_cache[server_url]
                else:
                    libraries = (
                        get_libraries_jellyfin(server_info)
                        if server_type == "jellyfin"
                        else get_libraries_emby(server_info)
                    )

                # Find best matching library based on path
                normalized_file_path = os.path.normpath(file_path)
                best_match_length = 0
                for lib in libraries:
                    for location in lib.get("locations", []):
                        if _is_path_in_library(normalized_file_path, location):
                            normalized_location = os.path.normpath(location)
                            normalized_location_key = os.path.normcase(
                                normalized_location
                            )
                            if len(normalized_location_key) > best_match_length:
                                best_match = {
                                    "section_id": lib["key"],
                                    "section_title": lib["title"],
                                    "server_url": server_url,
                                    "token": server_info["token"],
                                    "server_type": server_type,
                                }
                                best_match_length = len(normalized_location_key)

                if not best_match:
                    server_status_list.append(
                        _skip_unmatched_library_status(server_info, file_path)
                    )
                    continue

                if server_url in _failed_cache_servers:
                    server_status_list.append(
                        _skip_unmatched_library_status(server_info, file_path)
                    )
                    continue

                library_info = best_match

                filename = os.path.basename(file_path)
                library_name = library_info.get("section_title", "All Libraries")
                logger.debug(
                    f"[OK] {server_type.capitalize()} | {library_name} | {filename}"
                )

                search_start = time.time()
                if server_type == "jellyfin":
                    found_here = check_file_jellyfin(
                        file_path, library_info, server_info
                    )
                else:
                    found_here = check_file_emby(file_path, library_info, server_info)
                search_duration = time.time() - search_start

                if found_here:
                    found_on_this_server = True
                    found_anywhere = True
                    library_info_for_scan = library_info
                    if METADATA_REPAIR_ENABLED and server_type in [
                        "jellyfin",
                        "emby",
                    ]:
                        item = _get_cached_server_item(server_url, file_path)
                        if _item_has_incomplete_metadata(item):
                            metadata_refresh = _metadata_refresh_request(
                                file_path, server_info, item
                            )
                    logger.debug(
                        f"[OK] {server_type.capitalize()} | {library_name} | {filename} ({search_duration:.2f}s)"
                    )
                else:
                    library_info_for_scan = library_info
                    logger.debug(
                        f"[MISS] {server_type.capitalize()} | {library_name} | {filename} ({search_duration:.2f}s)"
                    )

            # Store status for this server
            server_status_list.append(
                {
                    "server_type": server_type,
                    "server_url": server_url,
                    "found": found_on_this_server,
                    "skipped": False,
                    "library_info": library_info_for_scan,
                    "token": server_info["token"],
                    "metadata_refresh": metadata_refresh,
                }
            )

        except Exception as e:
            logger.debug(
                f"[FAIL] Error checking {server_type} {server_url} for file {file_path}: {str(e)}"
            )
            # Still add to status list even if error occurred
            server_status_list.append(
                {
                    "server_type": server_type,
                    "server_url": server_url,
                    "found": False,
                    "skipped": False,
                    "library_info": None,
                    "token": server_info["token"],
                    "metadata_refresh": None,
                }
            )
            continue

    result = {"found_anywhere": found_anywhere, "server_status": server_status_list}

    # Cache the result (cap size to prevent unbounded growth)
    with directory_cache_lock:
        if len(directory_cache) >= 50000:
            directory_cache.clear()
        directory_cache[cache_key] = result

    return result


def is_in_media_server(file_path):
    """Check if a file exists in ANY media server using direct file search.

    This is a convenience wrapper that returns True/False for backward compatibility.
    """
    result = check_file_in_all_servers(file_path)
    return result["found_anywhere"]


def scan_folder_plex(library_id, folder_path, server_url, token):
    """Trigger a library scan for a specific folder on a Plex server."""
    library_id = str(library_id)
    encoded_path = quote(folder_path)
    url = f"{server_url}/library/sections/{library_id}/refresh?path={encoded_path}"
    headers = {"X-Plex-Token": token}

    scan_start = time.time()
    try:
        response = _request_with_retry(requests.get, url, headers=headers, timeout=30)
        scan_duration = time.time() - scan_start

        if response.status_code == 200:
            logger.info(f"[OK] Plex | Scan completed ({scan_duration:.2f}s)")
        else:
            logger.warning(
                f"[WARN] Plex | Scan returned status {response.status_code} ({scan_duration:.2f}s)"
            )
    except requests.exceptions.RequestException as e:
        scan_duration = time.time() - scan_start
        logger.error(f"[FAIL] Plex | Scan failed: {str(e)} ({scan_duration:.2f}s)")


def scan_folder_jellyfin_emby(library_id, folder_paths, server_url, token, server_type):
    """Trigger folder scans on a Jellyfin or Emby server using the Media/Updated endpoint."""
    url = f"{server_url}/Library/Media/Updated"
    headers = {"X-Emby-Token": token, "Content-Type": "application/json"}

    if isinstance(folder_paths, str):
        folder_paths = [folder_paths]
    payload = {"Updates": [{"Path": p, "UpdateType": "Modified"} for p in folder_paths]}

    scan_start = time.time()
    try:
        response = _request_with_retry(
            requests.post, url, headers=headers, json=payload, timeout=30
        )
        scan_duration = time.time() - scan_start

        if response.status_code in [200, 204]:
            logger.info(
                f"[OK] {server_type.capitalize()} | Scan completed ({scan_duration:.2f}s)"
            )
            return True
        else:
            logger.warning(
                f"[WARN] {server_type.capitalize()} | Scan returned status {response.status_code} ({scan_duration:.2f}s)"
            )
            return False
    except requests.exceptions.RequestException as e:
        scan_duration = time.time() - scan_start
        logger.error(
            f"[FAIL] {server_type.capitalize()} | Scan failed: {str(e)} ({scan_duration:.2f}s)"
        )
        return False


def scan_folder(library_id, folder_path, server_url, token, server_type):
    """Trigger a library scan for a specific folder on a media server."""
    if server_type == "plex":
        scan_folder_plex(library_id, folder_path, server_url, token)
    elif server_type in ["jellyfin", "emby"]:
        scan_folder_jellyfin_emby(
            library_id, folder_path, server_url, token, server_type
        )
    else:
        logger.warning(f"[WARN] Unknown server type: {server_type}")
        return


def refresh_item_metadata(refresh_request):
    server_type = refresh_request["server_type"]
    if server_type not in ["jellyfin", "emby"]:
        return False

    item_id = refresh_request["item_id"]
    url = f"{refresh_request['server_url']}/Items/{item_id}/Refresh"
    headers = {"X-Emby-Token": refresh_request["token"]}
    params = {
        "metadataRefreshMode": "FullRefresh",
        "imageRefreshMode": "Default",
        "replaceAllMetadata": "false",
        "replaceAllImages": "false",
    }

    refresh_start = time.time()
    try:
        response = _request_with_retry(
            requests.post, url, headers=headers, params=params, timeout=30
        )
        refresh_duration = time.time() - refresh_start
        if response.status_code in [200, 204]:
            logger.info(
                f"[OK] {server_type.capitalize()} | Metadata refresh queued "
                f"({refresh_duration:.2f}s)"
            )
            state_cache.mark_metadata_refresh_processed(refresh_request)
            return True

        logger.warning(
            f"[WARN] {server_type.capitalize()} | Metadata refresh returned status "
            f"{response.status_code} ({refresh_duration:.2f}s)"
        )
    except requests.exceptions.RequestException as e:
        refresh_duration = time.time() - refresh_start
        logger.error(
            f"[FAIL] {server_type.capitalize()} | Metadata refresh failed: {str(e)} "
            f"({refresh_duration:.2f}s)"
        )

    return False


def _queue_metadata_refresh(pending_refreshes, refresh_request):
    if not isinstance(refresh_request, dict):
        return refresh_request or "none"

    server_status = {
        "server_type": refresh_request["server_type"],
        "server_url": refresh_request["server_url"],
    }
    signature = (refresh_request["size"], refresh_request["mtime_ns"])
    now = time.time()
    if state_cache.recent_metadata_refresh_applies(
        refresh_request["file_path"],
        server_status,
        refresh_request["item_id"],
        signature,
        now,
    ):
        return "cooldown"

    key = (
        refresh_request["server_type"],
        refresh_request["server_url"],
        refresh_request["item_id"],
    )
    if key in pending_refreshes:
        return "pending"

    pending_refreshes[key] = refresh_request
    state_cache.mark_metadata_refresh_queued(
        refresh_request["file_path"],
        server_status,
        refresh_request["item_id"],
        signature,
        now,
    )
    return "queued"


def process_pending_metadata_refreshes(pending_refreshes):
    processed = 0

    for refresh_request in pending_refreshes.values():
        if _shutdown_requested:
            logger.info("[SHUTDOWN] Metadata refreshes aborted cleanly")
            break

        server_name = refresh_request["server_type"].capitalize()
        logger.info(
            f"[REFRESH] {server_name} | Metadata | "
            f"{os.path.basename(refresh_request['file_path'])}"
        )
        if refresh_item_metadata(refresh_request):
            processed += 1

    return processed


def _queue_scan_request(pending_scans, server_status, parent_folder, file_path=None):
    library_info = server_status["library_info"]
    section_id = library_info.get("section_id") if library_info else None

    if server_status["server_type"] == "plex" and not section_id:
        return "missing_library"

    key = (
        server_status["server_type"],
        server_status["server_url"],
        parent_folder,
    )

    signature = None
    now = time.time()
    if file_path:
        signature = state_cache.file_signature(file_path)

    if key in pending_scans:
        state_cache.record_missing_file(
            file_path, server_status, parent_folder, signature, now
        )
        return "pending"

    if state_cache.recent_repair_scan_applies(
        file_path, server_status, parent_folder, signature, now
    ):
        state_cache.record_missing_file(
            file_path, server_status, parent_folder, signature, now
        )
        return "cooldown"

    pending_scans[key] = {
        "section_id": section_id or "",
        "folder_path": parent_folder,
        "server_url": server_status["server_url"],
        "token": server_status["token"],
        "server_type": server_status["server_type"],
    }
    state_cache.mark_scan_queued(pending_scans[key], now)
    state_cache.record_missing_file(
        file_path, server_status, parent_folder, signature, now
    )
    return "queued"


def process_pending_scans(pending_scans):
    processed = 0
    batches_sent = 0

    plex_scans = [r for r in pending_scans.values() if r["server_type"] == "plex"]
    jf_scans = [
        r for r in pending_scans.values() if r["server_type"] in ["jellyfin", "emby"]
    ]

    # Plex: one folder at a time with SCAN_INTERVAL (unchanged)
    for scan_request in plex_scans:
        if _shutdown_requested:
            logger.info("[SHUTDOWN] Pending scans aborted cleanly")
            return processed, batches_sent
        server_name = scan_request["server_type"].capitalize()
        logger.info(f"[SCAN] {server_name} | {scan_request['folder_path']}")
        scan_folder(
            scan_request["section_id"],
            scan_request["folder_path"],
            scan_request["server_url"],
            scan_request["token"],
            scan_request["server_type"],
        )
        state_cache.mark_scan_processed(scan_request)
        processed += 1
        if SCAN_INTERVAL > 0:
            logger.info(f"[WAIT] {SCAN_INTERVAL}s before next scan")
            time.sleep(SCAN_INTERVAL)

    # Jellyfin/Emby: batch by server, chunk by BATCH_SIZE
    by_server = defaultdict(list)
    for r in jf_scans:
        by_server[(r["server_url"], r["server_type"], r["token"])].append(r)

    for (server_url, server_type, token), scan_requests in by_server.items():
        server_name = server_type.capitalize()
        total_batches = (len(scan_requests) + BATCH_SIZE - 1) // BATCH_SIZE
        for batch_num, i in enumerate(
            range(0, len(scan_requests), BATCH_SIZE), start=1
        ):
            if _shutdown_requested:
                logger.info("[SHUTDOWN] Pending scans aborted cleanly")
                return processed, batches_sent
            chunk = scan_requests[i : i + BATCH_SIZE]
            folder_paths = [r["folder_path"] for r in chunk]
            logger.info(
                f"[BATCH] {server_name} | {len(folder_paths)} folders "
                f"(batch {batch_num}/{total_batches}) -> {server_url}"
            )
            ok = scan_folder_jellyfin_emby(
                "", folder_paths, server_url, token, server_type
            )
            if ok:
                for r in chunk:
                    state_cache.mark_scan_processed(r)
                processed += len(chunk)
            else:
                logger.warning(
                    f"[WARN] {server_name} | Batch {batch_num}/{total_batches} failed — will retry next cycle"
                )
            batches_sent += 1
            if batch_num < total_batches and BATCH_DELAY > 0:
                logger.info(f"[WAIT] {BATCH_DELAY}s before next batch")
                time.sleep(BATCH_DELAY)

    return processed, batches_sent


def is_broken_symlink(file_path):
    """Check if a path is a broken symlink and return the target if broken."""
    if not os.path.islink(file_path):
        return False, None
    target = os.readlink(file_path)
    if not os.path.exists(file_path):
        return True, target
    return False, None


def _get_walk_roots(scan_path, library_roots):
    if not library_roots:
        return [scan_path]

    roots = []
    for library_root in library_roots:
        if _is_path_parent_of(scan_path, library_root):
            roots.append(library_root)
        elif _is_path_parent_of(library_root, scan_path):
            roots.append(scan_path)

    deduped_roots = []
    seen = set()
    for root in sorted(roots, key=len):
        normalized_root = os.path.normcase(os.path.normpath(root))
        if normalized_root in seen:
            continue
        if any(_is_path_parent_of(existing, root) for existing in deduped_roots):
            continue
        seen.add(normalized_root)
        deduped_roots.append(root)
    return deduped_roots


def _log_pruned_scan_children(scan_path, walk_roots):
    try:
        children = os.listdir(scan_path)
    except OSError:
        return 0

    pruned = 0
    for child in children:
        child_path = os.path.join(scan_path, child)
        if not os.path.isdir(child_path):
            continue
        if any(_is_path_parent_of(child_path, root) for root in walk_roots):
            continue
        if any(_is_path_parent_of(root, child_path) for root in walk_roots):
            continue
        pruned += 1
        logger.info(f"[SKIP] Pruned non-library directory: {child_path}")
    return pruned


def _scan_walk_root(walk_root):
    result = {
        "files": 0,
        "directories": 0,
        "broken_symlinks": 0,
        "missing_events": [],
        "metadata_events": [],
        "missing_items": [],
        "warnings": [],
        "skipped_no_library": 0,
        "metadata_cooldown_skips": 0,
    }

    for root, dirs, files in os.walk(walk_root):
        result["directories"] += 1
        media_files_in_dir = 0

        if SYMLINK_CHECK:
            for d in dirs[:]:
                dir_path = os.path.join(root, d)
                broken, target = is_broken_symlink(dir_path)
                if broken:
                    target_info = f" -> {target}" if target else ""
                    logger.warning(f"[SKIP] Broken directory symlink: {d}{target_info}")
                    result["broken_symlinks"] += 1
                    dirs.remove(d)

        for file in files:
            if file.startswith("."):
                continue

            file_ext = os.path.splitext(file)[1].lower()
            if file_ext not in MEDIA_EXTENSIONS:
                continue

            result["files"] += 1
            media_files_in_dir += 1
            file_path = os.path.join(root, file)

            if SYMLINK_CHECK:
                broken, target = is_broken_symlink(file_path)
                if broken:
                    target_info = f" -> {target}" if target else ""
                    logger.warning(
                        f"[SKIP] Broken symlink: {os.path.basename(file_path)}"
                        f"{target_info}"
                    )
                    result["broken_symlinks"] += 1
                    continue

            if result["files"] % 100 == 0:
                logger.info(
                    f"[PROGRESS] {result['files']} files checked in {walk_root}"
                )

            file_status = check_file_in_all_servers(file_path)
            filename = os.path.basename(file_path)
            missing_servers = [
                s
                for s in file_status["server_status"]
                if not s["found"] and not s.get("skipped")
            ]
            skipped_servers = [
                s for s in file_status["server_status"] if s.get("skipped")
            ]

            for server_status in file_status["server_status"]:
                refresh_request = server_status.get("metadata_refresh")
                if refresh_request == "cooldown":
                    result["metadata_cooldown_skips"] += 1
                elif refresh_request:
                    result["metadata_events"].append(refresh_request)

            if missing_servers:
                if not file_status["found_anywhere"]:
                    library_info = get_library_id_for_path(file_path)
                    if library_info and library_info.get("section_title"):
                        result["missing_items"].append(
                            (library_info["section_title"], file_path)
                        )
                        logger.debug(f"[MISS] Not indexed on any server: {filename}")

                parent_folder = os.path.dirname(file_path)
                for server_status in missing_servers:
                    if server_status["library_info"]:
                        result["missing_events"].append(
                            (server_status, parent_folder, file_path)
                        )
                    else:
                        warning_msg = (
                            f"[WARN] Could not determine library for {filename} on "
                            f"{server_status['server_type']}"
                        )
                        logger.warning(warning_msg)
                        result["warnings"].append(warning_msg)
            elif skipped_servers:
                result["skipped_no_library"] += 1
                logger.debug(f"[SKIP] No matching library on all servers: {filename}")
            else:
                logger.debug(f"[OK] Exists on all servers: {filename}")

        if media_files_in_dir > 0:
            logger.debug(
                f"[OK] Directory done: {os.path.basename(root)} "
                f"({media_files_in_dir} files)"
            )

    return result


def run_scan():
    """Main scan logic."""
    stats = RunStats()
    scan_start_time = time.time()

    # Clear directory cache at the start of a new scan
    with directory_cache_lock:
        directory_cache.clear()
    _server_path_caches.clear()
    _server_item_caches.clear()
    _failed_cache_servers.clear()
    logger.info("--- SCAN CYCLE START ---")

    lib_attempt = 0
    while True:
        library_ids = get_library_ids()
        if library_ids:
            break
        lib_attempt += 1
        unlimited = CACHE_RETRY_ATTEMPTS == 0
        if not unlimited and lib_attempt >= CACHE_RETRY_ATTEMPTS:
            error_msg = "Could not find any libraries in any media server."
            logger.error(f"[FAIL] {error_msg}")
            stats.add_error(error_msg)
            asyncio.run(stats.send_discord_summary())
            return
        attempt_label = (
            f"{lib_attempt}/∞" if unlimited else f"{lib_attempt}/{CACHE_RETRY_ATTEMPTS}"
        )
        logger.warning(
            f"[WARN] No libraries found (attempt {attempt_label}), retrying in {CACHE_RETRY_WAIT}s"
        )
        time.sleep(CACHE_RETRY_WAIT)

    server_counts = {}
    for server in media_servers:
        server_type = server["type"]
        server_counts[server_type] = server_counts.get(server_type, 0) + 1

    server_summary = ", ".join(
        [f"{count} {server_type}" for server_type, count in server_counts.items()]
    )
    logger.info(f"Found {len(library_ids)} libraries across {server_summary}")
    library_roots = _get_library_roots()
    for server_info in media_servers:
        if server_info["type"] not in ["jellyfin", "emby"]:
            continue
        label = server_info["type"].capitalize()
        if not _ping_server(server_info):
            logger.warning(
                f"[WARN] {label} | Server not reachable at {server_info['url']} — skipping cache build"
            )
            _failed_cache_servers.add(server_info["url"])
        else:
            _server_path_caches[server_info["url"]] = _build_server_path_cache(
                server_info, label
            )
    attempt = 0
    limit_label = str(CACHE_RETRY_ATTEMPTS) if CACHE_RETRY_ATTEMPTS > 0 else "∞"
    while _failed_cache_servers and not _shutdown_requested:
        if CACHE_RETRY_ATTEMPTS > 0 and attempt >= CACHE_RETRY_ATTEMPTS:
            break
        attempt += 1
        logger.warning(
            f"[WARN] Cache failed for {len(_failed_cache_servers)} server(s) — "
            f"retry {attempt}/{limit_label} in {CACHE_RETRY_WAIT}s"
        )
        time.sleep(CACHE_RETRY_WAIT)
        if _shutdown_requested:
            break
        retry_servers = list(_failed_cache_servers)
        _failed_cache_servers.clear()
        for server_info in media_servers:
            if server_info["url"] not in retry_servers:
                continue
            label = server_info["type"].capitalize()
            logger.info(
                f"[CACHE] {label} | Retrying path cache for {server_info['url']} "
                f"(attempt {attempt}/{limit_label})"
            )
            if not _ping_server(server_info):
                logger.warning(
                    f"[WARN] {label} | Still not reachable at {server_info['url']}"
                )
                _failed_cache_servers.add(server_info["url"])
            else:
                _server_path_caches[server_info["url"]] = _build_server_path_cache(
                    server_info, label
                )
        recovered = [u for u in retry_servers if u not in _failed_cache_servers]
        for url in recovered:
            msg = f"Cache recovered for {url} after {attempt} {'retry' if attempt == 1 else 'retries'} — scan proceeding"
            logger.info(f"[OK] {msg}")
            asyncio.run(_send_discord_alert(msg, color=Color.green()))
    for url in _failed_cache_servers:
        retry_label = f"after {attempt} retries" if attempt else "on first attempt"
        error_msg = (
            f"Cache failed for {url} {retry_label} — "
            f"files will not be checked against this server this cycle"
        )
        logger.warning(f"[WARN] {error_msg}")
        stats.add_error(error_msg)
    pending_scans = {}
    pending_metadata_refreshes = {}
    pruned_directories = 0
    skipped_no_library = 0
    cooldown_skipped_scans = 0
    metadata_cooldown_skipped = 0
    total_files_found = 0
    total_directories_searched = 0

    for SCAN_PATH in SCAN_PATHS:
        if _shutdown_requested:
            logger.info("[SHUTDOWN] Scan aborted cleanly")
            break
        logger.info(f"--- Scanning: {SCAN_PATH} ---")

        if not os.path.isdir(SCAN_PATH):
            error_msg = f"Directory not found: {SCAN_PATH}"
            logger.error(f"[FAIL] {error_msg}")
            stats.add_error(error_msg)
            continue

        if not _should_walk_path(SCAN_PATH, library_roots):
            logger.info(f"[SKIP] No configured library under scan path: {SCAN_PATH}")
            continue

        walk_roots = _get_walk_roots(SCAN_PATH, library_roots)
        pruned_directories += _log_pruned_scan_children(SCAN_PATH, walk_roots)

        if not walk_roots:
            logger.info(f"[SKIP] No configured library under scan path: {SCAN_PATH}")
            continue

        files_in_path = 0
        directories_in_path = 0
        worker_count = max(1, min(LIBRARY_WORKERS, len(walk_roots)))
        logger.info(
            f"[WORKERS] Scanning {len(walk_roots)} libraries with {worker_count} workers"
        )

        if worker_count == 1:
            scan_results = [_scan_walk_root(walk_roots[0])]
        else:
            scan_results = []
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                future_to_root = {
                    executor.submit(_scan_walk_root, walk_root): walk_root
                    for walk_root in walk_roots
                }
                for future in as_completed(future_to_root):
                    walk_root = future_to_root[future]
                    try:
                        scan_results.append(future.result())
                    except Exception as e:
                        error_msg = f"Library worker failed for {walk_root}: {e}"
                        logger.error(f"[FAIL] {error_msg}", exc_info=True)
                        stats.add_error(error_msg)

        missing_events_by_folder = defaultdict(list)
        metadata_refresh_count = 0
        for scan_result in scan_results:
            files_in_path += scan_result["files"]
            directories_in_path += scan_result["directories"]
            skipped_no_library += scan_result["skipped_no_library"]
            metadata_cooldown_skipped += scan_result["metadata_cooldown_skips"]
            for _ in range(scan_result["files"]):
                stats.increment_scanned()
            for _ in range(scan_result["broken_symlinks"]):
                stats.increment_broken_symlinks()
            for section_title, file_path in scan_result["missing_items"]:
                stats.add_missing_item(section_title, file_path)
            for warning_msg in scan_result["warnings"]:
                stats.add_warning(warning_msg)
            for server_status, parent_folder, file_path in scan_result[
                "missing_events"
            ]:
                key = (
                    server_status["server_type"],
                    server_status["server_url"],
                    parent_folder,
                )
                missing_events_by_folder[key].append(
                    (server_status, parent_folder, file_path)
                )
            for refresh_request in scan_result["metadata_events"]:
                queue_result = _queue_metadata_refresh(
                    pending_metadata_refreshes, refresh_request
                )
                if queue_result == "queued":
                    metadata_refresh_count += 1
                elif queue_result == "cooldown":
                    metadata_cooldown_skipped += 1

        for events in missing_events_by_folder.values():
            server_status, parent_folder, _ = events[0]
            server_name = server_status["server_type"].capitalize()
            queued = False
            cooldowns = 0
            for _, _, file_path in events:
                queue_result = _queue_scan_request(
                    pending_scans, server_status, parent_folder, file_path
                )
                if queue_result == "queued":
                    queued = True
                elif queue_result == "cooldown":
                    cooldowns += 1

            if queued:
                logger.info(
                    f"[QUEUE] {server_name} | {parent_folder} "
                    f"({len(events)} missing files)"
                )
            cooldown_skipped_scans += cooldowns

        if metadata_refresh_count:
            logger.info(f"[QUEUE] Metadata refreshes: {metadata_refresh_count}")

        total_files_found += files_in_path
        total_directories_searched += directories_in_path
        logger.info(
            f"[DONE] {SCAN_PATH} - {files_in_path} files in "
            f"{directories_in_path} directories"
        )

    processed_scans, batches_sent = process_pending_scans(pending_scans)
    processed_metadata_refreshes = process_pending_metadata_refreshes(
        pending_metadata_refreshes
    )
    scan_duration = time.time() - scan_start_time
    logger.info("--- SCAN SUMMARY ---")
    logger.info(f" Files checked:       {total_files_found}")
    logger.info(f" Directories:         {total_directories_searched}")
    logger.info(f" Rescans queued:      {len(pending_scans)}")
    batches_str = f" ({batches_sent} batches)" if batches_sent else ""
    logger.info(f" Rescans processed:   {processed_scans}{batches_str}")
    logger.info(f" Metadata queued:     {len(pending_metadata_refreshes)}")
    logger.info(f" Metadata processed:  {processed_metadata_refreshes}")
    logger.info(f" Pruned directories:  {pruned_directories}")
    logger.info(f" Skipped no library:  {skipped_no_library}")
    logger.info(f" Repair cooldown skips: {cooldown_skipped_scans}")
    logger.info(f" Metadata cooldown skips: {metadata_cooldown_skipped}")
    logger.info(f" Missing files:       {stats.total_missing}")
    logger.info(f" Broken symlinks:     {stats.broken_symlinks}")
    logger.info(f" Duration:            {scan_duration:.1f}s")
    logger.info("--------------------")

    # Send the final summary to Discord
    asyncio.run(stats.send_discord_summary())


def _safe_run_scan():
    """Wrapper for scheduled calls — prevents exceptions from crashing the scheduler loop."""
    try:
        run_scan()
    except Exception as e:
        logger.error(f"[FAIL] Scheduled scan failed: {e}", exc_info=True)


def main():
    """Main function to run the scanner on a schedule."""
    server_labels = []
    for s in media_servers:
        server_labels.append(f"{s['type'].capitalize()} ({s['url']})")
    servers_str = ", ".join(server_labels) if server_labels else "none"
    paths_str = ", ".join(SCAN_PATHS) if SCAN_PATHS else "none"
    symlink_str = "enabled" if SYMLINK_CHECK else "disabled"

    logger.info("========================================")
    logger.info(" Rescan - Media Server File Scanner")
    logger.info("========================================")
    logger.info(f" Servers:  {servers_str}")
    logger.info(f" Paths:    {paths_str}")
    logger.info(f" Interval: Every {RUN_INTERVAL} hours")
    logger.info(f" Symlinks: Checking {symlink_str}")
    logger.info(f" Workers:  {max(1, LIBRARY_WORKERS)} library workers")
    logger.info(
        f" Metadata: Repair {'enabled' if METADATA_REPAIR_ENABLED else 'disabled'}"
    )
    logger.info("========================================")

    # Run immediately on startup
    try:
        run_scan()
    except Exception as e:
        logger.error(f"[FAIL] Initial scan failed: {e}", exc_info=True)

    # Schedule subsequent runs (use _safe_run_scan so errors don't crash the loop)
    schedule.every(RUN_INTERVAL).hours.do(_safe_run_scan)

    while not _shutdown_requested:
        schedule.run_pending()
        idle = schedule.idle_seconds()
        time.sleep(min(max(1, idle), 60) if idle is not None else 60)
    logger.info("[SHUTDOWN] Exiting cleanly")


if __name__ == "__main__":
    main()
