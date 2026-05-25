import argparse as _argparse
import signal
import os
import sys
import requests
import configparser
import sqlite3
import xml.etree.ElementTree as ET
from urllib.parse import quote
import time
from collections import defaultdict
from plexapi.server import PlexServer
import logging
from datetime import datetime
import schedule
import discord
from discord import Webhook, Embed, Color
import asyncio
import aiohttp

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
# Per-server library cache to avoid redundant API calls
_library_cache = {}
# Bulk path cache for Jellyfin/Emby (built once per scan cycle)
_server_path_caches: dict = {}  # {server_url: set of normalized file paths}
_state_db_conn = None
_state_cache_available = STATE_CACHE_ENABLED

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


def _get_state_db():
    global _state_db_conn, _state_cache_available

    if not STATE_CACHE_ENABLED or not _state_cache_available:
        return None

    if _state_db_conn is not None:
        return _state_db_conn

    try:
        db_dir = os.path.dirname(STATE_DB_PATH)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        conn = sqlite3.connect(STATE_DB_PATH, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS file_state (
                path TEXT NOT NULL,
                server_type TEXT NOT NULL,
                server_url TEXT NOT NULL,
                size INTEGER NOT NULL,
                mtime_ns INTEGER NOT NULL,
                status TEXT NOT NULL,
                folder_path TEXT NOT NULL,
                last_seen_at REAL NOT NULL,
                last_changed_at REAL NOT NULL,
                PRIMARY KEY (path, server_type, server_url)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scan_queue_state (
                server_type TEXT NOT NULL,
                server_url TEXT NOT NULL,
                folder_path TEXT NOT NULL,
                last_queued_at REAL,
                last_processed_at REAL,
                PRIMARY KEY (server_type, server_url, folder_path)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_file_state_folder
            ON file_state (server_type, server_url, folder_path)
            """
        )
        _state_db_conn = conn
    except (OSError, sqlite3.Error) as e:
        _state_cache_available = False
        logger.warning(f"[WARN] State cache disabled: {e}")
        return None

    return _state_db_conn


def _file_signature(file_path):
    try:
        stat_result = os.stat(file_path)
    except OSError as e:
        logger.debug(f"[CACHE] Could not stat {file_path}: {e}")
        return None

    return stat_result.st_size, stat_result.st_mtime_ns


def _repair_scan_cooldown_seconds():
    return max(0.0, REPAIR_SCAN_COOLDOWN_HOURS * 3600)


def _recent_repair_scan_applies(
    conn, file_path, server_status, parent_folder, signature, now
):
    cooldown_seconds = _repair_scan_cooldown_seconds()
    if cooldown_seconds <= 0:
        return False

    size, mtime_ns = signature
    file_row = conn.execute(
        """
        SELECT size, mtime_ns, status
        FROM file_state
        WHERE path = ? AND server_type = ? AND server_url = ?
        """,
        (file_path, server_status["server_type"], server_status["server_url"]),
    ).fetchone()
    if not file_row:
        return False
    if file_row["status"] != "missing":
        return False
    if file_row["size"] != size or file_row["mtime_ns"] != mtime_ns:
        return False

    scan_row = conn.execute(
        """
        SELECT last_processed_at
        FROM scan_queue_state
        WHERE server_type = ? AND server_url = ? AND folder_path = ?
        """,
        (server_status["server_type"], server_status["server_url"], parent_folder),
    ).fetchone()
    if not scan_row or scan_row["last_processed_at"] is None:
        return False

    return now - float(scan_row["last_processed_at"]) < cooldown_seconds


def _record_missing_file_state(
    conn, file_path, server_status, parent_folder, signature, now
):
    if not conn or not signature:
        return

    size, mtime_ns = signature
    previous = conn.execute(
        """
        SELECT size, mtime_ns, last_changed_at
        FROM file_state
        WHERE path = ? AND server_type = ? AND server_url = ?
        """,
        (file_path, server_status["server_type"], server_status["server_url"]),
    ).fetchone()
    if previous and previous["size"] == size and previous["mtime_ns"] == mtime_ns:
        last_changed_at = previous["last_changed_at"]
    else:
        last_changed_at = now

    conn.execute(
        """
        INSERT INTO file_state (
            path, server_type, server_url, size, mtime_ns, status, folder_path,
            last_seen_at, last_changed_at
        )
        VALUES (?, ?, ?, ?, ?, 'missing', ?, ?, ?)
        ON CONFLICT(path, server_type, server_url) DO UPDATE SET
            size = excluded.size,
            mtime_ns = excluded.mtime_ns,
            status = excluded.status,
            folder_path = excluded.folder_path,
            last_seen_at = excluded.last_seen_at,
            last_changed_at = excluded.last_changed_at
        """,
        (
            file_path,
            server_status["server_type"],
            server_status["server_url"],
            size,
            mtime_ns,
            parent_folder,
            now,
            last_changed_at,
        ),
    )


def _mark_scan_queued(scan_request, now=None):
    conn = _get_state_db()
    if not conn:
        return

    now = time.time() if now is None else now
    conn.execute(
        """
        INSERT INTO scan_queue_state (
            server_type, server_url, folder_path, last_queued_at, last_processed_at
        )
        VALUES (?, ?, ?, ?, NULL)
        ON CONFLICT(server_type, server_url, folder_path) DO UPDATE SET
            last_queued_at = excluded.last_queued_at
        """,
        (
            scan_request["server_type"],
            scan_request["server_url"],
            scan_request["folder_path"],
            now,
        ),
    )


def _mark_scan_processed(scan_request, now=None):
    conn = _get_state_db()
    if not conn:
        return

    now = time.time() if now is None else now
    conn.execute(
        """
        INSERT INTO scan_queue_state (
            server_type, server_url, folder_path, last_queued_at, last_processed_at
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(server_type, server_url, folder_path) DO UPDATE SET
            last_processed_at = excluded.last_processed_at
        """,
        (
            scan_request["server_type"],
            scan_request["server_url"],
            scan_request["folder_path"],
            now,
            now,
        ),
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


def _build_server_path_cache(server_info, server_label):
    """Fetch all media item paths from a Jellyfin/Emby server in pages.

    Returns a set of normalised absolute paths.
    Called once per scan cycle; result stored in _server_path_caches.
    """
    url = f"{server_info['url']}/Items"
    headers = {"X-Emby-Token": server_info["token"]}
    page_size = 500
    base_params = {
        "recursive": "true",
        "includeItemTypes": "Movie,Episode",
        "fields": "Path,MediaSources",
        "enableTotalRecordCount": "true",
    }
    paths: set = set()
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
            response = _request_with_retry(
                requests.get, url, headers=headers, params=params, timeout=60
            )
            response.raise_for_status()
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
                    paths.add(os.path.normpath(item["Path"]))
                for ms in item.get("MediaSources", []):
                    if "Path" in ms:
                        paths.add(os.path.normpath(ms["Path"]))

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
        _is_path_parent_of(path, library_root)
        or _is_path_parent_of(library_root, path)
        for library_root in library_roots
    )


def _skip_unmatched_library_status(server_info, file_path):
    server_type = server_info["type"]
    server_url = server_info["url"]
    logger.info(
        f"[SKIP] {server_type.capitalize()} | No matching library for: {file_path}"
    )
    return {
        "server_type": server_type,
        "server_url": server_url,
        "found": False,
        "skipped": True,
        "library_info": None,
        "token": server_info["token"],
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
    if cache_key in directory_cache:
        cached_result = directory_cache[cache_key]
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

        try:
            # Small delay to be respectful to API
            time.sleep(0.05)

            if server_type == "plex":
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
                        logger.info(
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
                    logger.debug(
                        f"[OK] {server_type.capitalize()} | {library_name} | {filename} ({search_duration:.2f}s)"
                    )
                else:
                    library_info_for_scan = library_info
                    logger.info(
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
                }
            )
            continue

    result = {"found_anywhere": found_anywhere, "server_status": server_status_list}

    # Cache the result (cap size to prevent unbounded growth)
    if len(directory_cache) >= 50000:
        directory_cache.clear()
    directory_cache[cache_key] = result

    searchable_statuses = [s for s in server_status_list if not s.get("skipped")]
    if not found_anywhere and searchable_statuses:
        filename = os.path.basename(file_path)
        logger.info(f"[MISS] Not indexed on any server: {filename}")

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


def scan_folder_jellyfin_emby(library_id, folder_path, server_url, token, server_type):
    """Trigger a folder-specific scan on a Jellyfin or Emby server using the Media/Updated endpoint."""
    url = f"{server_url}/Library/Media/Updated"
    headers = {"X-Emby-Token": token, "Content-Type": "application/json"}

    payload = {"Updates": [{"Path": folder_path, "UpdateType": "Modified"}]}

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
        else:
            logger.warning(
                f"[WARN] {server_type.capitalize()} | Scan returned status {response.status_code} ({scan_duration:.2f}s)"
            )
    except requests.exceptions.RequestException as e:
        scan_duration = time.time() - scan_start
        logger.error(
            f"[FAIL] {server_type.capitalize()} | Scan failed: {str(e)} ({scan_duration:.2f}s)"
        )


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

    state_conn = None
    signature = None
    now = time.time()
    if file_path:
        state_conn = _get_state_db()
        signature = _file_signature(file_path) if state_conn else None

    if key in pending_scans:
        _record_missing_file_state(
            state_conn, file_path, server_status, parent_folder, signature, now
        )
        return "pending"

    if signature and _recent_repair_scan_applies(
        state_conn, file_path, server_status, parent_folder, signature, now
    ):
        _record_missing_file_state(
            state_conn, file_path, server_status, parent_folder, signature, now
        )
        return "cooldown"

    pending_scans[key] = {
        "section_id": section_id or "",
        "folder_path": parent_folder,
        "server_url": server_status["server_url"],
        "token": server_status["token"],
        "server_type": server_status["server_type"],
    }
    _mark_scan_queued(pending_scans[key], now)
    _record_missing_file_state(
        state_conn, file_path, server_status, parent_folder, signature, now
    )
    return "queued"


def process_pending_scans(pending_scans):
    processed = 0

    for scan_request in pending_scans.values():
        if _shutdown_requested:
            logger.info("[SHUTDOWN] Pending scans aborted cleanly")
            break

        server_name = scan_request["server_type"].capitalize()
        logger.info(f"[SCAN] {server_name} | {scan_request['folder_path']}")
        scan_folder(
            scan_request["section_id"],
            scan_request["folder_path"],
            scan_request["server_url"],
            scan_request["token"],
            scan_request["server_type"],
        )
        _mark_scan_processed(scan_request)
        processed += 1

        if SCAN_INTERVAL > 0:
            logger.info(f"[WAIT] {SCAN_INTERVAL}s before next scan")
            time.sleep(SCAN_INTERVAL)

    return processed


def is_broken_symlink(file_path):
    """Check if a path is a broken symlink and return the target if broken."""
    if not os.path.islink(file_path):
        return False, None
    target = os.readlink(file_path)
    if not os.path.exists(file_path):
        return True, target
    return False, None


def run_scan():
    """Main scan logic."""
    stats = RunStats()
    scan_start_time = time.time()

    # Clear directory cache at the start of a new scan
    directory_cache.clear()
    _server_path_caches.clear()
    logger.info("--- SCAN CYCLE START ---")

    library_ids = get_library_ids()

    # Check if we have at least one library configured
    if not library_ids:
        error_msg = "Could not find any libraries in any media server."
        logger.error(f"[FAIL] {error_msg}")
        stats.add_error(error_msg)
        asyncio.run(stats.send_discord_summary())
        return

    server_counts = {}
    for server in media_servers:
        server_type = server["type"]
        server_counts[server_type] = server_counts.get(server_type, 0) + 1

    server_summary = ", ".join(
        [f"{count} {server_type}" for server_type, count in server_counts.items()]
    )
    logger.info(f"Found {len(library_ids)} libraries across {server_summary}")
    library_roots = _get_library_roots()
    pending_scans = {}
    pruned_directories = 0
    skipped_no_library = 0
    cooldown_skipped_scans = 0
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

        files_in_path = 0
        directories_in_path = 0

        for root, dirs, files in os.walk(SCAN_PATH):
            directories_in_path += 1
            media_files_in_dir = 0

            kept_dirs = []
            for d in dirs:
                dir_path = os.path.join(root, d)
                if _should_walk_path(dir_path, library_roots):
                    kept_dirs.append(d)
                else:
                    pruned_directories += 1
                    logger.info(f"[SKIP] Pruned non-library directory: {dir_path}")
            dirs[:] = kept_dirs

            # Check for broken directory symlinks
            if SYMLINK_CHECK:
                for d in dirs[:]:  # iterate over a copy since we may remove
                    dir_path = os.path.join(root, d)
                    broken, target = is_broken_symlink(dir_path)
                    if broken:
                        target_info = f" -> {target}" if target else ""
                        logger.warning(
                            f"[SKIP] Broken directory symlink: {d}{target_info}"
                        )
                        stats.increment_broken_symlinks()
                        dirs.remove(d)

            root_in_library = not library_roots or any(
                _is_path_parent_of(library_root, root)
                for library_root in library_roots
            )
            if not root_in_library:
                continue

            for file in files:
                if file.startswith("."):
                    continue  # skip hidden/system files

                file_ext = os.path.splitext(file)[1].lower()
                if file_ext not in MEDIA_EXTENSIONS:
                    continue  # skip non-media files

                files_in_path += 1
                media_files_in_dir += 1
                file_path = os.path.join(root, file)

                # Check for broken symlinks if enabled
                if SYMLINK_CHECK:
                    broken, target = is_broken_symlink(file_path)
                    if broken:
                        target_info = f" -> {target}" if target else ""
                        logger.warning(
                            f"[SKIP] Broken symlink: {os.path.basename(file_path)}{target_info}"
                        )
                        stats.increment_broken_symlinks()
                        continue

                stats.increment_scanned()

                # Log progress every 100 files
                if files_in_path % 100 == 0:
                    logger.info(
                        f"[PROGRESS] {files_in_path} files checked in {SCAN_PATH}"
                    )

                # Check file in all servers
                file_status = check_file_in_all_servers(file_path)

                filename = os.path.basename(file_path)

                # Check if file is missing from any server
                missing_servers = [
                    s
                    for s in file_status["server_status"]
                    if not s["found"] and not s.get("skipped")
                ]
                skipped_servers = [
                    s for s in file_status["server_status"] if s.get("skipped")
                ]

                if missing_servers:
                    # File is missing from at least one server
                    if not file_status["found_anywhere"]:
                        # File not found in any server - use path-based matching for reporting
                        library_info = get_library_id_for_path(file_path)
                        if library_info and library_info.get("section_title"):
                            stats.add_missing_item(
                                library_info["section_title"], file_path
                            )
                            logger.info(f"[MISS] Not indexed on any server: {filename}")

                    # Queue scans on servers where file was not found.
                    parent_folder = os.path.dirname(file_path)
                    for server_status in missing_servers:
                        if server_status["library_info"]:
                            server_name = server_status["server_type"].capitalize()
                            queue_result = _queue_scan_request(
                                pending_scans, server_status, parent_folder, file_path
                            )
                            if queue_result == "queued":
                                logger.info(f"[QUEUE] {server_name} | {parent_folder}")
                            elif queue_result == "cooldown":
                                cooldown_skipped_scans += 1
                                logger.debug(
                                    f"[CACHE] {server_name} | Repair cooldown active: "
                                    f"{filename}"
                                )
                        else:
                            warning_msg = f"[WARN] Could not determine library for {filename} on {server_status['server_type']}"
                            logger.warning(warning_msg)
                            stats.add_warning(warning_msg)
                elif skipped_servers:
                    skipped_no_library += 1
                    logger.debug(
                        f"[SKIP] No matching library on all servers: {filename}"
                    )
                else:
                    logger.debug(f"[OK] Exists on all servers: {filename}")

            # Log directory completion if it had media files
            if media_files_in_dir > 0:
                logger.debug(
                    f"[OK] Directory done: {os.path.basename(root)} ({media_files_in_dir} files)"
                )

        total_files_found += files_in_path
        total_directories_searched += directories_in_path
        logger.info(
            f"[DONE] {SCAN_PATH} - {files_in_path} files in {directories_in_path} directories"
        )

    processed_scans = process_pending_scans(pending_scans)
    scan_duration = time.time() - scan_start_time
    logger.info("--- SCAN SUMMARY ---")
    logger.info(f" Files checked:       {total_files_found}")
    logger.info(f" Directories:         {total_directories_searched}")
    logger.info(f" Rescans queued:      {len(pending_scans)}")
    logger.info(f" Rescans processed:   {processed_scans}")
    logger.info(f" Pruned directories:  {pruned_directories}")
    logger.info(f" Skipped no library:  {skipped_no_library}")
    logger.info(f" Repair cooldown skips: {cooldown_skipped_scans}")
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
        time.sleep(60)  # Check every minute for pending tasks
    logger.info("[SHUTDOWN] Exiting cleanly")


if __name__ == "__main__":
    main()
