import os
import requests
import configparser
import xml.etree.ElementTree as ET
from urllib.parse import quote
import time
from collections import defaultdict
from plexapi.server import PlexServer
import logging
import json
from datetime import datetime
import schedule
import discord
from discord import Webhook, Embed, Color
import asyncio
import aiohttp

# === CONFIG ===

config = configparser.ConfigParser()
config.read('config.ini')

try:
    LOG_LEVEL = config['logs']['loglevel']
    SCAN_INTERVAL = int(config['behaviour']['scan_interval'])
    RUN_INTERVAL = int(config['behaviour']['run_interval'])
    SYMLINK_CHECK = config.getboolean('behaviour', 'symlink_check', fallback=False)
    NOTIFICATIONS_ENABLED = config.getboolean('notifications', 'enabled', fallback=True)
    directories_raw = config['scan']['directories']
except (KeyError, configparser.NoSectionError) as e:
    import sys
    print(f"ERROR: Missing required config section/key: {e}. Please check your config.ini against config-example.ini.")
    sys.exit(1)

DISCORD_WEBHOOK_URL = config.get('notifications', 'discord_webhook_url', fallback='')
DISCORD_AVATAR_URL = "https://raw.githubusercontent.com/pukabyte/rescan/master/assets/logo.png"
DISCORD_WEBHOOK_NAME = "Rescan"

# Support both comma-separated or line-separated values
SCAN_PATHS = [path.strip() for path in directories_raw.replace('\n', ',').split(',') if path.strip()]

# Media file extensions to look for
MEDIA_EXTENSIONS = {
    '.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm',
    '.m4v', '.m4p', '.m4b', '.m4r', '.3gp', '.mpg', '.mpeg',
    '.m2v', '.m2ts', '.ts', '.vob', '.iso'
}

# Global library IDs and path mappings (per server)
library_ids = {}
library_paths = {}
# Cache for directory-level searches to minimize API calls
directory_cache = {}
# Per-server library cache to avoid redundant API calls
_library_cache = {}

# ANSI escape codes for text formatting
BOLD = '\033[1m'
RESET = '\033[0m'

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper()),
    format='%(asctime)s %(levelname)-5s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def _request_with_retry(method, url, retries=2, **kwargs):
    """Wrap requests calls with simple retry logic on transient errors."""
    transient_statuses = {429, 500, 502, 503, 504}
    transient_exceptions = (requests.exceptions.ConnectionError, requests.exceptions.Timeout)
    delay = 1
    last_exc = None
    for attempt in range(retries + 1):
        try:
            response = method(url, **kwargs)
            if response.status_code not in transient_statuses or attempt == retries:
                return response
            logger.warning(f"[RETRY] HTTP {response.status_code} from {url} retrying in {delay}s")
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
    total = len(embed.title or '') + len(embed.description or '')
    for f in embed.fields:
        total += len(f.name) + len(f.value)
    return total

# Initialize media servers (support Plex, Jellyfin, and Emby)
media_servers = []

# Helper function to parse server entries
def parse_server_entry(entry, default_type='plex'):
    """Parse a server entry in format: type:url:token or url:token (for backward compat)
    
    Handles URLs with ports (e.g., http://host:port:token) by checking if first part is a known type.
    """
    entry = entry.strip()
    
    # Check if entry starts with a known server type
    known_types = ['plex', 'jellyfin', 'emby']
    for server_type in known_types:
        if entry.startswith(f"{server_type}:"):
            # Format: type:url:token (where url may contain :port)
            # Remove the type prefix, then split on last colon
            remaining = entry[len(server_type) + 1:]  # Remove "type:"
            last_colon_idx = remaining.rfind(':')
            if last_colon_idx > 0:
                url_part = remaining[:last_colon_idx]
                token_part = remaining[last_colon_idx + 1:]
                return {'type': server_type.lower(), 'url': url_part, 'token': token_part}
    
    # Otherwise, treat as url:token format (backward compatible)
    # Find the last colon to split url from token (handles URLs with ports)
    last_colon_idx = entry.rfind(':')
    if last_colon_idx > 0:
        url_part = entry[:last_colon_idx]
        token_part = entry[last_colon_idx + 1:]
        return {'type': default_type, 'url': url_part, 'token': token_part}
    
    return None

# Load Plex servers (backward compatible)
if 'plex' in config:
    if 'servers' in config['plex']:
        # New format: multiple servers
        servers_raw = config['plex']['servers']
        servers_list = [s.strip() for s in servers_raw.replace('\n', ',').split(',') if s.strip()]
        for server_entry in servers_list:
            server_info = parse_server_entry(server_entry, default_type='plex')
            if server_info and server_info['type'] == 'plex':
                try:
                    plex_server = PlexServer(server_info['url'], server_info['token'])
                    media_servers.append({
                        'type': 'plex',
                        'url': server_info['url'],
                        'token': server_info['token'],
                        'server': plex_server
                    })
                    logger.info(f"[OK] Connected to Plex: {server_info['url']}")
                except Exception as e:
                    logger.error(f"[FAIL] Could not connect to Plex: {server_info['url']} - {str(e)}")
            elif server_info:
                logger.warning(f"[WARN] Invalid server type in plex section: {server_info['type']}")
    elif 'server' in config['plex'] and 'token' in config['plex']:
        # Old format: single server (backward compatible)
        PLEX_URL = config['plex']['server']
        TOKEN = config['plex']['token']
        try:
            plex_server = PlexServer(PLEX_URL, TOKEN)
            media_servers.append({
                'type': 'plex',
                'url': PLEX_URL,
                'token': TOKEN,
                'server': plex_server
            })
            logger.info(f"[OK] Connected to Plex: {PLEX_URL}")
        except Exception as e:
            logger.error(f"[FAIL] Could not connect to Plex: {PLEX_URL} - {str(e)}")

# Load Jellyfin servers
if 'jellyfin' in config:
    if 'servers' in config['jellyfin']:
        servers_raw = config['jellyfin']['servers']
        servers_list = [s.strip() for s in servers_raw.replace('\n', ',').split(',') if s.strip()]
        for server_entry in servers_list:
            server_info = parse_server_entry(server_entry, default_type='jellyfin')
            if server_info and server_info['type'] == 'jellyfin':
                media_servers.append({
                    'type': 'jellyfin',
                    'url': server_info['url'],
                    'token': server_info['token']
                })
                logger.info(f"[OK] Connected to Jellyfin: {server_info['url']}")
    elif 'server' in config['jellyfin'] and 'token' in config['jellyfin']:
        media_servers.append({
            'type': 'jellyfin',
            'url': config['jellyfin']['server'],
            'token': config['jellyfin']['token']
        })
        logger.info(f"[OK] Connected to Jellyfin: {config['jellyfin']['server']}")

# Load Emby servers
if 'emby' in config:
    if 'servers' in config['emby']:
        servers_raw = config['emby']['servers']
        servers_list = [s.strip() for s in servers_raw.replace('\n', ',').split(',') if s.strip()]
        for server_entry in servers_list:
            server_info = parse_server_entry(server_entry, default_type='emby')
            if server_info and server_info['type'] == 'emby':
                media_servers.append({
                    'type': 'emby',
                    'url': server_info['url'],
                    'token': server_info['token']
                })
                logger.info(f"[OK] Connected to Emby: {server_info['url']}")
    elif 'server' in config['emby'] and 'token' in config['emby']:
        media_servers.append({
            'type': 'emby',
            'url': config['emby']['server'],
            'token': config['emby']['token']
        })
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
            async with aiohttp.ClientSession() as session:
                webhook = Webhook.from_url(DISCORD_WEBHOOK_URL, session=session)

                # Create embed
                embed = Embed(
                    title="Rescan Summary",
                    color=Color.blue(),
                    timestamp=datetime.now()
                )

                # Add overview
                embed.add_field(
                    name="📊 Overview",
                    value=f"Found **{self.total_missing}** items from **{self.total_scanned}** scanned files",
                    inline=False
                )

                # Add broken symlinks summary if any
                if self.broken_symlinks > 0:
                    embed.add_field(
                        name="⚠️ Issues",
                        value=f"Broken Symlinks Skipped: **{self.broken_symlinks}**",
                        inline=False
                    )

                # Add library-specific stats
                for library, items in self.missing_items.items():
                    embed.add_field(
                        name=f"📁 {library}",
                        value=f"Found: **{len(items)}** items",
                        inline=True
                    )

                # Add other errors and warnings if any
                if self.errors or self.warnings:
                    error_text = "\n".join([f"❌ {e}" for e in self.errors])
                    warning_text = "\n".join([f"⚠️ {w}" for w in self.warnings])
                    if error_text or warning_text:
                        embed.add_field(
                            name="⚠️ Other Issues",
                            value=f"{error_text}\n{warning_text}",
                            inline=False
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
                title=embed.title,
                color=embed.color,
                timestamp=embed.timestamp
            )
            
            # Add overview field
            if embed.fields and embed.fields[0].name == "📊 Overview":
                base_embed.add_field(
                    name=embed.fields[0].name,
                    value=embed.fields[0].value,
                    inline=False
                )
            
            # Send base embed
            await webhook.send(
                embed=base_embed,
                avatar_url=DISCORD_AVATAR_URL,
                username=DISCORD_WEBHOOK_NAME,
                wait=True
            )
            
            # Create additional embeds for libraries
            current_embed = Embed(
                title="📁 Library Details",
                color=embed.color,
                timestamp=embed.timestamp
            )
            
            # Add library fields
            for field in embed.fields[1:]:
                if field.name.startswith("📁"):
                    if _embed_content_length(current_embed) + len(field.name) + len(field.value) > 6000:
                        # Send current embed and create new one
                        await webhook.send(
                            embed=current_embed,
                            avatar_url=DISCORD_AVATAR_URL,
                            username=DISCORD_WEBHOOK_NAME,
                            wait=True
                        )
                        current_embed = Embed(
                            title="📁 Library Details (continued)",
                            color=embed.color,
                            timestamp=embed.timestamp
                        )
                    current_embed.add_field(
                        name=field.name,
                        value=field.value,
                        inline=field.inline
                    )
            
            # Send final library embed if it has fields
            if current_embed.fields:
                await webhook.send(
                    embed=current_embed,
                    avatar_url=DISCORD_AVATAR_URL,
                    username=DISCORD_WEBHOOK_NAME,
                    wait=True
                )
            
            # Send issues in separate embed if they exist
            if embed.fields and embed.fields[-1].name == "⚠️ Issues":
                issues_embed = Embed(
                    title="⚠️ Issues",
                    color=Color.red(),
                    timestamp=embed.timestamp
                )
                issues_embed.add_field(
                    name=embed.fields[-1].name,
                    value=embed.fields[-1].value,
                    inline=False
                )
                await webhook.send(
                    embed=issues_embed,
                    avatar_url=DISCORD_AVATAR_URL,
                    username=DISCORD_WEBHOOK_NAME,
                    wait=True
                )
        else:
            # Send single embed if within limits
            await webhook.send(
                embed=embed,
                avatar_url=DISCORD_AVATAR_URL,
                username=DISCORD_WEBHOOK_NAME,
                wait=True
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
        plex = server_info['server']
        for section in plex.library.sections():
            lib_type = section.type
            lib_key = section.key
            lib_title = section.title
            lib_locations = []
            for location in section.locations:
                lib_locations.append(location)
            libraries.append({
                'type': lib_type,
                'key': lib_key,
                'title': lib_title,
                'locations': lib_locations
            })
    except Exception as e:
        logger.error(f"[FAIL] Plex | Could not fetch libraries: {str(e)}")
    return libraries

def get_libraries_jellyfin(server_info):
    """Get libraries from a Jellyfin server."""
    libraries = []
    try:
        url = f"{server_info['url']}/Library/VirtualFolders"
        headers = {'X-Emby-Token': server_info['token']}
        response = _request_with_retry(requests.get, url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Jellyfin API returns a list directly, not a dict with 'Items'
        library_list = data if isinstance(data, list) else data.get('Items', [])
        
        for library in library_list:
            lib_key = library.get('ItemId')
            lib_title = library.get('Name')
            lib_locations = library.get('Locations', [])
            libraries.append({
                'type': library.get('CollectionType', 'unknown'),
                'key': lib_key,
                'title': lib_title,
                'locations': lib_locations
            })
    except Exception as e:
        logger.error(f"[FAIL] Jellyfin | Could not fetch libraries: {str(e)}")
    return libraries

def get_libraries_emby(server_info):
    """Get libraries from an Emby server."""
    libraries = []
    try:
        url = f"{server_info['url']}/Library/VirtualFolders"
        headers = {'X-Emby-Token': server_info['token']}
        response = _request_with_retry(requests.get, url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Emby API may return a list or a dict with 'Items'
        library_list = data if isinstance(data, list) else data.get('Items', [])
        
        for library in library_list:
            lib_key = library.get('ItemId')
            lib_title = library.get('Name')
            lib_locations = library.get('Locations', [])
            libraries.append({
                'type': library.get('CollectionType', 'unknown'),
                'key': lib_key,
                'title': lib_title,
                'locations': lib_locations
            })
    except Exception as e:
        logger.error(f"[FAIL] Emby | Could not fetch libraries: {str(e)}")
    return libraries

def get_library_ids():
    """Fetch library section IDs and paths dynamically from all media servers."""
    global library_ids, library_paths, _library_cache
    library_ids = {}
    library_paths = {}
    _library_cache = {}

    for server_info in media_servers:
        server_type = server_info['type']
        server_url = server_info['url']
        server_token = server_info['token']

        libraries = []
        if server_type == 'plex':
            libraries = get_libraries_plex(server_info)
        elif server_type == 'jellyfin':
            libraries = get_libraries_jellyfin(server_info)
        elif server_type == 'emby':
            libraries = get_libraries_emby(server_info)

        # Populate the library cache for this server
        _library_cache[server_url] = libraries
        
        for lib in libraries:
            lib_key_with_server = f"{server_url}:{lib['key']}"
            library_ids[lib_key_with_server] = {
                'type': lib['type'],
                'key': lib['key'],
                'title': lib['title'],
                'url': server_url,
                'token': server_token,
                'server_type': server_type
            }
            
            for location in lib['locations']:
                if location not in library_paths:
                    library_paths[location] = []
                library_paths[location].append({
                    'key': lib['key'],
                    'title': lib['title'],
                    'url': server_url,
                    'token': server_token,
                    'server_type': server_type
                })
                logger.debug(f"[LIB] {server_type.capitalize()} | \"{lib['title']}\" (ID: {lib['key']}) at {location}")

    return library_ids

def get_library_id_for_path(file_path):
    """Get the library section ID and server info for a given file path."""
    best_match = None
    best_match_length = 0

    # Check all media servers
    for server_info in media_servers:
        server_type = server_info['type']
        server_url = server_info['url']
        server_token = server_info['token']

        try:
            # Use cached libraries if available, otherwise fetch
            if server_url in _library_cache:
                libraries = _library_cache[server_url]
            else:
                libraries = []
                if server_type == 'plex':
                    libraries = get_libraries_plex(server_info)
                elif server_type == 'jellyfin':
                    libraries = get_libraries_jellyfin(server_info)
                elif server_type == 'emby':
                    libraries = get_libraries_emby(server_info)

            # Find matching sections
            for lib in libraries:
                for location_path in lib['locations']:
                    # Normalize paths for comparison
                    normalized_scan_path = os.path.normpath(file_path)
                    normalized_location = os.path.normpath(location_path)
                    
                    # Check if the file path starts with the library location
                    if normalized_scan_path.startswith(normalized_location):
                        # Use the longest matching path (most specific)
                        if len(normalized_location) > best_match_length:
                            best_match = {
                                'section_id': lib['key'],
                                'section_title': lib['title'],
                                'server_url': server_url,
                                'token': server_token,
                                'server_type': server_type
                            }
                            best_match_length = len(normalized_location)
        except Exception as e:
            logger.debug(f"[FAIL] Error checking {server_type} {server_url} for path {file_path}: {str(e)}")
            continue
    
    if best_match:
        logger.debug(f"[LIB] Matched section: {best_match['section_title']} (ID: {best_match['section_id']}) on {best_match['server_type']} {best_match['server_url']}")
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
    match = re.match(r'^(.*?)\s*\(\d{4}\)', parent)
    if match:
        return match.group(1).strip()

    # Try grandparent folder (TV: .../Show (Year)/Season XX/file.mkv)
    grandparent = os.path.basename(os.path.dirname(os.path.dirname(file_path)))
    match = re.match(r'^(.*?)\s*\(\d{4}\)', grandparent)
    if match:
        return match.group(1).strip()

    return os.path.splitext(os.path.basename(file_path))[0]

def _short_search_term(title, max_words=3):
    """Shorten a title to the first few words for fuzzy API searches.

    Jellyfin/Emby SearchTerm works best with short queries; the exact
    file-path match afterwards ensures correctness.
    """
    words = title.split()
    return ' '.join(words[:max_words]) if len(words) > max_words else title

def _search_term_variants(term):
    """Return search term variants to handle common mismatches.

    Handles: and/&, stripped apostrophes (Youre->You're, Its->It's, etc.)
    """
    import re
    variants = [term]

    # and / & variants
    if ' and ' in term.lower():
        variants.append(term.replace(' and ', ' & ').replace(' And ', ' & '))
    elif ' & ' in term:
        variants.append(term.replace(' & ', ' and '))

    # Apostrophe variants: restore common contractions stripped from folder names
    apostrophe_map = {
        r'\b(\w+)re\b': r"\1're",     # Youre -> You're
        r'\b(\w+)nt\b': r"\1n't",     # Doesnt -> Doesn't, Dont -> Don't
        r'\b(\w+)ts\b': r"\1t's",     # Its -> It's, Whats -> What's
        r'\bIts\b': "It's",
    }
    for pattern, replacement in apostrophe_map.items():
        result = re.sub(pattern, replacement, term)
        if result != term and result not in variants:
            variants.append(result)

    # Possessive: Antonias -> Antonia's, Writers -> Writer's
    poss = re.sub(r'\b(\w+[^s])s\b', r"\1's", term)
    if poss != term and poss not in variants:
        variants.append(poss)

    return variants

def _check_plex_parts(xml_content, file_path):
    """Check if a file path matches any Part in Plex XML response."""
    root = ET.fromstring(xml_content)
    normalized = os.path.normpath(file_path)
    for part in root.iter('Part'):
        part_file = part.get('file')
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
        section_id = library_info['section_id']
        server_url = server_info['url']
        token = server_info['token']
        headers = {'X-Plex-Token': token}
        url = f"{server_url}/library/sections/{section_id}/all"

        # 1. Try path-based search first (works for movies)
        parent_folder = os.path.dirname(file_path)
        response = _request_with_retry(requests.get, url, headers=headers,
                                       params={'file': parent_folder}, timeout=15)
        if response is not None and _check_plex_parts(response.content, file_path):
            return True

        # 2. Fall back to title search (needed for TV shows where file= doesn't work)
        full_title = _extract_title_from_path(file_path)
        skip_words = {'the', 'a', 'an', 'of', 'in', 'on', 'at', 'to', 'for', 'is', 'it'}
        words = full_title.split()
        search_attempts = []
        for length in [min(3, len(words)), min(2, len(words)), 1]:
            term = ' '.join(words[:length])
            if length == 1 and term.lower() in skip_words:
                continue
            for variant in _search_term_variants(term):
                if variant not in search_attempts:
                    search_attempts.append(variant)

        for search_title in search_attempts:
            response = _request_with_retry(requests.get, url, headers=headers,
                                           params={'title': search_title}, timeout=15)
            if response is None:
                continue

            # Check Video entries (movies or episodes in some cases)
            if _check_plex_parts(response.content, file_path):
                return True

            # TV shows return Directory entries — fetch episodes via /allLeaves
            root = ET.fromstring(response.content)
            for directory in root.iter('Directory'):
                rating_key = directory.get('ratingKey')
                if not rating_key:
                    continue
                leaves_url = f"{server_url}/library/metadata/{rating_key}/allLeaves"
                leaves_response = _request_with_retry(requests.get, leaves_url, headers=headers, timeout=15)
                if leaves_response and _check_plex_parts(leaves_response.content, file_path):
                    return True

        return False
    except Exception as e:
        logger.error(f"[FAIL] Plex | File check error: {str(e)}")
        return False

def _check_file_emby_api(file_path, library_info, server_info, server_label):
    """Shared file check for Jellyfin and Emby (same API)."""
    try:
        url = f"{server_info['url']}/Items"
        headers = {'X-Emby-Token': server_info['token']}
        title = _extract_title_from_path(file_path)

        # Progressive broadening: 3 words, 2 words, 1 word (with &/and variants)
        skip_words = {'the', 'a', 'an', 'of', 'in', 'on', 'at', 'to', 'for', 'is', 'it'}
        words = title.split()
        search_attempts = []
        for length in [min(3, len(words)), min(2, len(words)), 1]:
            term = ' '.join(words[:length])
            if length == 1 and term.lower() in skip_words:
                continue
            for variant in _search_term_variants(term):
                if variant not in search_attempts:
                    search_attempts.append(variant)

        section_id = library_info.get('section_id')
        normalized_file_path = os.path.normpath(file_path)

        for search_term in search_attempts:
            params = {
                'Recursive': 'true',
                'IncludeItemTypes': 'Movie,Episode',
                'Fields': 'Path,MediaSources',
                'SearchTerm': search_term,
            }
            if section_id:
                params['ParentId'] = section_id

            response = _request_with_retry(requests.get, url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            for item in data.get('Items', []):
                if 'Path' in item:
                    if os.path.normpath(item['Path']) == normalized_file_path:
                        return True
                if 'MediaSources' in item:
                    for media_source in item['MediaSources']:
                        if 'Path' in media_source:
                            if os.path.normpath(media_source['Path']) == normalized_file_path:
                                return True

        return False
    except Exception as e:
        logger.error(f"[FAIL] {server_label} | File check error: {str(e)}")
        return False

def check_file_jellyfin(file_path, library_info, server_info):
    """Check if a file exists in a Jellyfin server."""
    return _check_file_emby_api(file_path, library_info, server_info, 'Jellyfin')

def check_file_emby(file_path, library_info, server_info):
    """Check if a file exists in an Emby server."""
    return _check_file_emby_api(file_path, library_info, server_info, 'Emby')

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
        if isinstance(cached_result, dict) and 'found_anywhere' in cached_result:
            logger.debug(f"[CACHE] Hit for: {os.path.basename(file_path)}")
            return cached_result
    
    server_status_list = []
    found_anywhere = False
    
    # Check ALL servers to see if file exists
    for server_info in media_servers:
        server_type = server_info['type']
        server_url = server_info['url']
        found_on_this_server = False
        library_info_for_scan = None
        
        try:
            # Small delay to be respectful to API
            time.sleep(0.05)
            
            if server_type == 'plex':
                # Plex requires checking each library individually
                # First, find the best matching library based on path
                libraries = _library_cache.get(server_url) or get_libraries_plex(server_info)
                normalized_file_path = os.path.normpath(file_path)
                best_match = None
                best_match_length = 0
                
                # Find best matching library based on path
                for lib in libraries:
                    for location in lib.get('locations', []):
                        normalized_location = os.path.normpath(location)
                        if normalized_file_path.startswith(normalized_location):
                            if len(normalized_location) > best_match_length:
                                best_match = {
                                    'section_id': lib['key'],
                                    'section_title': lib['title'],
                                    'server_url': server_url,
                                    'token': server_info['token'],
                                    'server_type': server_type
                                }
                                best_match_length = len(normalized_location)
                
                # Use best match or first library as fallback
                if best_match:
                    library_info_for_scan = best_match
                elif libraries:
                    first_lib = libraries[0]
                    library_info_for_scan = {
                        'section_id': first_lib['key'],
                        'section_title': first_lib['title'],
                        'server_url': server_url,
                        'token': server_info['token'],
                        'server_type': server_type
                    }
                
                # Check the best matching library first
                if library_info_for_scan:
                    filename = os.path.basename(file_path)
                    logger.debug(f"[OK] {server_type.capitalize()} | {library_info_for_scan['section_title']} | {filename}")

                    search_start = time.time()
                    found_here = check_file_plex(file_path, library_info_for_scan, server_info)
                    search_duration = time.time() - search_start

                    if found_here:
                        found_on_this_server = True
                        found_anywhere = True
                        logger.debug(f"[OK] {server_type.capitalize()} | {library_info_for_scan['section_title']} | {filename} ({search_duration:.2f}s)")
                    else:
                        logger.info(f"[MISS] {server_type.capitalize()} | {library_info_for_scan['section_title']} | {filename} ({search_duration:.2f}s)")
            elif server_type in ['jellyfin', 'emby']:
                # Jellyfin/Emby search globally, so we only need one call per server
                # First, get the best matching library for this path
                best_match = None
                if server_url in _library_cache:
                    libraries = _library_cache[server_url]
                else:
                    libraries = get_libraries_jellyfin(server_info) if server_type == 'jellyfin' else get_libraries_emby(server_info)
                
                # Find best matching library based on path
                normalized_file_path = os.path.normpath(file_path)
                best_match_length = 0
                for lib in libraries:
                    for location in lib.get('locations', []):
                        normalized_location = os.path.normpath(location)
                        if normalized_file_path.startswith(normalized_location):
                            if len(normalized_location) > best_match_length:
                                best_match = {
                                    'section_id': lib['key'],
                                    'section_title': lib['title'],
                                    'server_url': server_url,
                                    'token': server_info['token'],
                                    'server_type': server_type
                                }
                                best_match_length = len(normalized_location)
                
                # Use best match or create default library_info
                library_info = best_match or {
                    'section_id': '',
                    'section_title': 'All Libraries',
                    'server_url': server_url,
                    'token': server_info['token'],
                    'server_type': server_type
                }
                
                filename = os.path.basename(file_path)
                library_name = library_info.get('section_title', 'All Libraries')
                logger.debug(f"[OK] {server_type.capitalize()} | {library_name} | {filename}")

                search_start = time.time()
                if server_type == 'jellyfin':
                    found_here = check_file_jellyfin(file_path, library_info, server_info)
                else:
                    found_here = check_file_emby(file_path, library_info, server_info)
                search_duration = time.time() - search_start

                if found_here:
                    found_on_this_server = True
                    found_anywhere = True
                    library_info_for_scan = library_info
                    logger.debug(f"[OK] {server_type.capitalize()} | {library_name} | {filename} ({search_duration:.2f}s)")
                else:
                    library_info_for_scan = library_info
                    logger.info(f"[MISS] {server_type.capitalize()} | {library_name} | {filename} ({search_duration:.2f}s)")
            
            # Store status for this server
            server_status_list.append({
                'server_type': server_type,
                'server_url': server_url,
                'found': found_on_this_server,
                'library_info': library_info_for_scan,
                'token': server_info['token']
            })
                
        except Exception as e:
            logger.debug(f"[FAIL] Error checking {server_type} {server_url} for file {file_path}: {str(e)}")
            # Still add to status list even if error occurred
            server_status_list.append({
                'server_type': server_type,
                'server_url': server_url,
                'found': False,
                'library_info': None,
                'token': server_info['token']
            })
            continue
    
    result = {
        'found_anywhere': found_anywhere,
        'server_status': server_status_list
    }
    
    # Cache the result (cap size to prevent unbounded growth)
    if len(directory_cache) >= 50000:
        directory_cache.clear()
    directory_cache[cache_key] = result
    
    if not found_anywhere:
        filename = os.path.basename(file_path)
        logger.info(f"[MISS] Not indexed on any server: {filename}")
    
    return result

def is_in_media_server(file_path):
    """Check if a file exists in ANY media server using direct file search.
    
    This is a convenience wrapper that returns True/False for backward compatibility.
    """
    result = check_file_in_all_servers(file_path)
    return result['found_anywhere']

def scan_folder_plex(library_id, folder_path, server_url, token):
    """Trigger a library scan for a specific folder on a Plex server."""
    library_id = str(library_id)
    encoded_path = quote(folder_path)
    url = f"{server_url}/library/sections/{library_id}/refresh?path={encoded_path}"
    headers = {'X-Plex-Token': token}

    scan_start = time.time()
    try:
        response = _request_with_retry(requests.get, url, headers=headers, timeout=30)
        scan_duration = time.time() - scan_start
        
        if response.status_code == 200:
            logger.info(f"[OK] Plex | Scan completed ({scan_duration:.2f}s)")
        else:
            logger.warning(f"[WARN] Plex | Scan returned status {response.status_code} ({scan_duration:.2f}s)")
    except requests.exceptions.RequestException as e:
        scan_duration = time.time() - scan_start
        logger.error(f"[FAIL] Plex | Scan failed: {str(e)} ({scan_duration:.2f}s)")

def scan_folder_jellyfin_emby(library_id, folder_path, server_url, token, server_type):
    """Trigger a folder-specific scan on a Jellyfin or Emby server using the Media/Updated endpoint."""
    url = f"{server_url}/Library/Media/Updated"
    headers = {
        'X-Emby-Token': token,
        'Content-Type': 'application/json'
    }

    payload = {
        'Updates': [
            {
                'Path': folder_path,
                'UpdateType': 'Modified'
            }
        ]
    }

    scan_start = time.time()
    try:
        response = _request_with_retry(requests.post, url, headers=headers, json=payload, timeout=30)
        scan_duration = time.time() - scan_start

        if response.status_code in [200, 204]:
            logger.info(f"[OK] {server_type.capitalize()} | Scan completed ({scan_duration:.2f}s)")
        else:
            logger.warning(f"[WARN] {server_type.capitalize()} | Scan returned status {response.status_code} ({scan_duration:.2f}s)")
    except requests.exceptions.RequestException as e:
        scan_duration = time.time() - scan_start
        logger.error(f"[FAIL] {server_type.capitalize()} | Scan failed: {str(e)} ({scan_duration:.2f}s)")

def scan_folder(library_id, folder_path, server_url, token, server_type):
    """Trigger a library scan for a specific folder on a media server."""
    if server_type == 'plex':
        scan_folder_plex(library_id, folder_path, server_url, token)
    elif server_type in ['jellyfin', 'emby']:
        scan_folder_jellyfin_emby(library_id, folder_path, server_url, token, server_type)
    else:
        logger.warning(f"[WARN] Unknown server type: {server_type}")
        return

    logger.info(f"[WAIT] {SCAN_INTERVAL}s before next scan")
    time.sleep(SCAN_INTERVAL)  # Wait between scans

def is_broken_symlink(file_path):
    """Check if a file is a broken symlink."""
    if not os.path.islink(file_path):
        return False
    return not os.path.exists(os.path.realpath(file_path))

def run_scan():
    """Main scan logic."""
    stats = RunStats()
    scan_start_time = time.time()
    
    # Clear directory cache at the start of a new scan
    directory_cache.clear()
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
        server_type = server['type']
        server_counts[server_type] = server_counts.get(server_type, 0) + 1
    
    server_summary = ", ".join([f"{count} {server_type}" for server_type, count in server_counts.items()])
    logger.info(f"Found {len(library_ids)} libraries across {server_summary}")
    scanned_folders = set()
    total_files_found = 0
    total_directories_searched = 0

    for SCAN_PATH in SCAN_PATHS:
        logger.info(f"--- Scanning: {SCAN_PATH} ---")

        if not os.path.isdir(SCAN_PATH):
            error_msg = f"Directory not found: {SCAN_PATH}"
            logger.error(f"[FAIL] {error_msg}")
            stats.add_error(error_msg)
            continue

        files_in_path = 0
        directories_in_path = 0
        
        for root, dirs, files in os.walk(SCAN_PATH):
            directories_in_path += 1
            media_files_in_dir = 0
            
            for file in files:
                if file.startswith('.'):
                    continue  # skip hidden/system files

                file_ext = os.path.splitext(file)[1].lower()
                if file_ext not in MEDIA_EXTENSIONS:
                    continue  # skip non-media files

                files_in_path += 1
                media_files_in_dir += 1
                file_path = os.path.join(root, file)
                
                # Check for broken symlinks if enabled
                if SYMLINK_CHECK and is_broken_symlink(file_path):
                    logger.warning(f"[SKIP] Broken symlink: {os.path.basename(file_path)}")
                    stats.increment_broken_symlinks()
                    continue

                stats.increment_scanned()
                
                # Log progress every 100 files
                if files_in_path % 100 == 0:
                    logger.info(f"[PROGRESS] {files_in_path} files checked in {SCAN_PATH}")

                # Check file in all servers
                file_status = check_file_in_all_servers(file_path)
                
                filename = os.path.basename(file_path)
                
                # Check if file is missing from any server
                missing_servers = [s for s in file_status['server_status'] if not s['found']]
                
                if missing_servers:
                    # File is missing from at least one server
                    if not file_status['found_anywhere']:
                        # File not found in any server - use path-based matching for reporting
                        library_info = get_library_id_for_path(file_path)
                        if library_info and library_info.get('section_title'):
                            stats.add_missing_item(library_info['section_title'], file_path)
                            logger.info(f"[MISS] Not indexed on any server: {filename}")
                    
                    # Trigger scans on servers where file was not found
                    parent_folder = os.path.dirname(file_path)
                    servers_scanned = False
                    for server_status in missing_servers:
                        if server_status['library_info']:
                            server_key = f"{server_status['server_type']}:{server_status['server_url']}"
                            folder_key = f"{server_key}:{parent_folder}"
                            
                            if folder_key not in scanned_folders:
                                library_info = server_status['library_info']
                                section_id = library_info.get('section_id') if library_info else None
                                
                                # For Jellyfin/Emby, we can scan even without section_id since we use path-based scanning
                                # For Plex, we need section_id
                                if section_id or server_status['server_type'] in ['jellyfin', 'emby']:
                                    server_name = server_status['server_type'].capitalize()
                                    logger.info(f"[SCAN] {server_name} | {parent_folder}")
                                    scan_folder(
                                        section_id or '',
                                        parent_folder,
                                        server_status['server_url'],
                                        server_status['token'],
                                        server_status['server_type']
                                    )
                                    scanned_folders.add(folder_key)
                                    servers_scanned = True
                                else:
                                    warning_msg = f"[WARN] Could not determine library for {filename} on {server_status['server_type']}"
                                    logger.warning(warning_msg)
                                    stats.add_warning(warning_msg)
                else:
                    logger.debug(f"[OK] Exists on all servers: {filename}")
            
            # Log directory completion if it had media files
            if media_files_in_dir > 0:
                logger.debug(f"[OK] Directory done: {os.path.basename(root)} ({media_files_in_dir} files)")
        
        total_files_found += files_in_path
        total_directories_searched += directories_in_path
        logger.info(f"[DONE] {SCAN_PATH} - {files_in_path} files in {directories_in_path} directories")

    scan_duration = time.time() - scan_start_time
    logger.info("--- SCAN SUMMARY ---")
    logger.info(f" Files checked:       {total_files_found}")
    logger.info(f" Directories:         {total_directories_searched}")
    logger.info(f" Rescans triggered:   {len(scanned_folders)}")
    logger.info(f" Missing files:       {stats.total_missing}")
    logger.info(f" Broken symlinks:     {stats.broken_symlinks}")
    logger.info(f" Duration:            {scan_duration:.1f}s")
    logger.info("--------------------")

    # Send the final summary to Discord
    asyncio.run(stats.send_discord_summary())

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
    run_scan()
    
    # Schedule subsequent runs
    schedule.every(RUN_INTERVAL).hours.do(run_scan)
    
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute for pending tasks

if __name__ == '__main__':
    # Check if config exists
    if not os.path.exists('config.ini'):
        logger.error("[FAIL] config.ini not found")
        exit(1)
    
    main()
