<div align="center">
  <a href="https://github.com/Pukabyte/rescan">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="assets/logo.png" width="400">
      <img alt="rescan" src="assets/logo.png" width="400">
    </picture>
  </a>
</div>

<div align="center">
  <a href="https://github.com/Pukabyte/rescan/stargazers"><img alt="GitHub Repo stars" src="https://img.shields.io/github/stars/Pukabyte/rescan?label=Rescan"></a>
  <a href="https://github.com/Pukabyte/rescan/issues"><img alt="Issues" src="https://img.shields.io/github/issues/Pukabyte/rescan" /></a>
  <a href="https://github.com/Pukabyte/rescan/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/github/license/Pukabyte/rescan"></a>
  <a href="https://github.com/Pukabyte/rescan/graphs/contributors"><img alt="Contributors" src="https://img.shields.io/github/contributors/Pukabyte/rescan" /></a>
  <a href="https://discord.gg/vMSnNcd7m5"><img alt="Discord" src="https://img.shields.io/badge/Join%20discord-8A2BE2" /></a>
  <br/>
  <a href="https://github.com/Pukabyte/rescan/actions"><img alt="Docker Build" src="https://img.shields.io/github/actions/workflow/status/Pukabyte/rescan/docker-build.yml?label=docker%20build" /></a>
  <a href="https://github.com/Pukabyte/rescan/actions"><img alt="Lint" src="https://img.shields.io/github/actions/workflow/status/Pukabyte/rescan/lint.yml?label=lint" /></a>
</div>

<div align="center">
  <p>Keep your Plex, Jellyfin, and Emby libraries in sync with your media files.</p>
</div>

# Rescan

Scan your media libraries for missing files and trigger rescans when needed.<br/>
This is a good once-over in case your autoscan tool misses an import or an upgrade from your *arr apps.<br/>
It can also provide Discord notification summaries with detailed statistics.<br/>

<img alt="rescan" src="assets/discord.png" width="400">

## Features

- **Multi-server support** — Plex, Jellyfin, and Emby
- **Multiple servers per platform** — connect several Plex, Jellyfin, or Emby instances at once
- **Fast Jellyfin/Emby scanning** — bulk path cache for O(1) lookups instead of per-file API calls
- **Discord notifications** — detailed summaries with library statistics, missing items, and broken symlinks
- **Docker support** — pre-built multi-arch images (amd64 + arm64) via GitHub Container Registry
- **Graceful shutdown** — handles SIGTERM/SIGINT cleanly so `docker stop` exits immediately
- **Flexible configuration** — config file, `--config` CLI flag, or environment variable overrides
- **Broken symlink detection** — optionally check for and report broken symlinks
- **Scheduled scanning** — configurable intervals with crash protection and request timeouts
- **Both movie and TV show libraries** — works across all library types

## Prerequisites

- Python 3.11 or higher (for manual installation)
- Plex Media Server, Jellyfin, or Emby
- Discord webhook URL (optional, for notifications)

## Quick Start (Docker)

The easiest way to run Rescan is with the pre-built GHCR image.

1. Create a directory for your config:
```bash
mkdir -p /opt/rescan
```

2. Download the example config:
```bash
curl -o /opt/rescan/config.ini https://raw.githubusercontent.com/Pukabyte/rescan/main/config-example.ini
```

3. Edit `/opt/rescan/config.ini` with your settings (see [Configuration](#configuration)).

4. Run with Docker:
```bash
docker run -d \
  --name rescan \
  --restart unless-stopped \
  -v /opt/rescan:/app/config \
  -v /mnt:/mnt \
  -v /etc/localtime:/etc/localtime:ro \
  ghcr.io/drmxrcy/rescan:latest
```

Or use Docker Compose:
```yaml
services:
  rescan:
    image: ghcr.io/drmxrcy/rescan:latest
    container_name: rescan
    restart: unless-stopped
    volumes:
      - /opt/rescan:/app/config
      - /mnt:/mnt
      - /etc/localtime:/etc/localtime:ro
```

## Configuration

Rescan can be configured via `config.ini`, environment variables, or the `--config` CLI flag.

### Config File (`config.ini`)

```ini
[logs]
loglevel = INFO

[plex]
# Single server:
server = http://localhost:32400
token = your_plex_token_here

# Or multiple servers (comma-separated):
# servers = http://localhost:32400:token1,http://plex2:32400:token2

[jellyfin]
server = http://localhost:8096
token = your_jellyfin_api_token_here

[emby]
server = http://localhost:8096
token = your_emby_api_token_here

[scan]
directories = /path/to/your/media/folder

[behaviour]
scan_interval = 5
run_interval = 24
symlink_check = true

[notifications]
enabled = false
discord_webhook_url = your_discord_webhook_url_here
```

### Environment Variables

You can override key settings via environment variables (useful for Docker secrets or quick changes):

| Variable | Description |
|----------|-------------|
| `PLEX_TOKEN` | Override Plex token |
| `JELLYFIN_TOKEN` | Override Jellyfin token |
| `DISCORD_WEBHOOK_URL` | Override Discord webhook URL |

### CLI Flags

```bash
python rescan.py --config /path/to/custom/config.ini
```

### Setting Reference

**Plex / Jellyfin / Emby Settings**
- `server` — Single server URL
- `token` — API token for the server
- `servers` — Multiple servers as `url:token` pairs (comma-separated)

**Scan Settings**
- `directories` — Comma-separated list of directories to scan
- `scan_interval` — Seconds to wait between rescans
- `run_interval` — Hours between full scans
- `symlink_check` — Enable/disable broken symlink detection

**Notification Settings**
- `enabled` — Enable/disable Discord notifications
- `discord_webhook_url` — Your Discord webhook URL

## Discord Notifications

When enabled, Rescan sends detailed notifications to Discord including:
- Overview of missing items across all servers
- Library-specific statistics
- Broken symlinks (if enabled)
- Errors and warnings

## Manual Installation

1. Clone the repository:
```bash
git clone https://github.com/Pukabyte/rescan.git
cd rescan
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Copy and configure the config file:
```bash
cp config-example.ini config.ini
# Edit config.ini with your settings
```

4. Run the script:
```bash
python rescan.py
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [PlexAPI](https://github.com/pkkid/python-plexapi) for Plex server interaction
- [aiohttp](https://docs.aiohttp.org/) for async HTTP requests
