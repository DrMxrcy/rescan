import os
import sqlite3
import time


class StateCache:
    def __init__(self, enabled, db_path, repair_cooldown_hours, logger):
        self.enabled = enabled
        self.db_path = db_path
        self.repair_cooldown_hours = repair_cooldown_hours
        self.logger = logger
        self.available = enabled
        self.conn = None

    def connect(self):
        if not self.enabled or not self.available:
            return None

        if self.conn is not None:
            return self.conn

        try:
            db_dir = os.path.dirname(self.db_path)
            if db_dir:
                os.makedirs(db_dir, exist_ok=True)

            conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
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
                CREATE TABLE IF NOT EXISTS metadata_refresh_state (
                    server_type TEXT NOT NULL,
                    server_url TEXT NOT NULL,
                    item_id TEXT NOT NULL,
                    path TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    mtime_ns INTEGER NOT NULL,
                    last_queued_at REAL,
                    last_processed_at REAL,
                    PRIMARY KEY (server_type, server_url, item_id)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_file_state_folder
                ON file_state (server_type, server_url, folder_path)
                """
            )
            self.conn = conn
        except (OSError, sqlite3.Error) as e:
            self.available = False
            self.logger.warning(f"[WARN] State cache disabled: {e}")
            return None

        return self.conn

    @staticmethod
    def file_signature(file_path):
        try:
            stat_result = os.stat(file_path)
        except OSError:
            return None

        return stat_result.st_size, stat_result.st_mtime_ns

    def cooldown_seconds(self):
        return max(0.0, self.repair_cooldown_hours * 3600)

    def recent_repair_scan_applies(
        self, file_path, server_status, parent_folder, signature, now
    ):
        conn = self.connect()
        cooldown_seconds = self.cooldown_seconds()
        if not conn or not signature or cooldown_seconds <= 0:
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

    def record_missing_file(
        self, file_path, server_status, parent_folder, signature, now
    ):
        conn = self.connect()
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

    def mark_scan_queued(self, scan_request, now=None):
        conn = self.connect()
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

    def mark_scan_processed(self, scan_request, now=None):
        conn = self.connect()
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

    def recent_metadata_refresh_applies(
        self, file_path, server_status, item_id, signature, now
    ):
        conn = self.connect()
        cooldown_seconds = self.cooldown_seconds()
        if not conn or not signature or cooldown_seconds <= 0:
            return False

        size, mtime_ns = signature
        row = conn.execute(
            """
            SELECT path, size, mtime_ns, last_processed_at
            FROM metadata_refresh_state
            WHERE server_type = ? AND server_url = ? AND item_id = ?
            """,
            (server_status["server_type"], server_status["server_url"], item_id),
        ).fetchone()
        if not row or row["last_processed_at"] is None:
            return False
        if row["path"] != file_path:
            return False
        if row["size"] != size or row["mtime_ns"] != mtime_ns:
            return False

        return now - float(row["last_processed_at"]) < cooldown_seconds

    def mark_metadata_refresh_queued(
        self, file_path, server_status, item_id, signature, now=None
    ):
        conn = self.connect()
        if not conn or not signature:
            return

        now = time.time() if now is None else now
        size, mtime_ns = signature
        conn.execute(
            """
            INSERT INTO metadata_refresh_state (
                server_type, server_url, item_id, path, size, mtime_ns,
                last_queued_at, last_processed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
            ON CONFLICT(server_type, server_url, item_id) DO UPDATE SET
                path = excluded.path,
                size = excluded.size,
                mtime_ns = excluded.mtime_ns,
                last_queued_at = excluded.last_queued_at
            """,
            (
                server_status["server_type"],
                server_status["server_url"],
                item_id,
                file_path,
                size,
                mtime_ns,
                now,
            ),
        )

    def mark_metadata_refresh_processed(self, refresh_request, now=None):
        conn = self.connect()
        if not conn:
            return

        now = time.time() if now is None else now
        conn.execute(
            """
            INSERT INTO metadata_refresh_state (
                server_type, server_url, item_id, path, size, mtime_ns,
                last_queued_at, last_processed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(server_type, server_url, item_id) DO UPDATE SET
                last_processed_at = excluded.last_processed_at
            """,
            (
                refresh_request["server_type"],
                refresh_request["server_url"],
                refresh_request["item_id"],
                refresh_request["file_path"],
                refresh_request["size"],
                refresh_request["mtime_ns"],
                now,
                now,
            ),
        )
