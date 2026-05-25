import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


class ConfigPathStartupTest(unittest.TestCase):
    def test_default_docker_config_path_allows_startup_without_root_config(self):
        repo_root = Path(__file__).resolve().parents[1]

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            scan_dir = tmp_path / "media"
            library_dir = scan_dir / "TV"
            outside_library_dir = scan_dir / "Anime_TV"
            config_dir = tmp_path / "config"
            stubs_dir = tmp_path / "stubs"
            library_dir.mkdir(parents=True)
            outside_library_dir.mkdir(parents=True)
            config_dir.mkdir()
            (library_dir / "example.mkv").write_text("", encoding="utf-8")
            (library_dir / "missing.mkv").write_text("", encoding="utf-8")
            for index in range(3):
                (outside_library_dir / f"outside-{index}.mkv").write_text(
                    "", encoding="utf-8"
                )
            self._write_dependency_stubs(stubs_dir)

            (config_dir / "config.ini").write_text(
                textwrap.dedent(
                    f"""
                    [logs]
                    loglevel = INFO

                    [jellyfin]
                    server = http://jellyfin:8096
                    token = test-token

                    [scan]
                    directories = {scan_dir}

                    [behaviour]
                    scan_interval = 0
                    run_interval = 24
                    symlink_check = false

                    [notifications]
                    enabled = false
                    discord_webhook_url =
                    """
                ).strip(),
                encoding="utf-8",
            )

            env = os.environ.copy()
            env["PYTHONPATH"] = str(stubs_dir)
            env["TEST_LIBRARY_DIR"] = str(library_dir)

            result = self._run_rescan(repo_root, tmp_path, env)

        output = result.stdout + result.stderr
        self.assertEqual(result.returncode, 0, output)
        self.assertNotIn("[FAIL] config.ini not found", output)
        self.assertIn(
            "[CACHE] Jellyfin | Fetching indexed paths from http://jellyfin:8096",
            output,
        )
        self.assertIn("[CACHE] Jellyfin | Cached 1 paths", output)
        self.assertIn("[SKIP] Pruned non-library directory:", output)
        self.assertNotIn("[MISS] Jellyfin |", output)
        self.assertNotIn("[MISS] Not indexed on any server:", output)
        self.assertNotIn("outside-0.mkv", output)
        self.assertNotIn("[MISS] Jellyfin | All Libraries | outside-0.mkv", output)
        self.assertNotIn(f"[SCAN] Jellyfin | {outside_library_dir}", output)
        self.assertIn(f"[QUEUE] Jellyfin | {library_dir}", output)
        self.assertIn(f"[SCAN] Jellyfin | {library_dir}", output)
        self.assertLess(
            output.index(f"[DONE] {scan_dir}"),
            output.index(f"[SCAN] Jellyfin | {library_dir}"),
        )
        self.assertIn(" Rescans queued:      1", output)

    def test_library_workers_scan_library_roots_and_merge_queues(self):
        repo_root = Path(__file__).resolve().parents[1]

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            scan_dir = tmp_path / "media"
            tv_dir = scan_dir / "TV"
            movie_dir = scan_dir / "Movies"
            config_dir = tmp_path / "config"
            stubs_dir = tmp_path / "stubs"
            tv_dir.mkdir(parents=True)
            movie_dir.mkdir(parents=True)
            config_dir.mkdir()
            (tv_dir / "missing-tv.mkv").write_text("", encoding="utf-8")
            (movie_dir / "missing-movie.mkv").write_text("", encoding="utf-8")
            self._write_dependency_stubs(stubs_dir)

            (config_dir / "config.ini").write_text(
                textwrap.dedent(
                    f"""
                    [logs]
                    loglevel = INFO

                    [jellyfin]
                    server = http://jellyfin:8096
                    token = test-token

                    [scan]
                    directories = {scan_dir}

                    [behaviour]
                    scan_interval = 0
                    run_interval = 24
                    symlink_check = false
                    library_workers = 2

                    [notifications]
                    enabled = false
                    discord_webhook_url =
                    """
                ).strip(),
                encoding="utf-8",
            )

            env = os.environ.copy()
            env["PYTHONPATH"] = str(stubs_dir)
            env["TEST_LIBRARY_DIRS"] = os.pathsep.join([str(tv_dir), str(movie_dir)])

            result = self._run_rescan(repo_root, tmp_path, env)

        output = result.stdout + result.stderr
        self.assertEqual(result.returncode, 0, output)
        self.assertIn("[WORKERS] Scanning 2 libraries with 2 workers", output)
        self.assertIn(f"[QUEUE] Jellyfin | {tv_dir}", output)
        self.assertIn(f"[QUEUE] Jellyfin | {movie_dir}", output)
        self.assertIn(" Rescans queued:      2", output)

    def test_metadata_repair_refreshes_indexed_item_with_missing_metadata(self):
        repo_root = Path(__file__).resolve().parents[1]

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            scan_dir = tmp_path / "media"
            library_dir = scan_dir / "TV"
            config_dir = tmp_path / "config"
            stubs_dir = tmp_path / "stubs"
            post_log = tmp_path / "posts.log"
            metadata_file = library_dir / "needs-meta.mkv"
            library_dir.mkdir(parents=True)
            config_dir.mkdir()
            metadata_file.write_text("indexed", encoding="utf-8")
            self._write_dependency_stubs(stubs_dir)

            (config_dir / "config.ini").write_text(
                textwrap.dedent(
                    f"""
                    [logs]
                    loglevel = INFO

                    [jellyfin]
                    server = http://jellyfin:8096
                    token = test-token

                    [scan]
                    directories = {scan_dir}

                    [behaviour]
                    scan_interval = 0
                    run_interval = 24
                    symlink_check = false
                    metadata_repair = true

                    [notifications]
                    enabled = false
                    discord_webhook_url =
                    """
                ).strip(),
                encoding="utf-8",
            )

            env = os.environ.copy()
            env["PYTHONPATH"] = str(stubs_dir)
            env["TEST_LIBRARY_DIR"] = str(library_dir)
            env["TEST_INDEXED_PATHS"] = str(metadata_file)
            env["TEST_METADATA_MISSING_PATH"] = str(metadata_file)
            env["TEST_POST_LOG"] = str(post_log)

            result = self._run_rescan(repo_root, tmp_path, env)
            post_lines = (
                post_log.read_text(encoding="utf-8").splitlines()
                if post_log.exists()
                else []
            )

        output = result.stdout + result.stderr
        self.assertEqual(result.returncode, 0, output)
        self.assertIn("[QUEUE] Metadata refreshes: 1", output)
        self.assertIn("[REFRESH] Jellyfin | Metadata | needs-meta.mkv", output)
        self.assertIn(" Metadata queued:     1", output)
        self.assertNotIn("[SCAN] Jellyfin |", output)
        self.assertEqual(
            post_lines,
            ["http://jellyfin:8096/Items/metadata-missing-item/Refresh"],
        )

    def test_repair_scan_cooldown_skips_unchanged_missing_files(self):
        repo_root = Path(__file__).resolve().parents[1]

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            scan_dir = tmp_path / "media"
            library_dir = scan_dir / "TV"
            config_dir = tmp_path / "config"
            stubs_dir = tmp_path / "stubs"
            post_log = tmp_path / "posts.log"
            missing_file = library_dir / "missing.mkv"
            library_dir.mkdir(parents=True)
            config_dir.mkdir()
            (library_dir / "example.mkv").write_text("indexed", encoding="utf-8")
            missing_file.write_text("", encoding="utf-8")
            self._write_dependency_stubs(stubs_dir)

            (config_dir / "config.ini").write_text(
                textwrap.dedent(
                    f"""
                    [logs]
                    loglevel = INFO

                    [jellyfin]
                    server = http://jellyfin:8096
                    token = test-token

                    [scan]
                    directories = {scan_dir}

                    [behaviour]
                    scan_interval = 0
                    run_interval = 24
                    symlink_check = false
                    state_cache = true
                    state_db = rescan-test.db
                    repair_scan_cooldown_hours = 24

                    [notifications]
                    enabled = false
                    discord_webhook_url =
                    """
                ).strip(),
                encoding="utf-8",
            )

            env = os.environ.copy()
            env["PYTHONPATH"] = str(stubs_dir)
            env["TEST_LIBRARY_DIR"] = str(library_dir)
            env["TEST_POST_LOG"] = str(post_log)

            first_result = self._run_rescan(repo_root, tmp_path, env)
            second_result = self._run_rescan(repo_root, tmp_path, env)
            missing_file.write_text("changed", encoding="utf-8")
            third_result = self._run_rescan(repo_root, tmp_path, env)

            post_lines = (
                post_log.read_text(encoding="utf-8").splitlines()
                if post_log.exists()
                else []
            )

        first_output = first_result.stdout + first_result.stderr
        second_output = second_result.stdout + second_result.stderr
        third_output = third_result.stdout + third_result.stderr
        self.assertEqual(first_result.returncode, 0, first_output)
        self.assertEqual(second_result.returncode, 0, second_output)
        self.assertEqual(third_result.returncode, 0, third_output)
        self.assertIn(f"[QUEUE] Jellyfin | {library_dir}", first_output)
        self.assertIn(f"[SCAN] Jellyfin | {library_dir}", first_output)
        self.assertNotIn(f"[QUEUE] Jellyfin | {library_dir}", second_output)
        self.assertNotIn(f"[SCAN] Jellyfin | {library_dir}", second_output)
        self.assertIn(" Repair cooldown skips: 1", second_output)
        self.assertIn(f"[QUEUE] Jellyfin | {library_dir}", third_output)
        self.assertIn(f"[SCAN] Jellyfin | {library_dir}", third_output)
        self.assertEqual(
            post_lines,
            [
                "http://jellyfin:8096/Library/Media/Updated",
                "http://jellyfin:8096/Library/Media/Updated",
            ],
        )

    def _run_rescan(self, repo_root, cwd, env):
        return subprocess.run(
            [sys.executable, str(repo_root / "rescan.py")],
            cwd=cwd,
            env=env,
            text=True,
            capture_output=True,
            timeout=10,
        )

    def _write_dependency_stubs(self, stubs_dir):
        (stubs_dir / "plexapi").mkdir(parents=True)
        (stubs_dir / "plexapi" / "__init__.py").write_text("", encoding="utf-8")
        (stubs_dir / "plexapi" / "server.py").write_text(
            textwrap.dedent(
                """
                class PlexServer:
                    def __init__(self, *args, **kwargs):
                        self.library = type("Library", (), {"sections": lambda self: []})()
                """
            ),
            encoding="utf-8",
        )
        (stubs_dir / "requests.py").write_text(
            textwrap.dedent(
                """
                import os


                class exceptions:
                    class ConnectionError(Exception):
                        pass

                    class Timeout(Exception):
                        pass

                    class RequestException(Exception):
                        pass


                class Response:
                    status_code = 200
                    content = b""

                    def raise_for_status(self):
                        return None

                    def json(self):
                        if self.url.endswith("/Library/VirtualFolders"):
                            library_dirs = os.environ.get("TEST_LIBRARY_DIRS")
                            if library_dirs:
                                locations = library_dirs.split(os.pathsep)
                            else:
                                locations = [os.environ["TEST_LIBRARY_DIR"]]
                            return [
                                {
                                    "ItemId": "library-1",
                                    "Name": "Movies",
                                    "Locations": locations,
                                    "CollectionType": "movies",
                                }
                            ]

                        if self.url.endswith("/Items"):
                            indexed_paths = os.environ.get("TEST_INDEXED_PATHS")
                            if indexed_paths:
                                paths = indexed_paths.split(os.pathsep)
                            else:
                                paths = [
                                    os.path.join(
                                        os.environ["TEST_LIBRARY_DIR"], "example.mkv"
                                    )
                                ]

                            metadata_missing_path = os.environ.get(
                                "TEST_METADATA_MISSING_PATH"
                            )
                            items = []
                            for index, path in enumerate(paths):
                                item = {
                                    "Id": f"item-{index}",
                                    "Path": path,
                                    "MediaSources": [],
                                    "ProviderIds": {"Tmdb": str(index)},
                                    "ProductionYear": 2026,
                                }
                                if metadata_missing_path and path == metadata_missing_path:
                                    item["Id"] = "metadata-missing-item"
                                    item["ProviderIds"] = {}
                                    item.pop("ProductionYear")
                                items.append(item)

                            return {"Items": items, "TotalRecordCount": len(items)}

                        return {}


                def get(url, *args, **kwargs):
                    params = kwargs.get("params") or {}
                    if url.endswith("/Items"):
                        limit = int(params.get("limit") or params.get("Limit") or 0)
                        if limit > 1000:
                            raise SystemExit("unbounded /Items request")
                    response = Response()
                    response.url = url
                    return response


                def post(*args, **kwargs):
                    log_path = os.environ.get("TEST_POST_LOG")
                    if log_path:
                        with open(log_path, "a", encoding="utf-8") as handle:
                            handle.write(str(args[0]) + "\\n")
                    response = Response()
                    response.url = args[0]
                    return response
                """
            ),
            encoding="utf-8",
        )
        (stubs_dir / "schedule.py").write_text(
            textwrap.dedent(
                """
                class _Every:
                    @property
                    def hours(self):
                        return self

                    def do(self, func):
                        return func


                def every(interval):
                    return _Every()


                def run_pending():
                    raise SystemExit(0)
                """
            ),
            encoding="utf-8",
        )
        (stubs_dir / "discord.py").write_text(
            textwrap.dedent(
                """
                class HTTPException(Exception):
                    pass


                class Webhook:
                    pass


                class Embed:
                    pass


                class Color:
                    @staticmethod
                    def blue():
                        return None

                    @staticmethod
                    def red():
                        return None
                """
            ),
            encoding="utf-8",
        )
        (stubs_dir / "aiohttp.py").write_text(
            textwrap.dedent(
                """
                class ClientTimeout:
                    def __init__(self, *args, **kwargs):
                        pass


                class ClientSession:
                    def __init__(self, *args, **kwargs):
                        pass
                """
            ),
            encoding="utf-8",
        )


if __name__ == "__main__":
    unittest.main()
