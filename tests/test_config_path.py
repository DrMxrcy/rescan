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
            config_dir = tmp_path / "config"
            stubs_dir = tmp_path / "stubs"
            scan_dir.mkdir()
            config_dir.mkdir()
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
                    scan_interval = 5
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

            result = subprocess.run(
                [sys.executable, str(repo_root / "rescan.py")],
                cwd=tmp_path,
                env=env,
                text=True,
                capture_output=True,
                timeout=10,
            )

        output = result.stdout + result.stderr
        self.assertEqual(result.returncode, 0, output)
        self.assertNotIn("[FAIL] config.ini not found", output)

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
                        return [
                            {
                                "ItemId": "library-1",
                                "Name": "Movies",
                                "Locations": [],
                                "CollectionType": "movies",
                            }
                        ]


                def get(*args, **kwargs):
                    return Response()


                def post(*args, **kwargs):
                    return Response()
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
