# SPDX-License-Identifier: AGPL-3.0-only
import pathlib
import subprocess
import sys
import tempfile
import unittest

SCRIPT = pathlib.Path(__file__).with_name("check-api-file-modes.py").resolve()


class FileModesTest(unittest.TestCase):
    def test_staged_tree_modes(self):
        for kind in ["regular", "delete", "executable", "symlink", "mode-change"]:
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                def git(*args):
                    return subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)
                git("init")
                git("config", "user.name", "Example")
                git("config", "user.email", "example@example.com")
                git("config", "commit.gpgsign", "false")
                git("config", "core.filemode", "true")
                path = root / "existing"
                path.write_text("old")
                git("add", ".")
                git("commit", "-m", "base")
                if kind == "delete":
                    path.unlink()
                elif kind == "mode-change":
                    path.chmod(0o755)
                else:
                    added = root / "new directory"
                    added.mkdir()
                    path = added / "new file"
                    if kind == "symlink":
                        path.symlink_to("missing")
                    else:
                        path.write_bytes(b"new\x00content\n")
                        if kind == "executable":
                            path.chmod(0o755)
                git("add", "-A")
                result = subprocess.run([sys.executable, str(SCRIPT), "."], cwd=root, capture_output=True)
                self.assertEqual(kind in ["regular", "delete"], result.returncode == 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
