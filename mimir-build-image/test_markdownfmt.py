# SPDX-License-Identifier: AGPL-3.0-only

"""Exercise the installed formatter during the build on both image architectures."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class MarkdownFormatterTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)

    def write(self, name, content):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        return path

    def run_formatter(self, *args):
        return subprocess.run(
            [sys.executable, os.environ.get("MARKDOWN_FORMATTER", "/usr/local/bin/markdownfmt"), *args],
            cwd=self.root, capture_output=True, text=True,
        )

    def test_check_and_write(self):
        page = self.write("page.md", "# Title\n\n-   Item\n")
        result = self.run_formatter("--check", "**/*.md")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(page.read_text(), "# Title\n\n-   Item\n")

        result = self.run_formatter("--write", "**/*.md")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(page.read_text(), "# Title\n\n- Item\n")
        result = self.run_formatter("--check", "**/*.md")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_prettier_exclusions_and_gitignored_markdown(self):
        self.write(".prettierignore", "# Existing exclusions\nCHANGELOG.md\nvendor\n.*/\nchart/README.md\n")
        self.write(".gitignore", "page.md\n")
        ignored = [
            "CHANGELOG.md", "nested/CHANGELOG.md", "vendor/page.md",
            "nested/vendor/page.md", ".hidden/page.md", "nested/.hidden/page.md",
            "chart/README.md", "node_modules/page.md", ".hidden.md",
        ]
        for name in ignored:
            self.write(name, "# Title\n\n-   Item\n")
        page = self.write("page.md", "# Title\n\n-   Item\n")
        result = self.run_formatter("--write", "**/*.md")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(page.read_text(), "# Title\n\n- Item\n")
        for name in ignored:
            with self.subTest(name=name):
                self.assertEqual((self.root / name).read_text(), "# Title\n\n-   Item\n")

    def test_prettier_ignore_range_and_hugo_shortcode(self):
        content = (
            '---\ntitle: "Page"\n---\n\n# Page\n\n'
            '<!-- prettier-ignore-start -->\n\n-   Unchanged\n\n<!-- prettier-ignore-end -->\n\n'
            '{{% responsive-table %}}\n\n| A | B |\n| --- | --- |\n| x | y |\n\n{{% /responsive-table %}}\n'
        )
        page = self.write("page.md", content)
        result = self.run_formatter("--write", "**/*.md")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("\n-   Unchanged\n", page.read_text())
        self.assertIn("{{% responsive-table %}}", page.read_text())
        self.assertIn("| A   | B   |", page.read_text())
        formatted = page.read_text()
        result = self.run_formatter("--write", "**/*.md")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(page.read_text(), formatted)


if __name__ == "__main__":
    unittest.main()
