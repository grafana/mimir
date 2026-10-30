# SPDX-License-Identifier: AGPL-3.0-only
import importlib.util
import pathlib
import tempfile
import unittest

spec = importlib.util.spec_from_file_location(
    "updater", pathlib.Path(__file__).with_name("update-build-image.py")
)
updater = importlib.util.module_from_spec(spec)
spec.loader.exec_module(updater)


class UpdateBuildImageTest(unittest.TestCase):
    def test_references(self):
        for image in ["grafana/mimir-build-image", "registry.test:5000/build/image"]:
            for old in ["old", "old@sha256:" + "a" * 64]:
                with self.subTest(image=image, old=old), tempfile.TemporaryDirectory() as directory:
                    path = pathlib.Path(directory) / "Makefile"
                    path.write_text(f"BUILD_IMAGE ?= {image}\nLATEST_BUILD_IMAGE_TAG ?= {old}\n")
                    reference = image + ":new@sha256:" + "b" * 64

                    updater.update(path, reference)
                    expected = f"BUILD_IMAGE ?= {image}\nLATEST_BUILD_IMAGE_TAG ?= new@sha256:" + "b" * 64 + "\n"
                    self.assertEqual(expected, path.read_text())
                    modified_at = path.stat().st_mtime_ns
                    updater.update(path, reference)
                    self.assertEqual(modified_at, path.stat().st_mtime_ns)

    def test_invalid_reference_leaves_file_unchanged(self):
        source = "BUILD_IMAGE ?= repo\nLATEST_BUILD_IMAGE_TAG ?= old\n"
        for reference in ["repo:new", "repo:new@sha256:bad", "other:new@sha256:" + "a" * 64, ""]:
            with self.subTest(reference=reference), tempfile.TemporaryDirectory() as directory:
                path = pathlib.Path(directory) / "Makefile"
                path.write_text(source)
                with self.assertRaises(ValueError):
                    updater.update(path, reference)
                self.assertEqual(source, path.read_text())

    def test_missing_or_duplicate_assignment_leaves_file_unchanged(self):
        for assignments in ["", "LATEST_BUILD_IMAGE_TAG ?= old\n" * 2]:
            with self.subTest(assignments=assignments), tempfile.TemporaryDirectory() as directory:
                path = pathlib.Path(directory) / "Makefile"
                source = "BUILD_IMAGE ?= repo\n" + assignments
                path.write_text(source)
                with self.assertRaises(ValueError):
                    updater.update(path, "repo:new@sha256:" + "b" * 64)
                self.assertEqual(source, path.read_text())


if __name__ == "__main__":
    unittest.main()
