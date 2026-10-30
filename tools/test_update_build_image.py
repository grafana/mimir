# SPDX-License-Identifier: AGPL-3.0-only
import importlib.util,tempfile,pathlib,unittest
spec=importlib.util.spec_from_file_location('updater','tools/update-build-image.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class Updates(unittest.TestCase):
 def test_references(self):
  for image in ['grafana/mimir-build-image','registry.test:5000/build/image']:
   for old in ['old','old@sha256:'+'a'*64]:
    with tempfile.TemporaryDirectory() as d:
     p=pathlib.Path(d)/'Makefile';p.write_text('BUILD_IMAGE ?= '+image+'\nLATEST_BUILD_IMAGE_TAG ?= '+old+'\n');ref=image+':new@sha256:'+'b'*64;m.update(p,ref);self.assertIn('new@sha256:'+'b'*64,p.read_text());before=p.stat().st_mtime_ns;m.update(p,ref);self.assertEqual(before,p.stat().st_mtime_ns)
 def test_invalid_is_unchanged(self):
  with tempfile.TemporaryDirectory() as d:
   p=pathlib.Path(d)/'Makefile';s='BUILD_IMAGE ?= repo\nLATEST_BUILD_IMAGE_TAG ?= old\n';p.write_text(s)
   for bad in ['repo:new','repo:new@sha256:bad','other:new@sha256:'+'a'*64,'']:
    with self.assertRaises(ValueError):m.update(p,bad)
    self.assertEqual(s,p.read_text())
unittest.main()
