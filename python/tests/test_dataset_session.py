# python 3.7+ from __future__ import annotations
import unittest
import os
import tempfile
import shutil
import arkimet as arki
from arkimet.test import daemon


class TestSession(unittest.TestCase):
    def test_aliases(self):
        with arki.dataset.Session(load_aliases=False) as session:
            with self.assertRaises(RuntimeError):
                session.matcher("origin:test")

            session.load_aliases("""
[origin]
test = GRIB1 or GRIB2
""")

            self.assertIsInstance(session.matcher("origin:test"), arki.Matcher)

    def test_dataset_access(self):
        with arki.dataset.Session() as session:
            reader = session.dataset_reader(cfg={
                "format": "grib",
                "name": "test.grib1",
                "path": "inbound/test.grib1",
                "type": "file",
            })
            self.assertEqual(str(reader), "dataset.Reader(file, test.grib1)")
            self.assertEqual(repr(reader), "dataset.Reader(file, test.grib1)")

            if os.path.exists("testds"):
                shutil.rmtree("testds")
            with session.dataset_writer(cfg={
                        "format": "grib",
                        "name": "testds",
                        "path": "testds",
                        "type": "iseg",
                        "step": "daily",
                    }) as writer:
                def do_import(md):
                    writer.acquire(md)

                reader.query_data(on_metadata=do_import)
                writer.flush()

            with session.dataset_checker(cfg={
                        "format": "grib",
                        "name": "testds",
                        "path": "testds",
                        "type": "iseg",
                        "step": "daily",
                    }) as checker:
                checker.check()

    def test_expand_query(self):
        http_test_daemon = os.path.join(os.environ["TOP_SRCDIR"], "arki/dataset/http-test-daemon")

        with arki.dataset.Session(load_aliases=False) as session:
            # Expand query without aliases
            m = session.matcher("reftime:=2020-06-08; product:GRIB1,,2")
            self.assertEqual(m.expanded, "product:GRIB1,,2; reftime:>=2020-06-08 00:00:00,<2020-06-09 00:00:00")
            with self.assertRaises(RuntimeError):
                session.matcher("origin:test")

            # Expand query with local aliases
            session.load_aliases("""
[origin]
test = GRIB1 or GRIB2
""")
            self.assertEqual(session.matcher("origin:test").expanded, "origin:GRIB1 or GRIB2")

            # Expand query with remote aliases from one server, after adding a remote dataset
            with self.assertRaises(RuntimeError):
                session.matcher("level:g00")
            with daemon(http_test_daemon, "--action=aliases", "--port=18001") as url:
                session.add_dataset(cfg=f"""
type=remote
name=remote1
path={url}/dataset/remote1
server={url}
""")
                self.assertEqual(session.matcher("level:g00").expanded, "level:GRIB1,1 or GRIB2S,1,0,0")

            # Expand query with a mix of remote aliases from multiple servers, after adding multiple remote datasets
            with tempfile.NamedTemporaryFile(mode="wt") as fd:
                fd.write("""
[level]
g01 = GRIB1,2 or GRIB2S,2,0,0
""")
                fd.flush()
                with daemon(http_test_daemon, "--action=aliases", "--port=18002", f"--alias-file={fd.name}") as url:
                    session.add_dataset(cfg=f"""
type=remote
name=remote2
path={url}/dataset/remote2
server={url}
""")
                    self.assertEqual(session.matcher("level:g00 or g01").expanded,
                                     "level:GRIB1,1 or GRIB2S,1,0,0 or GRIB1,2 or GRIB2S,2,0,0")

            # Add a remote dataset with conflicting aliases, and check that it
            #  raises an error and that the dataset does not get added
            with tempfile.NamedTemporaryFile(mode="wt") as fd:
                fd.write("""
[level]
g00 = GRIB1,2 or GRIB2S,2,0,0
g01 = GRIB1,3 or GRIB2S,3,0,0
""")
                fd.flush()
                with daemon(http_test_daemon, "--action=aliases", "--port=18003", f"--alias-file={fd.name}") as url:
                    with self.assertRaises(RuntimeError):
                        session.add_dataset(cfg=f"""
type=remote
name=remote3
path={url}/dataset/remote3
server={url}
""")
                    self.assertEqual(session.matcher("level:g00 or g01").expanded,
                                     "level:GRIB1,1 or GRIB2S,1,0,0 or GRIB1,2 or GRIB2S,2,0,0")

                    self.assertFalse(session.has_dataset("remote3"))
