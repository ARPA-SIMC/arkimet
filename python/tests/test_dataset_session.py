from __future__ import annotations
import unittest
import arkimet as arki


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
