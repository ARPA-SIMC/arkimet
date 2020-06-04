from __future__ import annotations
import unittest
import arkimet as arki


class TestSession(unittest.TestCase):
    def test_aliases(self):
        with arki.dataset.Session(load_aliases=False) as session:
            with self.assertRaises(RuntimeError):
                session.matcher("origin:test")

            session.load_aliases({
                "origin": {
                    "test": "GRIB1 or GRIB2",
                },
            })

            self.assertIsInstance(session.matcher("origin:test"), arki.Matcher)
