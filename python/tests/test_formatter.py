import unittest
import arkimet as arki


class TestFormatter(unittest.TestCase):
    def test_origin(self):
        formatter = arki.Formatter()
        res = formatter.format({"type": "origin", "style": "GRIB1", "centre": 200, "subcentre": 0, "process": 0})
        self.assertEqual(res, "GRIB1 from ARPAE SIMC Emilia Romagna, subcentre 0, process 0")
