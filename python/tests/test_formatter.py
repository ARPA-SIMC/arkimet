import unittest
import arkimet as arki
from arkimet.test import skip_unless_vm2


class TestFormatter(unittest.TestCase):
    def test_origin(self):
        formatter = arki.Formatter()
        res = formatter.format({"type": "origin", "style": "GRIB1", "centre": 200, "subcentre": 0, "process": 0})
        self.assertEqual(res, "GRIB1 from ARPAE SIMC Emilia Romagna, subcentre 0, process 0")

    def test_vm2(self):
        skip_unless_vm2()

        ds = arki.dataset.Reader({
            "format": "vm2",
            "name": "fixture.vm2",
            "path": "inbound/fixture.vm2",
            "type": "file",
        })
        mds = ds.query_data()
        formatter = arki.Formatter()
        res = formatter.format(mds[0].to_python("area"))
        self.assertEqual(res, "id=1, lat=4460016, lon=1207738, rep=locali")

        res = formatter.format(mds[0].to_python("product"))
        self.assertEqual(
                res, "id=227, bcode=B11002, l1=10000, l2=0, lt1=103, lt2=0, p1=86400, p2=86400, tr=0, unit=M/S")
