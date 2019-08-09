import unittest
import os
import arkimet as arki
import datetime


class TestScanGrib(unittest.TestCase):
    def read(self, pathname, format="grib"):
        """
        Read all the metadata from a file
        """
        ds = arki.dataset.Reader({
            "format": format,
            "name": os.path.basename(pathname),
            "path": pathname,
            "type": "file",
        })
        return ds.query_data()

    def assertGribSource(self, md, filename, offset, size):
        source = md.to_python("source")
        self.assertEqual(source, {
            "type": "source",
            "style": "BLOB",
            "format": "grib",
            "basedir": os.getcwd(),
            "file": filename,
            "offset": offset,
            "size": size,
        })

        data = md.data
        self.assertEqual(len(data), size)
        self.assertEqual(data[:4], b"GRIB")
        self.assertEqual(data[-4:], b"7777")

    def test_compact(self):
        """
        Scan a well-known grib file, with no padding between messages
        """
        mds = self.read("inbound/test.grib1")
        self.assertEqual(len(mds), 3)

        # First grib
        md = mds[0]

        self.assertGribSource(md, "inbound/test.grib1", 0, 7218)

        # Check notes
        notes = md.get_notes()
        self.assertEqual(len(notes), 1)
        self.assertEqual(notes[0]["type"], "note")
        self.assertEqual(notes[0]["value"], "Scanned from test.grib1:0+7218")
        self.assertIsInstance(notes[0]["time"], datetime.datetime)

        self.assertEqual(md["origin"], "GRIB1(200, 000, 101)")
        self.assertEqual(md["product"], "GRIB1(200, 140, 229)")
        self.assertEqual(md["level"], "GRIB1(001)")
        self.assertEqual(md["timerange"], "GRIB1(000, 000h)")
        self.assertEqual(
                md["area"],
                "GRIB(Ni=97, Nj=73, latfirst=40000000, latlast=46000000, lonfirst=12000000, lonlast=20000000, type=0)")
        self.assertEqual(md["proddef"], "GRIB(tod=1)")
        self.assertEqual(md["reftime"], "2007-07-08T13:00:00Z")
        self.assertEqual(md["run"], "MINUTE(13:00)")

        # Next grib
        md = mds[1]

        self.assertGribSource(md, "inbound/test.grib1", 7218, 34960)

        self.assertEqual(md["origin"], "GRIB1(080, 255, 100)")
        self.assertEqual(md["product"], "GRIB1(080, 002, 002)")
        self.assertEqual(md["level"], "GRIB1(102)")
        self.assertEqual(md["timerange"], "GRIB1(001)")
        self.assertEqual(
                md["area"],
                "GRIB(Ni=205, Nj=85, latfirst=30000000, latlast=72000000,"
                " lonfirst=-60000000, lonlast=42000000, type=0)")
        self.assertEqual(md["proddef"], "GRIB(tod=1)")
        self.assertEqual(md["reftime"], "2007-07-07T00:00:00Z")
        self.assertEqual(md["run"], "MINUTE(00:00)")

        # Last grib
        md = mds[2]

        self.assertGribSource(md, "inbound/test.grib1", 42178, 2234)

        self.assertEqual(md["origin"], "GRIB1(098, 000, 129)")
        self.assertEqual(md["product"], "GRIB1(098, 128, 129)")
        self.assertEqual(md["level"], "GRIB1(100, 01000)")
        self.assertEqual(md["timerange"], "GRIB1(000, 000h)")
        self.assertEqual(
                md["area"],
                "GRIB(Ni=43, Nj=25, latfirst=55500000, latlast=31500000, lonfirst=-11500000, lonlast=30500000, type=0)")
        self.assertEqual(md["proddef"], "GRIB(tod=1)")
        self.assertEqual(md["reftime"], "2007-10-09T00:00:00Z")
        self.assertEqual(md["run"], "MINUTE(00:00)")
