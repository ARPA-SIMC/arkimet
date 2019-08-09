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

    def test_padded(self):
        """
        Scan a well-known grib file, with extra padding data between messages
        """
        mds = self.read("inbound/padded.grib1")
        self.assertEqual(len(mds), 3)

        # First grib
        md = mds[0]

        self.assertGribSource(md, "inbound/padded.grib1", 100, 7218)

        # Check notes
        notes = md.get_notes()
        self.assertEqual(len(notes), 1)
        self.assertEqual(notes[0]["type"], "note")
        self.assertEqual(notes[0]["value"], "Scanned from padded.grib1:100+7218")
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

        self.assertGribSource(md, "inbound/padded.grib1", 7418, 34960)

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

        self.assertGribSource(md, "inbound/padded.grib1", 42478, 2234)

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

    def test_layers(self):
        """
        Test scanning layers instead of levels
        """
        mds = self.read("inbound/layer.grib1")
        self.assertEqual(len(mds), 1)
        md = mds[0]

        self.assertGribSource(md, "inbound/layer.grib1", 0, 30682)
        self.assertEqual(md["origin"], "GRIB1(200, 255, 045)")
        self.assertEqual(md["product"], "GRIB1(200, 002, 033)")
        self.assertEqual(md["level"], "GRIB1(110, 001, 002)")
        self.assertEqual(md["timerange"], "Timedef(0s, 254, 0s)")
        self.assertEqual(
                md["area"],
                "GRIB(Ni=169, Nj=181, latfirst=-21125000, latlast=-9875000, latp=-32500000,"
                " lonfirst=-2937000, lonlast=7563000, lonp=10000000, rot=0, type=10)")
        self.assertEqual(md["proddef"], "GRIB(tod=0)")
        self.assertEqual(md["reftime"], "2009-09-02T00:00:00Z")
        self.assertEqual(md["run"], "MINUTE(00:00)")

    def test_proselvo(self):
        """
        Scan a know item for which grib_api changed behaviour
        """
        mds = self.read("inbound/proselvo.grib1")
        self.assertEqual(len(mds), 1)
        md = mds[0]

        self.assertGribSource(md, "inbound/proselvo.grib1", 0, 298)
        self.assertEqual(md["origin"], "GRIB1(080, 098, 131)")
        self.assertEqual(md["product"], "GRIB1(080, 002, 079)")
        self.assertEqual(md["level"], "GRIB1(001)")
        self.assertEqual(md["timerange"], "GRIB1(000, 000h)")
        self.assertEqual(
                md["area"],
                "GRIB(Ni=231, Nj=265, latfirst=-16125000, latlast=375000, latp=-40000000,"
                " lonfirst=-5125000, lonlast=9250000, lonp=10000000, rot=0, type=10)")
        self.assertEqual(md["proddef"], "GRIB(ld=1, mt=9, nn=0, tod=1)")
        self.assertEqual(md["reftime"], "2010-08-11T12:00:00Z")
        self.assertEqual(md["run"], "MINUTE(12:00)")

    def test_cleps(self):
        """
        Scan a know item for which grib_api changed behaviour
        """
        mds = self.read("inbound/cleps_pf16_HighPriority.grib2")
        self.assertEqual(len(mds), 1)
        md = mds[0]

        self.assertGribSource(md, "inbound/cleps_pf16_HighPriority.grib2", 0, 432)
        self.assertEqual(md["origin"], "GRIB2(00250, 00098, 004, 255, 131)")
        self.assertEqual(md["product"], "GRIB2(00250, 002, 000, 000, 005, 000)")
        self.assertEqual(md["level"], "GRIB2S(001,   -,          -)")
        self.assertEqual(md["timerange"], "Timedef(0s, 254, 0s)")
        self.assertEqual(
                md["area"],
                "GRIB(Ni=511, Nj=415, latfirst=-16125000, latlast=9750000, latp=-40000000,"
                " lonfirst=344250000, lonlast=16125000, lonp=10000000, rot=0, tn=1)")
        self.assertEqual(md["proddef"], "GRIB(mc=ti, mt=0, pf=16, tf=16, tod=4, ty=3)")
        self.assertEqual(md["reftime"], "2013-10-22T00:00:00Z")
        self.assertEqual(md["run"], "MINUTE(00:00)")

    def test_issue120(self):
        mds = self.read("inbound/oddunits.grib")
        self.assertEqual(len(mds), 1)
        md = mds[0]

        self.assertGribSource(md, "inbound/oddunits.grib", 0, 245)
        self.assertEqual(md["origin"], "GRIB2(00080, 00255, 005, 255, 015)")
        self.assertEqual(md["product"], "GRIB2(00080, 000, 001, 052, 011, 001)")
        self.assertEqual(md["level"], "GRIB2S(001,   -,          -)")
        self.assertEqual(md["timerange"], "Timedef(27h, 4, 24h)")
        self.assertEqual(
                md["area"],
                "GRIB(Ni=576, Nj=701, latfirst=-8500000, latlast=5500000, latp=-47000000,"
                " lonfirst=-3800000, lonlast=7700000, lonp=10000000, rot=0, tn=1)")
        self.assertEqual(md["proddef"], "GRIB(tod=5)")
        self.assertEqual(md["reftime"], "2018-01-25T21:00:00Z")
        self.assertEqual(md["run"], "MINUTE(21:00)")
