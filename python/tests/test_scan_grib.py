import unittest
import os
import arkimet as arki
from arkimet.test import skip_unless_arpae_tests
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

    def test_issue120_and_issue201(self):
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
        # issue201: set pl and pt
        self.assertEqual(md["proddef"], "GRIB(pl=-1,3,0,0, pt=3, tod=5)")
        self.assertEqual(md["reftime"], "2018-01-25T21:00:00Z")
        self.assertEqual(md["run"], "MINUTE(21:00)")

    def test_issue264(self):
        mds = self.read("inbound/issue264.grib")
        self.assertEqual(len(mds), 1)
        md = mds[0]

        self.assertGribSource(md, "inbound/issue264.grib", 0, 179)
        self.assertEqual(md["origin"], "GRIB2(00098, 00000, 000, 255, 254)")
        self.assertEqual(md["product"], "GRIB2(00098, 003, 000, 000, 004, 000)")
        self.assertEqual(md["level"], "GRIB2S(  -,   -,          -)")
        self.assertNotIn("timerange", md)
        self.assertEqual(md["area"], "GRIB(tn=90)")
        # issue201: set pl and pt
        self.assertEqual(md["proddef"], "GRIB(tod=0)")
        self.assertEqual(md["reftime"], "2021-03-05T23:00:00Z")
        self.assertEqual(md["run"], "MINUTE(23:00)")

    def test_ninfa(self):
        """
        Check scanning of some Timedef cases
        """
        skip_unless_arpae_tests()

        mds = self.read("inbound/ninfa_ana.grib2")
        self.assertEqual(len(mds), 1)
        self.assertEqual(mds[0]["timerange"], "Timedef(0s, 254, 0s)")

        mds = self.read("inbound/ninfa_forc.grib2")
        self.assertEqual(len(mds), 1)
        self.assertEqual(mds[0]["timerange"], "Timedef(3h, 254, 0s)")

    def test_utm_areas(self):
        """
        Scan a GRIB2 with experimental UTM areas
        """
        skip_unless_arpae_tests()

        mds = self.read("inbound/calmety_20110215.grib2")
        self.assertEqual(len(mds), 1)

        self.assertEqual(mds[0]["origin"], "GRIB2(00200, 00000, 000, 000, 203)")
        self.assertEqual(mds[0]["product"], "GRIB2(00200, 000, 200, 033, 005, 000)")
        self.assertEqual(mds[0]["level"], "GRIB2S(103, 000, 0000000010)")
        self.assertEqual(mds[0]["timerange"], "Timedef(0s, 254, 0s)")
        self.assertEqual(
                mds[0]["area"],
                "GRIB(Ni=90, Nj=52, fe=0, fn=0, latfirst=4852500, latlast=5107500, lonfirst=402500, lonlast=847500,"
                " tn=32768, utm=1, zone=32)")
        self.assertEqual(mds[0]["proddef"], "GRIB(tod=0)")
        self.assertEqual(mds[0]["reftime"], "2011-02-15T00:00:00Z")
        self.assertEqual(mds[0]["run"], "MINUTE(00:00)")

    def test_cosmo_nudging(self):
        """
        Check scanning COSMO nudging timeranges
        """
        skip_unless_arpae_tests()

        mds = self.read("inbound/cosmonudging-t2.grib1")
        self.assertEqual(len(mds), 35)
        for i in range(5):
            self.assertEqual(mds[i]["timerange"], "Timedef(0s, 254, 0s)")
        self.assertEqual(mds[5]["timerange"], "Timedef(0s, 2, 1h)")
        self.assertEqual(mds[6]["timerange"], "Timedef(0s, 3, 1h)")
        for i in range(7, 13):
            self.assertEqual(mds[i]["timerange"], "Timedef(0s, 254, 0s)")
        self.assertEqual(mds[13]["timerange"], "Timedef(0s, 1, 12h)")
        for i in range(14, 19):
            self.assertEqual(mds[i]["timerange"], "Timedef(0s, 254, 0s)")
        for i in range(19, 21):
            self.assertEqual(mds[i]["timerange"], "Timedef(0s, 1, 12h)")
        for i in range(21, 26):
            self.assertEqual(mds[i]["timerange"], "Timedef(0s, 254, 0s)")
        self.assertEqual(mds[26]["timerange"], "Timedef(0s, 1, 12h)")
        for i in range(27, 35):
            self.assertEqual(mds[i]["timerange"], "Timedef(0s, 0, 12h)")

        mds = self.read("inbound/cosmonudging-t201.grib1")
        self.assertEqual(len(mds), 33)
        for i in range(0, 3):
            self.assertEqual(mds[i]["timerange"], "Timedef(0s, 0, 12h)")
        for i in range(3, 16):
            self.assertEqual(mds[i]["timerange"], "Timedef(0s, 254, 0s)")
        for i in range(16, 18):
            self.assertEqual(mds[i]["timerange"], "Timedef(0s, 1, 12h)")
        for i in range(18, 26):
            self.assertEqual(mds[i]["timerange"], "Timedef(0s, 254, 0s)")
        self.assertEqual(mds[26]["timerange"], "Timedef(0s, 2, 1h)")
        for i in range(27, 33):
            self.assertEqual(mds[i]["timerange"], "Timedef(0s, 254, 0s)")

        mds = self.read("inbound/cosmonudging-t202.grib1")
        self.assertEqual(len(mds), 11)
        for i in range(0, 11):
            self.assertEqual(mds[i]["timerange"], "Timedef(0s, 254, 0s)")

        mds = self.read("inbound/cosmonudging-t203.grib1")
        self.assertEqual(mds[0]["timerange"], "Timedef(0s, 254, 0s)")

        mds = self.read("inbound/cosmo/anist_1.grib")
        self.assertEqual(mds[0]["timerange"], "Timedef(0s, 254, 0s)")
        self.assertEqual(mds[0]["proddef"], "GRIB(tod=0)")

        mds = self.read("inbound/cosmo/anist_1.grib2")
        self.assertEqual(mds[0]["timerange"], "Timedef(0s, 254, 0s)")
        self.assertEqual(mds[0]["proddef"], "GRIB(tod=0)")

        mds = self.read("inbound/cosmo/fc0ist_1.grib")
        self.assertEqual(mds[0]["timerange"], "GRIB1(000, 000h)")
        # self.assertEqual(mds[0]["timerange"], "Timedef(0s, 254, 0s)")
        self.assertEqual(mds[0]["proddef"], "GRIB(tod=1)")

        mds = self.read("inbound/cosmo/anproc_1.grib")
        self.assertEqual(mds[0]["timerange"], "Timedef(0s, 1, 1h)")
        self.assertEqual(mds[0]["proddef"], "GRIB(tod=0)")

        mds = self.read("inbound/cosmo/anproc_2.grib")
        self.assertEqual(mds[0]["timerange"], "Timedef(0s, 0, 1h)")
        self.assertEqual(mds[0]["proddef"], "GRIB(tod=0)")

        mds = self.read("inbound/cosmo/anproc_3.grib")
        self.assertEqual(mds[0]["timerange"], "Timedef(0s, 2, 1h)")
        self.assertEqual(mds[0]["proddef"], "GRIB(tod=0)")

        mds = self.read("inbound/cosmo/anproc_4.grib")
        self.assertEqual(mds[0]["timerange"], "Timedef(0s, 0, 12h)")
        self.assertEqual(mds[0]["proddef"], "GRIB(tod=0)")

        mds = self.read("inbound/cosmo/anproc_1.grib2")
        self.assertEqual(mds[0]["timerange"], "Timedef(0s, 0, 24h)")
        self.assertEqual(mds[0]["proddef"], "GRIB(tod=0)")

        mds = self.read("inbound/cosmo/anproc_2.grib2")
        self.assertEqual(mds[0]["timerange"], "Timedef(0s, 1, 24h)")
        self.assertEqual(mds[0]["proddef"], "GRIB(tod=0)")

        mds = self.read("inbound/cosmo/fcist_1.grib")
        self.assertEqual(mds[0]["timerange"], "GRIB1(000, 006h)")
        # self.assertEqual(mds[0]["timerange"], "Timedef(6h,254,0s)")
        self.assertEqual(mds[0]["proddef"], "GRIB(tod=1)")

        mds = self.read("inbound/cosmo/fcist_1.grib2")
        self.assertEqual(mds[0]["timerange"], "Timedef(6h, 254, 0s)")
        self.assertEqual(mds[0]["proddef"], "GRIB(tod=1)")

        mds = self.read("inbound/cosmo/fcproc_1.grib")
        # self.assertEqual(mds[0]["timerange"], "Timedef(6h, 1, 6h)")
        self.assertEqual(mds[0]["timerange"], "GRIB1(004, 000h, 006h)")
        self.assertEqual(mds[0]["proddef"], "GRIB(tod=1)")

        mds = self.read("inbound/cosmo/fcproc_3.grib2")
        self.assertEqual(mds[0]["timerange"], "Timedef(48h, 1, 24h)")
        self.assertEqual(mds[0]["proddef"], "GRIB(tod=1)")

    def test_meteosat(self):
        """
        Check scanning meteosat GRIB images
        """
        # See issue #268
        # More test data can be found at https://github.com/ARPA-SIMC/qc_sample_data/blob/master/satellite/grib/

        mds = self.read("inbound/meteosat/MSG4_IR_016.grib")
        self.assertEqual(len(mds), 1)
        md = mds[0]
        self.assertEqual(md["origin"], "GRIB2(00098, 00000, 000, 255, 254)")
        self.assertEqual(md["product"], "GRIB2(00098, 003, 000, 000, 004, 000)")
        self.assertEqual(md["level"], "GRIB2S(  -,   -,          -)")
        self.assertNotIn("timerange", md)
        self.assertEqual(md["area"], "GRIB(tn=90)")
        self.assertEqual(md["proddef"], "GRIB(ch=IR016, sat=MSG4, tod=0)")
        self.assertEqual(md["reftime"], "2020-02-23T11:00:00Z")
        self.assertEqual(md["run"], "MINUTE(11:00)")

        mds = self.read("inbound/meteosat/MSG4_IR_039.grib")
        self.assertEqual(len(mds), 1)
        md = mds[0]
        self.assertEqual(md["origin"], "GRIB2(00098, 00000, 000, 255, 254)")
        self.assertEqual(md["product"], "GRIB2(00098, 003, 000, 000, 004, 000)")
        self.assertEqual(md["level"], "GRIB2S(  -,   -,          -)")
        self.assertNotIn("timerange", md)
        self.assertEqual(md["area"], "GRIB(tn=90)")
        self.assertEqual(md["proddef"], "GRIB(ch=IR039, sat=MSG4, tod=0)")
        self.assertEqual(md["reftime"], "2020-02-23T11:00:00Z")
        self.assertEqual(md["run"], "MINUTE(11:00)")

        mds = self.read("inbound/meteosat/MSG4_VIS006.grib")
        self.assertEqual(len(mds), 1)
        md = mds[0]
        self.assertEqual(md["origin"], "GRIB2(00098, 00000, 000, 255, 254)")
        self.assertEqual(md["product"], "GRIB2(00098, 003, 000, 000, 004, 000)")
        self.assertEqual(md["level"], "GRIB2S(  -,   -,          -)")
        self.assertNotIn("timerange", md)
        self.assertEqual(md["area"], "GRIB(tn=90)")
        self.assertEqual(md["proddef"], "GRIB(ch=VIS006, sat=MSG4, tod=0)")
        self.assertEqual(md["reftime"], "2020-02-23T11:00:00Z")
        self.assertEqual(md["run"], "MINUTE(11:00)")

        mds = self.read("inbound/meteosat/MSG4_WV_062.grib")
        self.assertEqual(len(mds), 1)
        md = mds[0]
        self.assertEqual(md["origin"], "GRIB2(00098, 00000, 000, 255, 254)")
        self.assertEqual(md["product"], "GRIB2(00098, 003, 000, 000, 004, 000)")
        self.assertEqual(md["level"], "GRIB2S(  -,   -,          -)")
        self.assertNotIn("timerange", md)
        self.assertEqual(md["area"], "GRIB(tn=90)")
        self.assertEqual(md["proddef"], "GRIB(ch=WV062, sat=MSG4, tod=0)")
        self.assertEqual(md["reftime"], "2020-02-23T11:00:00Z")
        self.assertEqual(md["run"], "MINUTE(11:00)")
