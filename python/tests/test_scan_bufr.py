import unittest
import os
import arkimet as arki


class TestScanBufr(unittest.TestCase):
    def read(self, pathname, format="bufr"):
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

    def assertBufrSource(self, md, filename, offset, size):
        source = md.to_python("source")
        self.assertEqual(source, {
            "type": "source",
            "style": "BLOB",
            "format": "bufr",
            "basedir": os.getcwd(),
            "file": filename,
            "offset": offset,
            "size": size,
        })

        data = md.data
        self.assertEqual(len(data), size)
        self.assertEqual(data[:4], b"BUFR")
        self.assertEqual(data[-4:], b"7777")

    def test_contiguous(self):
        """
        Scan a well-known bufr file, with no padding between messages
        """
        mds = self.read("inbound/test.bufr")
        self.assertEqual(len(mds), 3)

        # First grib
        md = mds[0]

        self.assertBufrSource(md, "inbound/test.bufr", 0, 194)

        # Check notes
        notes = md.get_notes()
        self.assertFalse(notes)
        # self.assertEqual(len(notes), 1)
        # self.assertEqual(notes[0]["type"], "note")
        # self.assertEqual(notes[0]["value"], "Scanned from test.bufr:0+194")
        # self.assertIsInstance(notes[0]["time"], datetime.datetime)
        self.assertEqual(md["origin"], "BUFR(098, 000)")
        self.assertEqual(md["product"], "BUFR(000, 255, 001, t=synop)")
        self.assertEqual(md["area"], "GRIB(lat=4153000, lon=2070000)")
        self.assertEqual(md["proddef"], "GRIB(blo=13, sta=577)")
        self.assertEqual(md["reftime"], "2005-12-01T18:00:00Z")
        self.assertNotIn("run", md)
        self.assertNotIn("level", md)
        self.assertNotIn("timerange", md)

        # Next grib
        md = mds[1]
        self.assertBufrSource(md, "inbound/test.bufr", 194, 220)
        self.assertEqual(md["origin"], "BUFR(098, 000)")
        self.assertEqual(md["product"], "BUFR(000, 255, 001, t=synop)")
        self.assertEqual(md["area"], "GRIB(lat=3388000, lon=-553000)")
        self.assertEqual(md["proddef"], "GRIB(blo=60, sta=150)")
        self.assertEqual(md["reftime"], "2004-11-30T12:00:00Z")

        # Last grib
        md = mds[2]
        self.assertBufrSource(md, "inbound/test.bufr", 414, 220)
        self.assertEqual(md["origin"], "BUFR(098, 000)")
        self.assertEqual(md["product"], "BUFR(000, 255, 003, t=synop)")
        self.assertEqual(md["area"], "GRIB(lat=4622000, lon=733000)")
        self.assertEqual(md["proddef"], "GRIB(blo=6, sta=720)")
        self.assertEqual(md["reftime"], "2004-11-30T12:00:00Z")

    def test_padded(self):
        """
        Scan a well-known bufr file, with extar padding data between messages
        """
        mds = self.read("inbound/padded.bufr")
        self.assertEqual(len(mds), 3)

        # First grib
        md = mds[0]
        self.assertBufrSource(md, "inbound/padded.bufr", 100, 194)
        self.assertEqual(md["origin"], "BUFR(098, 000)")
        self.assertEqual(md["product"], "BUFR(000, 255, 001, t=synop)")
        self.assertEqual(md["area"], "GRIB(lat=4153000, lon=2070000)")
        self.assertEqual(md["proddef"], "GRIB(blo=13, sta=577)")
        self.assertEqual(md["reftime"], "2005-12-01T18:00:00Z")
        self.assertNotIn("run", md)
        self.assertNotIn("level", md)
        self.assertNotIn("timerange", md)

        # Next grib
        md = mds[1]

        self.assertBufrSource(md, "inbound/padded.bufr", 394, 220)
        self.assertEqual(md["origin"], "BUFR(098, 000)")
        self.assertEqual(md["product"], "BUFR(000, 255, 001, t=synop)")
        self.assertEqual(md["area"], "GRIB(lat=3388000, lon=-553000)")
        self.assertEqual(md["proddef"], "GRIB(blo=60, sta=150)")
        self.assertEqual(md["reftime"], "2004-11-30T12:00:00Z")

        # Last grib
        md = mds[2]

        self.assertBufrSource(md, "inbound/padded.bufr", 714, 220)
        self.assertEqual(md["origin"], "BUFR(098, 000)")
        self.assertEqual(md["product"], "BUFR(000, 255, 003, t=synop)")
        self.assertEqual(md["area"], "GRIB(lat=4622000, lon=733000)")
        self.assertEqual(md["proddef"], "GRIB(blo=6, sta=720)")
        self.assertEqual(md["reftime"], "2004-11-30T12:00:00Z")

    def test_partial(self):
        """
        Test scanning a BUFR file that can only be decoded partially
        (updated: it can now be fully decoded)
        """
        mds = self.read("inbound/C23000.bufr")
        self.assertEqual(len(mds), 1)

        md = mds[0]
        self.assertBufrSource(md, "inbound/C23000.bufr", 0, 2206)
        self.assertEqual(md["origin"], "BUFR(098, 000)")
        self.assertEqual(md["product"], "BUFR(002, 255, 101, t=temp)")
        self.assertEqual(md["area"], "GRIB(lat=5090000, lon=32000)")
        self.assertEqual(md["proddef"], "GRIB(blo=3, sta=882)")
        self.assertEqual(md["reftime"], "2010-07-21T23:00:00Z")
        self.assertNotIn("run", md)
        self.assertNotIn("level", md)
        self.assertNotIn("timerange", md)

    def test_pollution(self):
        """
        Test scanning a pollution BUFR file
        """
        mds = self.read("inbound/pollution.bufr")
        self.assertEqual(len(mds), 1)

        md = mds[0]
        self.assertBufrSource(md, "inbound/pollution.bufr", 0, 178)
        self.assertEqual(md["origin"], "BUFR(098, 000)")
        self.assertEqual(md["product"], "BUFR(008, 255, 171, p=NO2, t=pollution)")
        self.assertEqual(md["area"], "GRIB(lat=4601194, lon=826889)")
        self.assertEqual(md["proddef"], "GRIB(gems=IT0002, name=NO_3118_PIEVEVERGONTE)")
        self.assertEqual(md["reftime"], "2010-08-08T23:00:00Z")
        self.assertNotIn("run", md)
        self.assertNotIn("level", md)
        self.assertNotIn("timerange", md)

    def test_zerodate(self):
        """
        Test scanning a BUFR file with undefined dates
        """
        mds = self.read("inbound/zerodate.bufr")
        self.assertEqual(len(mds), 1)
        self.assertNotIn("reftime", mds[0])

    def test_ship(self):
        """
        Test scanning a ship bulletin
        """
        mds = self.read("inbound/ship.bufr")
        self.assertEqual(len(mds), 1)

        md = mds[0]
        self.assertBufrSource(md, "inbound/ship.bufr", 0, 198)
        self.assertEqual(md["origin"], "BUFR(098, 000)")
        self.assertEqual(md["product"], "BUFR(001, 255, 011, t=ship)")
        self.assertEqual(md["area"], "GRIB(type=mob, x=-11, y=37)")
        self.assertEqual(md["proddef"], "GRIB(id=DHDE)")
        self.assertEqual(md["reftime"], "2004-11-30T12:00:00Z")
        self.assertNotIn("run", md)
        self.assertNotIn("level", md)
        self.assertNotIn("timerange", md)

    def test_amdar(self):
        """
        Test scanning an amdar
        """
        mds = self.read("inbound/amdar.bufr")
        self.assertEqual(len(mds), 1)

        md = mds[0]
        self.assertBufrSource(md, "inbound/amdar.bufr", 0, 162)
        self.assertEqual(md["origin"], "BUFR(098, 000)")
        self.assertEqual(md["product"], "BUFR(004, 255, 144, t=amdar)")
        self.assertEqual(md["area"], "GRIB(type=mob, x=21, y=64)")
        self.assertEqual(md["proddef"], "GRIB(id=EU4444)")
        self.assertEqual(md["reftime"], "2004-11-30T12:00:00Z")
        self.assertNotIn("run", md)
        self.assertNotIn("level", md)
        self.assertNotIn("timerange", md)

    def test_airep(self):
        """
        Test scanning an airep
        """
        mds = self.read("inbound/airep.bufr")
        self.assertEqual(len(mds), 1)

        md = mds[0]
        self.assertBufrSource(md, "inbound/airep.bufr", 0, 162)
        self.assertEqual(md["origin"], "BUFR(098, 000)")
        self.assertEqual(md["product"], "BUFR(004, 255, 142, t=airep)")
        self.assertEqual(md["area"], "GRIB(type=mob, x=-54, y=51)")
        self.assertEqual(md["proddef"], "GRIB(id=ACA872)")
        self.assertEqual(md["reftime"], "2005-09-01T00:09:00Z")
        self.assertNotIn("run", md)
        self.assertNotIn("level", md)
        self.assertNotIn("timerange", md)

    def test_acars(self):
        """
        Test scanning an acars
        """
        mds = self.read("inbound/acars.bufr")
        self.assertEqual(len(mds), 1)

        md = mds[0]
        self.assertBufrSource(md, "inbound/acars.bufr", 0, 238)
        self.assertEqual(md["origin"], "BUFR(098, 000)")
        self.assertEqual(md["product"], "BUFR(004, 255, 145, t=acars)")
        self.assertEqual(md["area"], "GRIB(type=mob, x=-88, y=39)")
        self.assertEqual(md["proddef"], "GRIB(id=JBNYR3RA)")
        self.assertEqual(md["reftime"], "2005-09-01T00:00:00Z")
        self.assertNotIn("run", md)
        self.assertNotIn("level", md)
        self.assertNotIn("timerange", md)

    def test_gts(self):
        """
        Test scanning a GTS synop
        """
        mds = self.read("inbound/synop-gts.bufr")
        self.assertEqual(len(mds), 1)

        md = mds[0]
        self.assertBufrSource(md, "inbound/synop-gts.bufr", 0, 220)
        self.assertEqual(md["origin"], "BUFR(074, 000)")
        self.assertEqual(md["product"], "BUFR(000, 000, 000, t=synop)")
        self.assertEqual(md["area"], "GRIB(lat=4586878, lon=717080)")
        self.assertEqual(md["proddef"], "GRIB(blo=6, sta=717)")
        self.assertEqual(md["reftime"], "2009-12-04T20:00:00Z")
        self.assertNotIn("run", md)
        self.assertNotIn("level", md)
        self.assertNotIn("timerange", md)

    def test_date_mismatch(self):
        """
        Test scanning a message with a different date in the header than in its contents
        """
        mds = self.read("inbound/synop-gts-different-date-in-header.bufr")
        self.assertEqual(len(mds), 1)

        md = mds[0]
        self.assertBufrSource(md, "inbound/synop-gts-different-date-in-header.bufr", 0, 220)
        self.assertEqual(md["origin"], "BUFR(074, 000)")
        self.assertEqual(md["product"], "BUFR(000, 000, 000, t=synop)")
        self.assertEqual(md["area"], "GRIB(lat=4586878, lon=717080)")
        self.assertEqual(md["proddef"], "GRIB(blo=6, sta=717)")
        self.assertEqual(md["reftime"], "2009-12-04T20:00:00Z")
        self.assertNotIn("run", md)
        self.assertNotIn("level", md)
        self.assertNotIn("timerange", md)

    def test_out_of_range(self):
        """
        Test scanning a message which raises domain errors when interpreted
        """
        mds = self.read("inbound/interpreted-range.bufr")
        self.assertEqual(len(mds), 1)

        md = mds[0]
        self.assertBufrSource(md, "inbound/interpreted-range.bufr", 0, 198)
        self.assertEqual(md["origin"], "BUFR(098, 000)")
        self.assertEqual(md["product"], "BUFR(001, 255, 013, t=ship)")
        self.assertEqual(md["area"], "GRIB(type=mob, x=10, y=53)")
        self.assertEqual(md["proddef"], "GRIB(id=DBBC)")
        self.assertEqual(md["reftime"], "2012-05-15T12:00:00Z")
        self.assertNotIn("run", md)
        self.assertNotIn("level", md)
        self.assertNotIn("timerange", md)

    def test_temp_reftime(self):
        """
        Test scanning a temp forecast, to see if we got the right reftime
        """
        # BUFR has datetime 2009-02-13 12:00:00, timerange instant
        mds = self.read("inbound/tempforecast.bufr")
        self.assertEqual(len(mds), 1)

        md = mds[0]
        self.assertBufrSource(md, "inbound/tempforecast.bufr", 0, 1778)
        self.assertEqual(md["origin"], "BUFR(200, 000)")
        self.assertEqual(md["product"], "BUFR(255, 255, 000, t=temp)")
        self.assertEqual(md["area"], "GRIB(lat=4502770, lon=966670)")
        self.assertEqual(md["proddef"], "GRIB(blo=0, sta=101)")
        self.assertEqual(md["reftime"], "2009-02-13T12:00:00Z")
        self.assertNotIn("run", md)
        self.assertNotIn("level", md)
        self.assertNotIn("timerange", md)

        # BUFR has datetime 2013-04-06 00:00:00 (validity time, in this case), timerange 254,259200,0 (+72h)
        # and should be archived with its emission time
        mds = self.read("inbound/tempforecast1.bufr")
        self.assertEqual(len(mds), 1)

        md = mds[0]
        self.assertBufrSource(md, "inbound/tempforecast1.bufr", 0, 1860)
        self.assertEqual(md["origin"], "BUFR(200, 000)")
        self.assertEqual(md["product"], "BUFR(255, 255, 000, t=temp)")
        self.assertEqual(md["area"], "GRIB(lat=4552620, lon=1224850)")
        self.assertEqual(md["proddef"], "GRIB(blo=0, sta=1)")
        self.assertEqual(md["reftime"], "2013-04-06T00:00:00Z")
        self.assertNotIn("run", md)
        self.assertNotIn("level", md)
        self.assertNotIn("timerange", md)

    def test_wrongdate(self):
        """
        Test scanning a bufr with all sorts of wrong dates
        """
        mds = self.read("inbound/wrongdate.bufr")
        self.assertEqual(len(mds), 6)

        self.assertBufrSource(mds[0], "inbound/wrongdate.bufr", 0, 242)
        self.assertNotIn("reftime", mds[0])
        self.assertNotIn("reftime", mds[1])
        self.assertNotIn("reftime", mds[2])
        self.assertNotIn("reftime", mds[3])
        self.assertNotIn("reftime", mds[4])
        self.assertNotIn("reftime", mds[5])

    def test_generic_forecast(self):
        """
        Test scanning a generic forecast. The generic scanner update the
        "t" key to the report name, add the timerange and the reftime is set to
        the emission time (validity time - p1)
        """
        mds = self.read("inbound/generic_forecast.bufr")
        self.assertEqual(len(mds), 1)

        md = mds[0]
        self.assertBufrSource(md, "inbound/generic_forecast.bufr", 0, 120)
        self.assertEqual(md["origin"], "BUFR(200, 000)")
        self.assertEqual(md["product"], "BUFR(255, 255, 000, t=test)")
        self.assertEqual(md["area"], "GRIB(lat=4448714, lon=1136473)")
        self.assertEqual(md["reftime"], "2019-09-19T00:00:00Z")
        self.assertEqual(md["timerange"], "Timedef(1h)")
        self.assertNotIn("run", md)
        self.assertNotIn("level", md)
        self.assertNotIn("proddef", md)
