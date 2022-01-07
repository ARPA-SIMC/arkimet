import unittest
import os
import datetime
from arkimet.test import SessionMixin


class TestScanNetCDF(SessionMixin, unittest.TestCase):
    def read(self, pathname, size, format="nc"):
        """
        Read all the metadata from a file
        """
        with self.session.dataset_reader(cfg={
                        "format": format,
                        "name": os.path.basename(pathname),
                        "path": pathname,
                        "type": "file",
                    }) as ds:
            mds = ds.query_data()
        self.assertEqual(len(mds), 1)
        md = mds[0]

        source = md.to_python("source")
        self.assertEqual(source, {
            "type": "source",
            "style": "BLOB",
            "format": "nc",
            "basedir": os.getcwd(),
            "file": pathname,
            "offset": 0,
            "size": size,
        })

        data = md.data
        self.assertEqual(len(data), size)
        self.assertIn(data[1:3], (b"HD", b"DF"))

        notes = md.get_notes()
        self.assertEqual(len(notes), 1)
        self.assertEqual(notes[0]["type"], "note")
        self.assertEqual(notes[0]["value"], "Scanned from {}".format(os.path.basename(pathname)))
        self.assertIsInstance(notes[0]["time"], datetime.datetime)
        return md

    def assert_empty1_contents(self, md):
        self.assertNotIn("origin", md)
        self.assertNotIn("product", md)
        self.assertNotIn("level", md)
        self.assertNotIn("task", md)
        # TODO: when quantity becomes product, reactivate this
        # self.assertNotIn("quantity", md)
        # TODO: when area scanning finalizes out of a stub, fix this
        # self.assertNotIn("area", md)
        self.assertEqual(md["reftime"], "2019-06-10T00:30:00Z")
        # self.assertEqual(md["origin"], "ODIMH5(wmo, rad, plc)")
        # self.assertEqual(md["product"], "ODIMH5(PVOL, SCAN)")
        # self.assertEqual(md["level"], "ODIMH5(0, 27)")
        # self.assertEqual(md["task"], "task")
        # self.assertEqual(md["quantity"],
        #                  "ACRR, BRDR, CLASS, DBZH, DBZV, HGHT, KDP, LDR, PHIDP, QIND, RATE, RHOHV,"
        #                  " SNR, SQI, TH, TV, UWND, VIL, VRAD, VWND, WRAD, ZDR, ad, ad_dev, chi2,"
        #                  " dbz, dbz_dev, dd, dd_dev, def, def_dev, div, div_dev, ff, ff_dev, n,"
        #                  " rhohv, rhohv_dev, w, w_dev, z, z_dev")
        # self.assertEqual(md["area"], "ODIMH5(lat=44456700, lon=11623600, radius=1000)")
        # self.assertEqual(md["reftime"], "2000-01-02T03:04:05Z")
        self.assertNotIn("run", md)
        self.assertNotIn("timerange", md)

    def test_nc3(self):
        """
        Scan a NetCDF 3 file
        """
        md = self.read("inbound/netcdf/empty1-3.nc", 1636)
        self.assert_empty1_contents(md)

    def test_nc4(self):
        """
        Scan a NetCDF 4 file
        """
        md = self.read("inbound/netcdf/empty1-4.nc", 9487)
        self.assert_empty1_contents(md)

    # NetCDF 5 does not seem supported on Centos 7 (see https://github.com/ARPA-SIMC/arkimet/issues/243)
    # and we currently do not need to support it.
    #
    # def test_nc5(self):
    #     """
    #     Scan a NetCDF 5 file
    #     """
    #     md = self.read("inbound/netcdf/empty1-5.nc", 1988)
    #     self.assert_empty1_contents(md)

    def test_nc6(self):
        """
        Scan a NetCDF 64-bit file
        """
        md = self.read("inbound/netcdf/empty1-6.nc", 1648)
        self.assert_empty1_contents(md)

    def test_nc7(self):
        """
        Scan a NetCDF 4 classic model file
        """
        md = self.read("inbound/netcdf/empty1-7.nc", 9545)
        self.assert_empty1_contents(md)

    def test_empty(self):
        """
        Check that the scanner silently discard an empty file
        """
        with self.session.dataset_reader(cfg={
                        "format": "nc",
                        "name": "empty.nc",
                        "path": "inbound/netcdf/empty.nc",
                        "type": "file",
                    }) as ds:
            mds = ds.query_data()
            self.assertEqual(len(mds), 0)

    def test_default_fixture(self):
        """
        Scan the files in the default fixture for dataset tests
        """
        md = self.read("inbound/fixture.nc/00.nc", 9544)
        self.assertEqual(md["reftime"], "2007-07-08T13:00:00Z")
        md = self.read("inbound/fixture.nc/01.nc", 9544)
        self.assertEqual(md["reftime"], "2007-07-07T00:00:00Z")
        md = self.read("inbound/fixture.nc/02.nc", 9544)
        self.assertEqual(md["reftime"], "2007-10-09T00:00:00Z")
