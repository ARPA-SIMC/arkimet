import unittest
import os
import arkimet as arki
import datetime


class TestScanBufr(unittest.TestCase):
    def read(self, pathname, size, format="odimh5"):
        """
        Read all the metadata from a file
        """
        ds = arki.dataset.Reader({
            "format": format,
            "name": os.path.basename(pathname),
            "path": pathname,
            "type": "file",
        })
        mds = ds.query_data()
        self.assertEqual(len(mds), 1)
        md = mds[0]

        source = md.to_python("source")
        self.assertEqual(source, {
            "type": "source",
            "style": "BLOB",
            "format": "odimh5",
            "basedir": os.getcwd(),
            "file": pathname,
            "offset": 0,
            "size": size,
        })

        data = md.data
        self.assertEqual(len(data), size)
        self.assertEqual(data[1:4], b"HDF")

        notes = md.get_notes()
        self.assertEqual(len(notes), 1)
        self.assertEqual(notes[0]["type"], "note")
        self.assertEqual(notes[0]["value"], "Scanned from {}".format(os.path.basename(pathname)))
        self.assertIsInstance(notes[0]["time"], datetime.datetime)
        return md

    def test_pvol(self):
        """
        Scan an ODIMH5 polar volume
        """
        md = self.read("inbound/odimh5/PVOL_v20.h5", 320696)
        self.assertEqual(md["origin"], "ODIMH5(wmo, rad, plc)")
        self.assertEqual(md["product"], "ODIMH5(PVOL, SCAN)")
        self.assertEqual(md["level"], "ODIMH5(0, 27)")
        self.assertEqual(md["task"], "task")
        self.assertEqual(md["quantity"],
                         "ACRR, BRDR, CLASS, DBZH, DBZV, HGHT, KDP, LDR, PHIDP, QIND, RATE, RHOHV,"
                         " SNR, SQI, TH, TV, UWND, VIL, VRAD, VWND, WRAD, ZDR, ad, ad_dev, chi2,"
                         " dbz, dbz_dev, dd, dd_dev, def, def_dev, div, div_dev, ff, ff_dev, n,"
                         " rhohv, rhohv_dev, w, w_dev, z, z_dev")
        self.assertEqual(md["area"], "ODIMH5(lat=44456700, lon=11623600, radius=1000)")
        self.assertEqual(md["reftime"], "2000-01-02T03:04:05Z")
        self.assertNotIn("run", md)
        self.assertNotIn("timerange", md)
