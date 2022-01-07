import unittest
import os
import arkimet as arki
import datetime


class TestScanODIMH5(unittest.TestCase):
    def read(self, pathname, size, format="odimh5"):
        """
        Read all the metadata from a file
        """
        with arki.dataset.Session() as session:
            with session.dataset_reader(cfg={
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

    def test_pvol_without_how(self):
        """
        Scan an ODIMH5 polar volume without how group
        """
        md = self.read("inbound/odimh5/ZOUFPLAN.h5", 516709)
        self.assertEqual(md["origin"], "ODIMH5(16106, , )")
        self.assertEqual(md["product"], "ODIMH5(PVOL, SCAN)")
        self.assertEqual(md["level"], "ODIMH5(-0.2, 16)")
        self.assertEqual(md["task"], "DPC Standard Scan")
        self.assertEqual(md["quantity"], "DBZH, QIND, VRAD")
        self.assertEqual(md["area"], "ODIMH5(lat=46562500, lon=12970300, radius=0)")
        self.assertEqual(md["reftime"], "2020-05-29T15:30:00Z")
        self.assertNotIn("run", md)
        self.assertNotIn("timerange", md)

    def test_time_without_seconds(self):
        md = self.read("inbound/odimh5/000461.odimh5", 458556)
        self.assertEqual(md["reftime"], "2020-05-30T04:40:00Z")

    def test_comp_cappi(self):
        md = self.read("inbound/odimh5/COMP_CAPPI_v20.h5", 49113)
        self.assertEqual(md["origin"], "ODIMH5(16144, IY46, itspc)")
        self.assertEqual(md["product"], "ODIMH5(COMP, CAPPI)")
        self.assertEqual(md["level"], "GRIB1(105, 00010)")
        self.assertEqual(md["reftime"], "2013-03-18T14:30:00Z")
        self.assertEqual(md["task"], "ZLR-BB")
        self.assertEqual(md["quantity"], "DBZH")
        self.assertEqual(md["area"],
                         "GRIB(latfirst=42314117, latlast=46912151, lonfirst=8273203, lonlast=14987079, type=0)")

    def test_comp_etop(self):
        md = self.read("inbound/odimh5/COMP_ETOP_v20.h5", 49113)
        self.assertEqual(md["origin"], "ODIMH5(16144, IY46, itspc)")
        self.assertEqual(md["product"], "ODIMH5(COMP, ETOP)")
        self.assertEqual(md["reftime"], "2013-03-18T14:30:00Z")
        self.assertEqual(md["task"], "ZLR-BB")
        self.assertEqual(md["quantity"], "HGHT")
        self.assertEqual(md["area"],
                         "GRIB(latfirst=42314117, latlast=46912151, lonfirst=8273203, lonlast=14987079, type=0)")
        self.assertNotIn("level", md)

    def test_comp_lbm(self):
        md = self.read("inbound/odimh5/COMP_LBM_v20.h5", 49057)
        self.assertEqual(md["origin"], "ODIMH5(16144, IY46, itspc)")
        self.assertEqual(md["product"], "ODIMH5(COMP, NEW:LBM_ARPA)")
        self.assertEqual(md["reftime"], "2013-03-18T14:30:00Z")
        self.assertEqual(md["task"], "ZLR-BB")
        self.assertEqual(md["quantity"], "DBZH")
        self.assertEqual(md["area"],
                         "GRIB(latfirst=42314117, latlast=46912151, lonfirst=8273203, lonlast=14987079, type=0)")
        self.assertNotIn("level", md)

    def test_comp_max(self):
        md = self.read("inbound/odimh5/COMP_MAX_v20.h5", 49049)
        self.assertEqual(md["origin"], "ODIMH5(16144, IY46, itspc)")
        self.assertEqual(md["product"], "ODIMH5(COMP, MAX)")
        self.assertEqual(md["reftime"], "2013-03-18T14:30:00Z")
        self.assertEqual(md["task"], "ZLR-BB")
        self.assertEqual(md["quantity"], "DBZH")
        self.assertEqual(md["area"],
                         "GRIB(latfirst=42314117, latlast=46912151, lonfirst=8273203, lonlast=14987079, type=0)")
        self.assertNotIn("level", md)

    def test_comp_pcappi(self):
        md = self.read("inbound/odimh5/COMP_PCAPPI_v20.h5", 49113)
        self.assertEqual(md["origin"], "ODIMH5(16144, IY46, itspc)")
        self.assertEqual(md["product"], "ODIMH5(COMP, PCAPPI)")
        self.assertEqual(md["level"], "GRIB1(105, 00010)")
        self.assertEqual(md["reftime"], "2013-03-18T14:30:00Z")
        self.assertEqual(md["task"], "ZLR-BB")
        self.assertEqual(md["quantity"], "DBZH")
        self.assertEqual(md["area"],
                         "GRIB(latfirst=42314117, latlast=46912151, lonfirst=8273203, lonlast=14987079, type=0)")

    def test_comp_ppi(self):
        md = self.read("inbound/odimh5/COMP_PPI_v20.h5", 49113)
        self.assertEqual(md["origin"], "ODIMH5(16144, IY46, itspc)")
        self.assertEqual(md["product"], "ODIMH5(COMP, PPI)")
        self.assertEqual(md["level"], "ODIMH5(10, 10)")
        self.assertEqual(md["reftime"], "2013-03-18T14:30:00Z")
        self.assertEqual(md["task"], "ZLR-BB")
        self.assertEqual(md["quantity"], "DBZH")
        self.assertEqual(md["area"],
                         "GRIB(latfirst=42314117, latlast=46912151, lonfirst=8273203, lonlast=14987079, type=0)")

    def test_comp_rr(self):
        md = self.read("inbound/odimh5/COMP_RR_v20.h5", 49049)
        self.assertEqual(md["origin"], "ODIMH5(16144, IY46, itspc)")
        self.assertEqual(md["product"], "ODIMH5(COMP, RR)")
        self.assertEqual(md["timerange"], "Timedef(0s, 1, 3600s)")
        self.assertEqual(md["reftime"], "2013-03-18T14:30:00Z")
        self.assertEqual(md["task"], "ZLR-BB")
        self.assertEqual(md["quantity"], "ACRR")
        self.assertEqual(md["area"],
                         "GRIB(latfirst=42314117, latlast=46912151, lonfirst=8273203, lonlast=14987079, type=0)")
        self.assertNotIn("level", md)

    def test_comp_vil(self):
        md = self.read("inbound/odimh5/COMP_VIL_v20.h5", 49097)
        self.assertEqual(md["origin"], "ODIMH5(16144, IY46, itspc)")
        self.assertEqual(md["product"], "ODIMH5(COMP, VIL)")
        self.assertEqual(md["level"], "GRIB1(106, 010, 000)")
        self.assertEqual(md["reftime"], "2013-03-18T14:30:00Z")
        self.assertEqual(md["task"], "ZLR-BB")
        self.assertEqual(md["quantity"], "VIL")
        self.assertEqual(md["area"],
                         "GRIB(latfirst=42314117, latlast=46912151, lonfirst=8273203, lonlast=14987079, type=0)")
        self.assertNotIn("timerange", md)

    def test_image_cappi(self):
        md = self.read("inbound/odimh5/IMAGE_CAPPI_v20.h5", 49113)
        self.assertEqual(md["origin"], "ODIMH5(16144, IY46, itspc)")
        self.assertEqual(md["product"], "ODIMH5(IMAGE, CAPPI)")
        self.assertEqual(md["level"], "GRIB1(105, 00500)")
        self.assertEqual(md["reftime"], "2013-03-18T14:30:00Z")
        self.assertEqual(md["task"], "ZLR-BB")
        self.assertEqual(md["quantity"], "DBZH")
        self.assertEqual(md["area"],
                         "GRIB(latfirst=42314117, latlast=46912151, lonfirst=8273203, lonlast=14987079, type=0)")
        self.assertNotIn("timerange", md)

    def test_image_etop(self):
        md = self.read("inbound/odimh5/IMAGE_ETOP_v20.h5", 49113)
        self.assertEqual(md["origin"], "ODIMH5(16144, IY46, itspc)")
        self.assertEqual(md["product"], "ODIMH5(IMAGE, ETOP)")
        self.assertEqual(md["reftime"], "2013-03-18T14:30:00Z")
        self.assertEqual(md["task"], "ZLR-BB")
        self.assertEqual(md["quantity"], "HGHT")
        self.assertEqual(md["area"],
                         "GRIB(latfirst=42314117, latlast=46912151, lonfirst=8273203, lonlast=14987079, type=0)")
        self.assertNotIn("level", md)

    def test_image_hvmi(self):
        md = self.read("inbound/odimh5/IMAGE_HVMI_v20.h5", 68777)
        self.assertEqual(md["origin"], "ODIMH5(16144, IY46, itspc)")
        self.assertEqual(md["product"], "ODIMH5(IMAGE, HVMI)")
        self.assertEqual(md["reftime"], "2013-03-18T14:30:00Z")
        self.assertEqual(md["task"], "ZLR-BB")
        self.assertEqual(md["quantity"], "DBZH")
        self.assertEqual(md["area"],
                         "GRIB(latfirst=42314117, latlast=46912151, lonfirst=8273203, lonlast=14987079, type=0)")
        self.assertNotIn("level", md)

    def test_image_max(self):
        md = self.read("inbound/odimh5/IMAGE_MAX_v20.h5", 49049)
        self.assertEqual(md["origin"], "ODIMH5(16144, IY46, itspc)")
        self.assertEqual(md["product"], "ODIMH5(IMAGE, MAX)")
        self.assertEqual(md["reftime"], "2013-03-18T14:30:00Z")
        self.assertEqual(md["task"], "ZLR-BB")
        self.assertEqual(md["quantity"], "DBZH")
        self.assertEqual(md["area"],
                         "GRIB(latfirst=42314117, latlast=46912151, lonfirst=8273203, lonlast=14987079, type=0)")
        self.assertNotIn("level", md)

    def test_image_pcappi(self):
        md = self.read("inbound/odimh5/IMAGE_PCAPPI_v20.h5", 49113)
        self.assertEqual(md["origin"], "ODIMH5(16144, IY46, itspc)")
        self.assertEqual(md["product"], "ODIMH5(IMAGE, PCAPPI)")
        self.assertEqual(md["level"], "GRIB1(105, 00500)")
        self.assertEqual(md["reftime"], "2013-03-18T14:30:00Z")
        self.assertEqual(md["task"], "ZLR-BB")
        self.assertEqual(md["quantity"], "DBZH")
        self.assertEqual(md["area"],
                         "GRIB(latfirst=42314117, latlast=46912151, lonfirst=8273203, lonlast=14987079, type=0)")

    def test_image_ppi(self):
        md = self.read("inbound/odimh5/IMAGE_PPI_v20.h5", 49113)
        self.assertEqual(md["origin"], "ODIMH5(16144, IY46, itspc)")
        self.assertEqual(md["product"], "ODIMH5(IMAGE, PPI)")
        self.assertEqual(md["level"], "ODIMH5(0.5, 0.5)")
        self.assertEqual(md["reftime"], "2013-03-18T14:30:00Z")
        self.assertEqual(md["task"], "ZLR-BB")
        self.assertEqual(md["quantity"], "DBZH")
        self.assertEqual(md["area"],
                         "GRIB(latfirst=42314117, latlast=46912151, lonfirst=8273203, lonlast=14987079, type=0)")

    def test_image_rr(self):
        md = self.read("inbound/odimh5/IMAGE_RR_v20.h5", 49049)
        self.assertEqual(md["origin"], "ODIMH5(16144, IY46, itspc)")
        self.assertEqual(md["product"], "ODIMH5(IMAGE, RR)")
        self.assertEqual(md["timerange"], "Timedef(0s, 1, 3600s)")
        self.assertEqual(md["reftime"], "2013-03-18T14:30:00Z")
        self.assertEqual(md["task"], "ZLR-BB")
        self.assertEqual(md["quantity"], "ACRR")
        self.assertEqual(md["area"],
                         "GRIB(latfirst=42314117, latlast=46912151, lonfirst=8273203, lonlast=14987079, type=0)")
        self.assertNotIn("level", md)

    def test_image_vil(self):
        md = self.read("inbound/odimh5/IMAGE_VIL_v20.h5", 49097)
        self.assertEqual(md["origin"], "ODIMH5(16144, IY46, itspc)")
        self.assertEqual(md["product"], "ODIMH5(IMAGE, VIL)")
        self.assertEqual(md["level"], "GRIB1(106, 010, 000)")
        self.assertEqual(md["reftime"], "2013-03-18T14:30:00Z")
        self.assertEqual(md["task"], "ZLR-BB")
        self.assertEqual(md["quantity"], "VIL")
        self.assertEqual(md["area"],
                         "GRIB(latfirst=42314117, latlast=46912151, lonfirst=8273203, lonlast=14987079, type=0)")

    def test_image_zlr_bb(self):
        md = self.read("inbound/odimh5/IMAGE_ZLR-BB_v20.h5", 62161)
        self.assertEqual(md["origin"], "ODIMH5(16144, IY46, itspc)")
        self.assertEqual(md["product"], "ODIMH5(IMAGE, NEW:LBM_ARPA)")
        self.assertEqual(md["reftime"], "2013-03-18T10:00:00Z")
        self.assertEqual(md["task"], "ZLR-BB")
        self.assertEqual(md["quantity"], "DBZH")
        self.assertEqual(md["area"],
                         "GRIB(latfirst=42314117, latlast=46912151, lonfirst=8273203, lonlast=14987079, type=0)")
        self.assertNotIn("level", md)

    def test_xsec(self):
        md = self.read("inbound/odimh5/XSEC_v21.h5", 19717)
        self.assertEqual(md["origin"], "ODIMH5(16144, IY46, itspc)")
        self.assertEqual(md["product"], "ODIMH5(XSEC, XSEC)")
        self.assertEqual(md["reftime"], "2013-11-04T14:10:00Z")
        self.assertEqual(md["task"], "XZS")
        self.assertEqual(md["quantity"], "DBZH")
        self.assertEqual(md["area"],
                         "GRIB(latfirst=44320636, latlast=44821945, lonfirst=11122189, lonlast=12546566, type=0)")
        self.assertNotIn("level", md)

    def test_empty(self):
        """
        Check that the scanner silently discard an empty file
        """
        with arki.dataset.Session() as session:
            with session.dataset_reader(cfg={
                        "format": "odimh5",
                        "name": "empty.h5",
                        "path": "inbound/odimh5/empty.h5",
                        "type": "file",
                    }) as ds:
                mds = ds.query_data()
                self.assertEqual(len(mds), 0)
