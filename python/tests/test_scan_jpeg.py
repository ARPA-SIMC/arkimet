import unittest
import os
import arkimet as arki
import datetime


class TestScanJPEG(unittest.TestCase):
    def read(self, pathname, size, format="jpeg"):
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
            "format": "jpeg",
            "basedir": os.getcwd(),
            "file": pathname,
            "offset": 0,
            "size": size,
        })

        data = md.data
        self.assertEqual(len(data), size)
        self.assertEqual(data[:2], b"\xff\xd8")
        self.assertEqual(data[-2:], b"\xff\xd9")

        notes = md.get_notes()
        self.assertEqual(len(notes), 1)
        self.assertEqual(notes[0]["type"], "note")
        self.assertEqual(notes[0]["value"], "Scanned from {}".format(os.path.basename(pathname)))
        self.assertIsInstance(notes[0]["time"], datetime.datetime)
        return md

    def test_autumn(self):
        """
        Scan a JPEG file with GPS coordinates
        """
        md = self.read("inbound/jpeg/autumn.jpg", 94701)
        self.assertNotIn("origin", md)
        self.assertNotIn("level", md)
        self.assertNotIn("timerange", md)
        self.assertNotIn("task", md)
        self.assertNotIn("quantity", md)
        self.assertNotIn("run", md)
        # TODO: scan product
        self.assertNotIn("product", md)
        self.assertEqual(md.to_python("area"), {
            'style': 'GRIB',
            'type': 'area',
            'value': {
                "lat": 459497,
                "lon": 110197,
            },
        })
        self.assertEqual(md["reftime"], "2021-10-24T11:11:39Z")

    def test_test(self):
        """
        Scan a JPEG file with GPS coordinates
        """
        md = self.read("inbound/jpeg/test.jpg", 79127)
        self.assertNotIn("origin", md)
        self.assertNotIn("level", md)
        self.assertNotIn("timerange", md)
        self.assertNotIn("task", md)
        self.assertNotIn("quantity", md)
        self.assertNotIn("run", md)
        # TODO: scan product
        self.assertNotIn("product", md)
        self.assertEqual(md.to_python("area"), {
            'style': 'GRIB',
            'type': 'area',
            'value': {
                "lat": 445008,
                "lon": 113287,
            },
        })
        self.assertEqual(md["reftime"], "2021-11-09T11:45:19Z")
