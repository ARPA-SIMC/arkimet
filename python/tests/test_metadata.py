import unittest
import os
import arkimet as arki


class TestMetadata(unittest.TestCase):
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

        res = []

        def store_md(md):
            nonlocal res
            res.append(md)
        ds.query_data(on_metadata=store_md)

        return res

    def test_to_python(self):
        mds = self.read("inbound/test.grib1")
        self.assertEqual(len(mds), 3)
        data = mds[0].to_python()
        source = next(i for i in data["i"] if i["t"] == "source")
        self.assertEquals(source["ofs"], 0)
        self.assertEquals(source["file"], "inbound/test.grib1")
        data = mds[1].to_python()
        source = next(i for i in data["i"] if i["t"] == "source")
        self.assertEquals(source["ofs"], 7218)
        self.assertEquals(source["file"], "inbound/test.grib1")
