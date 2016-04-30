import unittest
import os
import arkimet as arki

class TestSummary(unittest.TestCase):
    def read(self, pathname, format="grib"):
        """
        Read all the metadata from a file
        """
        ds = arki.DatasetReader({
            "format": format,
            "name": os.path.basename(pathname),
            "path": pathname,
            "type": "file",
        })

        return ds.query_summary()

    def test_to_python(self):
        s = self.read("inbound/test.grib1")
        data = s.to_python()
        items = data["items"]
        count = sum(i["summarystats"]["c"] for i in items)
        self.assertEquals(count, 3)
