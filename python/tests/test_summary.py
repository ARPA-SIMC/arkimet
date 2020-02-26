import unittest
import io
import os
import arkimet as arki


class TestSummary(unittest.TestCase):
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

        return ds.query_summary()

    def test_to_python(self):
        s = self.read("inbound/test.grib1")
        data = s.to_python()
        items = data["items"]
        count = sum(i["summarystats"]["c"] for i in items)
        self.assertEquals(count, 3)

    def test_write(self):
        s = self.read("inbound/test.grib1")
        with io.BytesIO() as out:
            s.write(out, format="binary")
        with io.BytesIO() as out:
            s.write(out, format="yaml")
        with io.BytesIO() as out:
            s.write(out, format="json")
