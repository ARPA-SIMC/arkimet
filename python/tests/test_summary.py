# python 3.7+ from __future__ import annotations
from typing import Union
import unittest
import io
import os
import arkimet as arki


class TestSummary(unittest.TestCase):
    def read(self, pathname: str, format: str = "grib", matcher: Union[arki.Matcher, str] = None):
        """
        Read all the metadata from a file
        """
        ds = arki.dataset.Reader({
            "format": format,
            "name": os.path.basename(pathname),
            "path": pathname,
            "type": "file",
        })

        return ds.query_summary(matcher=matcher)

    def test_to_python(self):
        s = self.read("inbound/test.grib1")
        data = s.to_python()
        items = data["items"]
        count = sum(i["summarystats"]["c"] for i in items)
        self.assertEquals(count, 3)

    def test_write_binary(self):
        self.maxDiff = None
        s = self.read("inbound/test.grib1")
        with io.BytesIO() as out:
            s.write(out, format="binary")
            self.assertEqual(out.getvalue()[:2], b"SU")

            out.seek(0)
            s1 = arki.Summary.read_binary(out)
            self.assertEqual(s.to_python(), s1.to_python())

    def test_write_yaml(self):
        self.maxDiff = None
        s = self.read("inbound/test.grib1")
        with io.BytesIO() as out:
            s.write(out, format="yaml")
            self.assertEqual(out.getvalue()[:11], b"SummaryItem")

            # Read from bytes()
            s1 = arki.Summary.read_yaml(out.getvalue())
            # self.assertCountEqual(s.to_python()["items"], s1.to_python()["items"])

            # Read from str()
            s1 = arki.Summary.read_yaml(out.getvalue().decode())
            # self.assertCountEqual(s.to_python()["items"], s1.to_python()["items"])

            # Read from binary abstract FD
            out.seek(0)
            s1 = arki.Summary.read_yaml(out)
            # self.assertCountEqual(s.to_python()["items"], s1.to_python()["items"])

            # Read from string abstract FD
            with io.StringIO(out.getvalue().decode()) as infd:
                s1 = arki.Summary.read_yaml(infd)
                # self.assertCountEqual(s.to_python()["items"], s1.to_python()["items"])

        with io.BytesIO() as out:
            s.write(out, format="yaml", annotate=True)
            self.assertEqual(out.getvalue()[:11], b"SummaryItem")
            self.assertIn(b'# sfc Surface (of the Earth, which includes sea surface)', out.getvalue())

            out.seek(0)
            s1 = arki.Summary.read_yaml(out)
            # self.assertCountEqual(s.to_python()["items"], s1.to_python()["items"])

    def test_write_json(self):
        s = self.read("inbound/test.grib1")
        with io.BytesIO() as out:
            s.write(out, format="json")
            self.assertEqual(out.getvalue()[:21], b'{"items":[{"origin":{')

            # Read from bytes()
            s1 = arki.Summary.read_json(out.getvalue())
            self.assertCountEqual(s.to_python()["items"], s1.to_python()["items"])

            # Read from str()
            s1 = arki.Summary.read_json(out.getvalue().decode())
            self.assertCountEqual(s.to_python()["items"], s1.to_python()["items"])

            # Read from binary abstract FD
            out.seek(0)
            s1 = arki.Summary.read_json(out)
            self.assertCountEqual(s.to_python()["items"], s1.to_python()["items"])

            # Read from string abstract FD
            with io.StringIO(out.getvalue().decode()) as infd:
                s1 = arki.Summary.read_json(infd)
                self.assertCountEqual(s.to_python()["items"], s1.to_python()["items"])

        with io.BytesIO() as out:
            s.write(out, format="json", annotate=True)
            self.assertEqual(out.getvalue()[:21], b'{"items":[{"origin":{')
            self.assertIn(b'"desc":"', out.getvalue())

            out.seek(0)
            s1 = arki.Summary.read_json(out)
            self.assertCountEqual(s.to_python()["items"], s1.to_python()["items"])

    def test_write_short(self):
        s = self.read("inbound/test.grib1")
        with io.BytesIO() as out:
            s.write_short(out, format="yaml")
        with io.BytesIO() as out:
            s.write_short(out, format="json")
