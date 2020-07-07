# python 3.7+ from __future__ import annotations
from typing import Union
import unittest
import io
import os
import tempfile
import json
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
            self.assertCountEqual(s.to_python()["items"], s1.to_python()["items"])

    def test_write_yaml(self):
        self.maxDiff = None
        s = self.read("inbound/test.grib1")
        with io.BytesIO() as out:
            s.write(out, format="yaml")
            self.assertEqual(out.getvalue()[:11], b"SummaryItem")
            self.assertNotIn(b'# sfc Surface (of the Earth, which includes sea surface)', out.getvalue())

            # Read from bytes()
            s1 = arki.Summary.read_yaml(out.getvalue())
            self.assertCountEqual(s.to_python()["items"], s1.to_python()["items"])

            # Read from str()
            s1 = arki.Summary.read_yaml(out.getvalue().decode())
            self.assertCountEqual(s.to_python()["items"], s1.to_python()["items"])

            # Read from binary abstract FD
            out.seek(0)
            s1 = arki.Summary.read_yaml(out)
            self.assertCountEqual(s.to_python()["items"], s1.to_python()["items"])

            # Read from string abstract FD
            with io.StringIO(out.getvalue().decode()) as infd:
                s1 = arki.Summary.read_yaml(infd)
                self.assertCountEqual(s.to_python()["items"], s1.to_python()["items"])

        with io.BytesIO() as out:
            s.write(out, format="yaml", annotate=True)
            self.assertEqual(out.getvalue()[:11], b"SummaryItem")
            self.assertIn(b'# sfc Surface (of the Earth, which includes sea surface)', out.getvalue())

            out.seek(0)
            s1 = arki.Summary.read_yaml(out)
            self.assertCountEqual(s.to_python()["items"], s1.to_python()["items"])

    def test_write_json(self):
        s = self.read("inbound/test.grib1")
        with io.BytesIO() as out:
            s.write(out, format="json")
            self.assertEqual(out.getvalue()[:21], b'{"items":[{"origin":{')
            self.assertNotIn(b'"desc":"', out.getvalue())

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

    def test_write_short_yaml(self):
        s = self.read("inbound/test.grib1")
        with io.BytesIO() as out:
            s.write_short(out, format="yaml")
            self.assertEqual(out.getvalue()[:11], b"SummaryStat")
            self.assertNotIn(b'# sfc Surface (of the Earth, which includes sea surface)', out.getvalue())

        with io.BytesIO() as out:
            s.write_short(out, format="yaml", annotate=True)
            self.assertEqual(out.getvalue()[:11], b"SummaryStat")
            self.assertIn(b'# sfc Surface (of the Earth, which includes sea surface)', out.getvalue())

    def test_write_short_json(self):
        s = self.read("inbound/test.grib1")
        with io.BytesIO() as out:
            s.write_short(out, format="json")
            self.assertEqual(out.getvalue()[:26], b'{"items":{"summarystats":{')
            self.assertNotIn(b'"desc":"', out.getvalue())

        with io.BytesIO() as out:
            s.write_short(out, format="json", annotate=True)
            self.assertEqual(out.getvalue()[:26], b'{"items":{"summarystats":{')
            self.assertIn(b'"desc":"', out.getvalue())

    def test_issue230(self):
        self.maxDiff = None
        # Read the binary summary
        with open("inbound/issue230.summary", "rb") as fd:
            summary = arki.Summary.read_binary(fd)

        # Serialize to JSON to check that what we read matches the sample
        with tempfile.NamedTemporaryFile("w+b") as out:
            summary.write(out, format="json")
            out.seek(0)
            parsed = json.load(out)

        self.assertCountEqual(parsed["items"], [
            {'origin': {'s': 'GRIB1', 'ce': 80, 'sc': 255, 'pr': 22},
             'product': {'s': 'GRIB1', 'or': 80, 'ta': 2, 'pr': 1},
             'level': {'s': 'GRIB1', 'lt': 1},
             'timerange': {'s': 'GRIB1', 'ty': 0, 'un': 1, 'p1': 1, 'p2': 0},
             'area': {'s': 'GRIB', 'va': {
                 'Ni': 1083, 'Nj': 559, 'latfirst': -13050000, 'latlast': 12060000, 'latp': -47000000,
                 'lonfirst': -25290000, 'lonlast': 23400000, 'lonp': 10000000, 'rot': 0, 'type': 10}},
             'proddef': {'s': 'GRIB', 'va': {'tod': 1}},
             'run': {'s': 'MINUTE', 'va': 0},
             'summarystats': {'b': [2019, 6, 21, 0, 0, 0], 'e': [2019, 6, 21, 0, 0, 0], 'c': 1, 's': 1210914}},
            {'origin': {'s': 'GRIB1', 'ce': 80, 'sc': 255, 'pr': 22},
             'product': {'s': 'GRIB1', 'or': 80, 'ta': 2, 'pr': 1},
             'level': {'s': 'GRIB1', 'lt': 1},
             'timerange': {'s': 'GRIB1', 'ty': 0, 'un': 1, 'p1': 0, 'p2': 0},
             'area': {'s': 'GRIB', 'va': {
                 'Ni': 1083, 'Nj': 559, 'latfirst': -13050000, 'latlast': 12060000, 'latp': -47000000,
                 'lonfirst': -25290000, 'lonlast': 23400000, 'lonp': 10000000, 'rot': 0, 'type': 10}},
             'proddef': {'s': 'GRIB', 'va': {'tod': 1}},
             'run': {'s': 'MINUTE', 'va': 0},
             'summarystats': {'b': [2019, 6, 24, 0, 0, 0], 'e': [2019, 6, 24, 0, 0, 0], 'c': 1, 's': 1210914}},
        ])

        p = summary.to_python()
        self.assertIn("items", p)

        with arki.dataset.Session() as session:
            cfg = arki.dataset.read_config("/home/enrico/lavori/arpa/arkimet/t/arkimet/lm5")
            with session.dataset_reader(cfg=cfg) as reader:
                summary = reader.query_summary('level:GRIB1,1;product:GRIB1,80,2,1')

        p = summary.to_python()
        self.assertIn("items", p)
        self.assertEqual(len(p['items']), 0)

        ds = arki.dataset.Reader({
            "format": "arkimet",
            "name": "issue230",
            "path": "inbound/issue230.arkimet",
            "type": "file",
        })
        summary = ds.query_summary()
        self.assertIn("items", summary.to_python())
