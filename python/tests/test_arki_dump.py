import unittest
import os
import arkimet as arki
from arkimet.cmdline.dump import Dump
from arkimet.test import daemon, CatchOutput


class TestArkiDump(unittest.TestCase):
    def test_query(self):
        out = CatchOutput()
        with out.redirect():
            res = Dump.main(args=["--query", "reftime:=2019-07-07"])
        self.assertEqual(out.stderr, b"")
        self.assertEqual(out.stdout, b"reftime:>=2019-07-07 00:00:00,<=2019-07-07 23:59:59\n")
        self.assertIsNone(res)

    def test_aliases_local(self):
        out = CatchOutput()
        with out.redirect():
            res = Dump.main(args=["--aliases"])
        self.assertEqual(out.stderr, b"")
        self.assertEqual(out.stdout[:7], b"[level]")
        self.assertIsNone(res)

    def test_aliases_remote(self):
        with daemon(os.path.join(os.environ["TOP_SRCDIR"], "arki/dataset/http-aliases-daemon")) as url:
            out = CatchOutput()
            with out.redirect():
                res = Dump.main(args=["--aliases", url])
            self.assertEqual(out.stderr, b"")
            self.assertEqual(out.stdout, b"[level]\ng00 = GRIB1,1 or GRIB2S,1,0,0\n")
            self.assertIsNone(res)

    def test_info(self):
        import json
        out = CatchOutput()
        with out.redirect():
            res = Dump.main(args=["--info"])
        self.assertEqual(out.stderr, b"")
        self.assertEqual(json.loads(out.stdout.decode()), arki.config())
        self.assertIsNone(res)

    def test_bbox(self):
        out = CatchOutput()
        with out.redirect():
            res = Dump.main(args=["--bbox", "inbound/test.grib1.arkimet"])
        self.assertEqual(out.stderr, b"")
        self.assertEqual(out.stdout[:7], b"POLYGON")
        self.assertEqual(res, 0)

    def test_from_yaml_data(self):
        out = CatchOutput()
        with out.redirect():
            res = Dump.main(args=["--from-yaml-data", "inbound/test.yaml"])
        self.assertEqual(out.stderr, b"")
        self.assertEqual(out.stdout[:2], b"MD")
        self.assertEqual(res, 0)

    def test_from_yaml_summary(self):
        out = CatchOutput()
        with out.redirect():
            res = Dump.main(args=["--from-yaml-summary", "inbound/test.summary.yaml"])
        self.assertEqual(out.stderr, b"")
        self.assertEqual(out.stdout[:2], b"SU")
        self.assertEqual(res, 0)

    def test_from_md(self):
        out = CatchOutput()
        with out.redirect():
            res = Dump.main(args=["inbound/test.grib1.arkimet"])
        self.assertEqual(out.stderr, b"")
        self.assertEqual(out.stdout[:8], b"Source: ")
        self.assertNotIn(b"# Geopotential", out.stdout)
        self.assertEqual(res, 0)

    def test_from_summary(self):
        out = CatchOutput()
        with out.redirect():
            res = Dump.main(args=["inbound/test.summary"])
        self.assertEqual(out.stderr, b"")
        self.assertEqual(out.stdout[:12], b"SummaryItem:")
        self.assertNotIn(b"# Geopotential", out.stdout)
        self.assertEqual(res, 0)

    def test_from_md_annotated(self):
        out = CatchOutput()
        with out.redirect():
            res = Dump.main(args=["--annotate", "inbound/test.grib1.arkimet"])
        self.assertEqual(out.stderr, b"")
        self.assertEqual(out.stdout[:8], b"Source: ")
        self.assertIn(b"# Geopotential", out.stdout)
        self.assertEqual(res, 0)

    def test_from_summary_annotated(self):
        out = CatchOutput()
        with out.redirect():
            res = Dump.main(args=["--annotate", "inbound/test.summary"])
        self.assertEqual(out.stderr, b"")
        self.assertEqual(out.stdout[:12], b"SummaryItem:")
        self.assertIn(b"# Geopotential", out.stdout)
        self.assertEqual(res, 0)

        # self.parser.add_argument("-o", "--output", metavar="file",
        #                          help="write the output to the given file instead of standard output")
