import unittest
import os
import arkimet as arki
from arkimet.cmdline.dump import Dump
from arkimet.test import daemon, CmdlineTestMixin


class TestArkiDump(CmdlineTestMixin, unittest.TestCase):
    command = Dump

    def test_query(self):
        out = self.call_output_success("--query", "reftime:=2019-07-07")
        self.assertEqual(out, "reftime:>=2019-07-07 00:00:00,<=2019-07-07 23:59:59\n")

    def test_aliases_local(self):
        out = self.call_output_success("--aliases")
        self.assertEqual(out[:7], "[level]")

    def test_aliases_remote(self):
        with daemon(os.path.join(os.environ["TOP_SRCDIR"], "arki/dataset/http-aliases-daemon")) as url:
            out = self.call_output_success("--aliases", url)
            self.assertEqual(out, "[level]\ng00 = GRIB1,1 or GRIB2S,1,0,0\n")

    def test_info(self):
        import json
        out = self.call_output_success("--info")
        self.assertEqual(json.loads(out), arki.config())

    def test_bbox(self):
        out = self.call_output_success("--bbox", "inbound/test.grib1.arkimet")
        self.assertEqual(out[:7], "POLYGON")

    def test_from_yaml_data(self):
        out = self.call_output_success("--from-yaml-data", "inbound/test.yaml", binary=True)
        self.assertEqual(out[:2], b"MD")

    def test_from_yaml_summary(self):
        out = self.call_output_success("--from-yaml-summary", "inbound/test.summary.yaml", binary=True)
        self.assertEqual(out[:2], b"SU")

    def test_from_md(self):
        out = self.call_output_success("inbound/test.grib1.arkimet", binary=True)
        self.assertEqual(out[:8], b"Source: ")
        self.assertNotIn(b"# Geopotential", out)

    def test_from_summary(self):
        out = self.call_output_success("inbound/test.summary", binary=True)
        self.assertEqual(out[:12], b"SummaryItem:")
        self.assertNotIn(b"# Geopotential", out)

    def test_from_md_annotated(self):
        out = self.call_output_success("--annotate", "inbound/test.grib1.arkimet", binary=True)
        self.assertEqual(out[:8], b"Source: ")
        self.assertIn(b"# Geopotential", out)

    def test_from_summary_annotated(self):
        out = self.call_output_success("--annotate", "inbound/test.summary", binary=True)
        self.assertEqual(out[:12], b"SummaryItem:")
        self.assertIn(b"# Geopotential", out)

        # self.parser.add_argument("-o", "--output", metavar="file",
        #                          help="write the output to the given file instead of standard output")
