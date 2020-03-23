import unittest
import os
import posix
import json
from contextlib import contextmanager
from arkimet.cmdline.query import Query
from arkimet.test import Env, CmdlineTestMixin, daemon


class TestArkiQuery(CmdlineTestMixin, unittest.TestCase):
    command = Query

    @contextmanager
    def dataset(self, srcfile, **kw):
        kw["name"] = "testds"
        kw["path"] = os.path.abspath("testenv/testds")
        kw.setdefault("format", "grib")
        kw.setdefault("step", "daily")
        kw.setdefault("type", "iseg")

        env = Env(**kw)
        env.import_file(srcfile)
        yield env
        env.cleanup()

    def test_postproc(self):
        with self.dataset("inbound/test.grib1"):
            out = self.call_output_success("--postproc=checkfiles", "", "testenv/testds", "--postproc-data=/dev/null",
                                           binary=True)
            self.assertEqual(out, b"/dev/null\n")

    def test_query_metadata(self):
        self.maxDiff = None
        out = self.call_output_success("--data", "", "inbound/test.grib1.arkimet", binary=True)
        self.assertEqual(out[:4], b"GRIB")

    def test_query_yaml_summary(self):
        self.maxDiff = None
        out = self.call_output_success("--summary", "--yaml", "", "inbound/test.grib1.arkimet", binary=True)
        self.assertEqual(out[:12], b"SummaryItem:")

    def test_query_dump_summary_short(self):
        self.maxDiff = None
        out = self.call_output_success("--summary-short", "--dump", "", "inbound/test.grib1.arkimet", binary=True)
        self.assertEqual(out[:13], b"SummaryStats:")

    def test_query_json_summary(self):
        self.maxDiff = None
        out = self.call_output_success("--summary", "--json", "", "inbound/test.grib1.arkimet", binary=True)
        parsed = json.loads(out)
        self.assertIn("items", parsed)

    def test_query_json_summary_short(self):
        self.maxDiff = None
        out = self.call_output_success("--summary-short", "--json", "", "inbound/test.grib1.arkimet", binary=True)
        parsed = json.loads(out)
        self.assertIn("items", parsed)

    def test_query_merged(self):
        with self.dataset("inbound/fixture.grib1"):
            out = self.call_output_success("--merged", "--data", "", "testenv/testds", binary=True)
            self.assertEqual(out[:4], b"GRIB")

    def test_query_qmacro(self):
        with self.dataset("inbound/fixture.grib1"):
            out = self.call_output_success("--qmacro=noop", "--data", "testds", "testenv/testds", binary=True)
            self.assertEqual(out[:4], b"GRIB")

    def test_query_stdin(self):
        with open("inbound/fixture.grib1", "rb") as fd:
            out = self.call_output_success("--stdin=grib", "--data", "", input=fd, binary=True)
        self.assertEqual(out[:4], b"GRIB")

        out, err, res = self.call_output("--stdin=grib", "", "test.metadata")
        self.assertIn("you cannot specify input files or datasets when using --stdin", err)
        self.assertEqual(out, "")
        self.assertEqual(res, posix.EX_USAGE)

        out, err, res = self.call_output("--stdin=grib", "--config=/dev/null", "")
        self.assertIn("--stdin cannot be used together with --config", err)
        self.assertEqual(out, "")
        self.assertEqual(res, posix.EX_USAGE)

    def test_issue_221(self):
        with daemon(
                os.path.join(os.environ["TOP_SRCDIR"], "arki/dataset/http-test-daemon"), "--action=query500") as url:
            with self.assertLogs() as log:
                out, err, res = self.call_output("--data", "--qmacro='expa 2020-03-16'", "--file=/dev/null", url)
                self.assertEqual(res, 1)
                self.assertFalse(err)
            self.assertEqual(log.output, [
                "WARNING:arkimet:'expa 2020-03-16' failed: POST http://localhost:18001/query "
                'got response code 500: /query simulating server POST error\n'])
