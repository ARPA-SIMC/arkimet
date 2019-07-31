import unittest
import os
import posix
from contextlib import contextmanager
from arkimet.cmdline.query import Query
from arkimet.test import CatchOutput, Env, CmdlineTestMixin


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
            out = self.call_output_success("--postproc=checkfiles", "", "testenv/testds", "--postproc-data=/dev/null")
            self.assertEqual(out, b"/dev/null\n")

    def test_query_metadata(self):
        out = CatchOutput()
        with out.redirect():
            res = self.runcmd("--data", "", "inbound/test.grib1.arkimet")
        self.assertEqual(out.stderr, b"")
        self.assertEqual(out.stdout[:4], b"GRIB")
        self.assertIsNone(res)

    def test_query_yaml_summary(self):
        out = CatchOutput()
        with out.redirect():
            res = self.runcmd("--summary", "--yaml", "", "inbound/test.grib1.arkimet")
        self.assertEqual(out.stderr, b"")
        self.assertEqual(out.stdout[:12], b"SummaryItem:")
        self.assertIsNone(res)

    def test_query_merged(self):
        with self.dataset("inbound/fixture.grib1"):
            out = CatchOutput()
            with out.redirect():
                res = self.runcmd("--merged", "--data", "", "testenv/testds")
            self.assertEqual(out.stderr, b"")
            self.assertEqual(out.stdout[:4], b"GRIB")
            self.assertIsNone(res)

    def test_query_qmacro(self):
        with self.dataset("inbound/fixture.grib1"):
            out = CatchOutput()
            with out.redirect():
                res = self.runcmd("--qmacro=noop", "--data", "testds", "testenv/testds")
            self.assertEqual(out.stderr, b"")
            self.assertEqual(out.stdout[:4], b"GRIB")
            self.assertIsNone(res)

    def test_query_qmacro_py_noop(self):
        with self.dataset("inbound/fixture.grib1"):
            out = CatchOutput()
            with out.redirect():
                res = self.runcmd("--qmacro=py_noop", "--data", "testds", "testenv/testds")
            self.assertEqual(out.stderr, b"")
            self.assertEqual(out.stdout[:4], b"GRIB")
            self.assertIsNone(res)

    def test_query_stdin(self):
        out = CatchOutput()
        with open("inbound/fixture.grib1", "rb") as fd:
            stdin = fd.read()
        with out.redirect(input=stdin):
            res = self.runcmd("--stdin=grib", "--data", "")
        self.assertEqual(out.stderr, b"")
        self.assertEqual(out.stdout[:4], b"GRIB")
        self.assertIsNone(res)

        with out.redirect():
            res = self.runcmd("--stdin=grib", "", "test.metadata")
        self.assertIn(b"you cannot specify input files or datasets when using --stdin", out.stderr)
        self.assertEqual(out.stdout, b"")
        self.assertEqual(res, posix.EX_USAGE)

        with out.redirect():
            res = self.runcmd("--stdin=grib", "--config=/dev/null", "")
        self.assertIn(b"--stdin cannot be used together with --config", out.stderr)
        self.assertEqual(out.stdout, b"")
        self.assertEqual(res, posix.EX_USAGE)
