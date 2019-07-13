import arkimet as arki
import unittest
import shutil
import os
from contextlib import contextmanager
from arkimet.cmdline.query import Query
from arkimet.test import CatchOutput


@contextmanager
def dataset(srcfile):
    try:
        shutil.rmtree("testds")
    except FileNotFoundError:
        pass
    os.mkdir("testds")

    cfg = arki.cfg.Section({
        "format": "grib",
        "name": "testds",
        "path": "testds",
        "type": "iseg",
        "step": "daily",
    })

    with open("testds/config", "wt") as fd:
        cfg.write(fd)

    src_cfg = arki.dataset.read_config(srcfile)
    source = arki.dataset.Reader(src_cfg)
    dest = arki.dataset.Writer(cfg)

    def do_import(md):
        dest.acquire(md)

    source.query_data(on_metadata=do_import)
    dest.flush()

    yield cfg

    shutil.rmtree("testds")


class TestArkiQuery(unittest.TestCase):
    def runcmd(self, *args):
        # arkiquery = arki.ArkiQuery()
        # return arkiquery.run(args=("arki-query",) + args)
        try:
            return Query.main(args)
        except SystemExit as e:
            return e.args[0]

    def test_postproc(self):
        with dataset("inbound/test.grib1"):
            out = CatchOutput()
            with out.redirect():
                res = self.runcmd("--postproc=checkfiles", "", "testds", "--postproc-data=/dev/null")
            self.assertEqual(out.stderr, b"")
            self.assertEqual(out.stdout, b"/dev/null\n")
            self.assertIsNone(res)

    def test_query_metadata(self):
        out = CatchOutput()
        with out.redirect():
            res = self.runcmd("--data", "", "inbound/test.arkimet")
        self.assertEqual(out.stderr, b"")
        self.assertEqual(out.stdout[:4], b"GRIB")
        self.assertIsNone(res)

    def test_query_merged(self):
        with dataset("inbound/fixture.grib1"):
            out = CatchOutput()
            with out.redirect():
                res = self.runcmd("--merged", "--data", "", "testds")
            self.assertEqual(out.stderr, b"")
            self.assertEqual(out.stdout[:4], b"GRIB")
            self.assertIsNone(res)

    def test_query_qmacro(self):
        with dataset("inbound/fixture.grib1"):
            out = CatchOutput()
            with out.redirect():
                res = self.runcmd("--qmacro=noop", "--data", "testds", "testds")
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
        self.assertEqual(res, 2)

        with out.redirect():
            res = self.runcmd("--stdin=grib", "--config=/dev/null", "")
        self.assertIn(b"--stdin cannot be used together with --config", out.stderr)
        self.assertEqual(out.stdout, b"")
        self.assertEqual(res, 2)
