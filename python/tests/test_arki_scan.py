import arkimet as arki
import unittest
import shutil
import os
from contextlib import contextmanager
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
        arkiscan = arki.ArkiScan()
        res = arkiscan.run(args=("arki-scan",) + args)
        if res == 0:
            return None
        return res
        # try:
        #     return Query.main(args)
        # except SystemExit as e:
        #     return e.args[0]

#     def test_stdin1(self):
#         with dataset("inbound/test.grib1"):
#             out = CatchOutput()
#             with out.redirect():
#                 res = self.runcmd("--postproc=checkfiles", "", "testds", "--postproc-data=/dev/null")
#             self.assertEqual(out.stderr, b"")
#             self.assertEqual(out.stdout, b"/dev/null\n")
#             self.assertIsNone(res)

    def read(self, fname):
        with open(fname, "rb") as fd:
            return fd.read()

    def test_stdin1(self):
        out = CatchOutput()
        with out.redirect(input=self.read("inbound/fixture.grib1")):
            res = self.runcmd("--yaml", "--stdin=grib")
        self.assertEqual(out.stderr, b"")
        self.assertRegex(out.stdout, b"\nOrigin:")
        self.assertIsNone(res)

    def test_stdin2(self):
        out = CatchOutput()
        with out.redirect():
            res = self.runcmd("--yaml", "--stdin=grib", "inbound/fixture.grib1")
        self.assertRegex(out.stderr, b"^you cannot specify input files or datasets when using --stdin")
        self.assertEqual(out.stdout, b"")
        self.assertEqual(res, 1)

    def test_stdin3(self):
        out = CatchOutput()
        with out.redirect():
            res = self.runcmd("--files=inbound/fixture.grib1", "--stdin=grib")
        self.assertRegex(out.stderr, b"^--stdin cannot be used together with --files")
        self.assertEqual(out.stdout, b"")
        self.assertEqual(res, 1)

    def test_stdin4(self):
        out = CatchOutput()
        with out.redirect():
            res = self.runcmd("--dispatch=/dev/null", "--stdin=grib")
        self.assertRegex(out.stderr, b"^--stdin cannot be used together with --dispatch")
        self.assertEqual(out.stdout, b"")
        self.assertEqual(res, 1)

    def test_scan_grib(self):
        out = CatchOutput()
        with out.redirect():
            res = self.runcmd("--yaml", "inbound/fixture.grib1")
        self.assertEqual(out.stderr, b"")
        self.assertRegex(out.stdout, b"\nOrigin:")
        self.assertIsNone(res)

    def test_scan_bufr(self):
        out = CatchOutput()
        with out.redirect():
            res = self.runcmd("--yaml", "inbound/fixture.bufr")
        self.assertEqual(out.stderr, b"")
        self.assertRegex(out.stdout, b"\nOrigin:")
        self.assertIsNone(res)

    def test_scan_bufr_multiple(self):
        out = CatchOutput()
        with out.redirect():
            res = self.runcmd("--yaml", "inbound/fixture.bufr", "inbound/ship.bufr")
        self.assertEqual(out.stderr, b"")
        self.assertRegex(out.stdout, b"\nOrigin:")
        self.assertIsNone(res)

    def test_scan_metadata(self):
        out = CatchOutput()
        with out.redirect():
            res = self.runcmd("--yaml", "inbound/test.arkimet")
        self.assertEqual(out.stderr, b"")
        self.assertRegex(out.stdout, b"\nOrigin:")
        self.assertIsNone(res)

        shutil.copyfile("inbound/test.arkimet", "test.foo")
        out = CatchOutput()
        with out.redirect():
            res = self.runcmd("--yaml", "metadata:test.foo")
        self.assertEqual(out.stderr, b"")
        self.assertRegex(out.stdout, b"\nOrigin:")
        self.assertIsNone(res)

    def test_scan_dash(self):
        out = CatchOutput()
        with out.redirect():
            res = self.runcmd("--yaml", "-")
        self.assertEqual(out.stderr, b"file - does not exist\n")
        self.assertEqual(out.stdout, b"")
        self.assertEqual(res, 1)

        out = CatchOutput()
        with out.redirect():
            res = self.runcmd("--json", "bufr:-")
        self.assertEqual(out.stderr, b"file - does not exist\n")
        self.assertEqual(out.stdout, b"")
        self.assertEqual(res, 1)

        out = CatchOutput()
        with out.redirect():
            res = self.runcmd("--dispatch=/dev/null", "-")
        self.assertEqual(out.stderr, b"file - does not exist\n")
        self.assertEqual(out.stdout, b"")
        self.assertEqual(res, 1)
