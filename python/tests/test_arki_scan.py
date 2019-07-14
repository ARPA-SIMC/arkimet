import arkimet as arki
import unittest
import shutil
import os
from contextlib import contextmanager
from arkimet.test import CatchOutput


@contextmanager
def datasets():
    try:
        shutil.rmtree("testenv")
    except FileNotFoundError:
        pass
    os.mkdir("testenv")
    os.mkdir("testenv/testds")
    os.mkdir("testenv/error")

    ds_cfg = arki.cfg.Section({
        "format": "grib",
        "name": "testds",
        "path": os.path.abspath("testenv/testds"),
        "type": "iseg",
        "step": "daily",
        "filter": "origin:GRIB1",
    })

    error_cfg = arki.cfg.Section({
        "name": "error",
        "path": os.path.abspath("testenv/error"),
        "type": "error",
        "step": "daily",
    })

    config = arki.cfg.Sections()
    config["testds"] = ds_cfg
    config["error"] = error_cfg

    with open("testenv/config", "wt") as fd:
        config.write(fd)

    with open("testenv/testds/config", "wt") as fd:
        ds_cfg.write(fd)

    with open("testenv/testds/error", "wt") as fd:
        error_cfg.write(fd)

    yield config

    shutil.rmtree("testenv")


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

    def test_dispatch_plain(self):
        with datasets():
            arki.counters.acquire_single_count.reset()
            arki.counters.acquire_batch_count.reset()

            out = CatchOutput()
            with out.redirect():
                res = self.runcmd("--dispatch=testenv/config", "inbound/test.grib1")
            self.assertEqual(out.stderr, b"")
            self.assertIsNone(res)

            mds = []

            def on_metadata(md):
                mds.append(md)

            arki.Metadata.read(out.stdout, dest=on_metadata)

            self.assertEqual(len(mds), 3)

            self.assertEqual(mds[0].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-08.grib"))
            self.assertEqual(mds[1].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-07.grib"))
            self.assertEqual(mds[2].to_python("source")["file"], os.path.abspath("testenv/testds/2007/10-09.grib"))

            self.assertEqual(arki.counters.acquire_single_count.value, 0)
            self.assertEqual(arki.counters.acquire_batch_count.value, 1)
