import arkimet as arki
import unittest
import shutil
import os
import posix
from contextlib import contextmanager
from arkimet.cmdline.scan import Scan
from arkimet.test import CatchOutput, skip_unless_vm2


@contextmanager
def datasets(**kw):
    try:
        shutil.rmtree("testenv")
    except FileNotFoundError:
        pass
    os.mkdir("testenv")
    os.mkdir("testenv/testds")
    os.mkdir("testenv/error")
    os.mkdir("testenv/copyok")
    os.mkdir("testenv/copyko")

    ds_cfg = {
        "format": "grib",
        "name": "testds",
        "path": os.path.abspath("testenv/testds"),
        "type": "iseg",
        "step": "daily",
        "filter": "origin:GRIB1",
    }
    ds_cfg.update(**kw)

    ds_cfg = arki.cfg.Section(ds_cfg)

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


def parse_metadata(buf):
    mds = []

    def on_metadata(md):
        mds.append(md)

    arki.Metadata.read_bundle(buf, dest=on_metadata)

    return mds


class TestArkiScan(unittest.TestCase):
    def runcmd(self, *args):
        try:
            return Scan.main(args)
        except SystemExit as e:
            return e.args[0]

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
        self.assertRegex(out.stderr, b"you cannot specify input files or datasets when using --stdin")
        self.assertEqual(out.stdout, b"")
        self.assertEqual(res, posix.EX_USAGE)

    def test_stdin3(self):
        out = CatchOutput()
        with out.redirect():
            res = self.runcmd("--files=inbound/fixture.grib1", "--stdin=grib")
        self.assertRegex(out.stderr, b"you cannot specify input files or datasets when using --stdin")
        self.assertEqual(out.stdout, b"")
        self.assertEqual(res, posix.EX_USAGE)

    def test_stdin4(self):
        out = CatchOutput()
        with out.redirect():
            res = self.runcmd("--dispatch=/dev/null", "--stdin=grib")
        self.assertRegex(out.stderr, b"--stdin cannot be used together with --dispatch")
        self.assertEqual(out.stdout, b"")
        self.assertEqual(res, posix.EX_USAGE)

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
            res = self.runcmd("--yaml", "inbound/test.grib1.arkimet")
        self.assertEqual(out.stderr, b"")
        self.assertRegex(out.stdout, b"\nOrigin:")
        self.assertIsNone(res)

        shutil.copyfile("inbound/test.grib1.arkimet", "test.foo")
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
        self.assertRegex(out.stderr, b"use --stdin to read data from standard input")
        self.assertEqual(out.stdout, b"")
        self.assertEqual(res, posix.EX_USAGE)

        out = CatchOutput()
        with self.assertRaises(RuntimeError) as e:
            with out.redirect():
                res = self.runcmd("--json", "bufr:-")
        self.assertRegex(str(e.exception), "file - does not exist")
        self.assertEqual(out.stderr, b"")
        self.assertEqual(out.stdout, b"")
        self.assertEqual(res, posix.EX_USAGE)

        out = CatchOutput()
        with out.redirect():
            res = self.runcmd("--dispatch=/dev/null", "-")
        self.assertRegex(out.stderr, b"use --stdin to read data from standard input")
        self.assertEqual(out.stdout, b"")
        self.assertEqual(res, posix.EX_USAGE)

    def test_dispatch_plain(self):
        with datasets():
            arki.counters.acquire_single_count.reset()
            arki.counters.acquire_batch_count.reset()

            out = CatchOutput()
            with out.redirect():
                res = self.runcmd("--dispatch=testenv/config", "inbound/test.grib1")
            self.assertEqual(out.stderr, b"")
            self.assertIsNone(res)
            mds = parse_metadata(out.stdout)
            self.assertEqual(len(mds), 3)

            self.assertEqual(mds[0].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-08.grib"))
            self.assertEqual(mds[1].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-07.grib"))
            self.assertEqual(mds[2].to_python("source")["file"], os.path.abspath("testenv/testds/2007/10-09.grib"))

            self.assertEqual(arki.counters.acquire_single_count.value, 0)
            self.assertEqual(arki.counters.acquire_batch_count.value, 1)

    def test_dispatch_flush_threshold(self):
        with datasets():
            arki.counters.acquire_single_count.reset()
            arki.counters.acquire_batch_count.reset()

            out = CatchOutput()
            with out.redirect():
                res = self.runcmd("--dispatch=testenv/config", "--flush-threshold=8k", "inbound/test.grib1")
            self.assertEqual(out.stderr, b"")
            self.assertIsNone(res)
            mds = parse_metadata(out.stdout)
            self.assertEqual(len(mds), 3)

            self.assertEqual(mds[0].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-08.grib"))
            self.assertEqual(mds[1].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-07.grib"))
            self.assertEqual(mds[2].to_python("source")["file"], os.path.abspath("testenv/testds/2007/10-09.grib"))

            self.assertEqual(arki.counters.acquire_single_count.value, 0)
            self.assertEqual(arki.counters.acquire_batch_count.value, 2)

    def test_dispatch_copyok_copyko(self):
        with datasets(filter="origin:GRIB1,200 or GRIB1,80"):
            out = CatchOutput()
            with out.redirect():
                res = self.runcmd(
                    "--copyok=testenv/copyok",
                    "--copyko=testenv/copyko",
                    "--dispatch=testenv/config",
                    "inbound/test.grib1"
                )
            self.assertEqual(out.stderr, b"")
            self.assertEqual(res, posix.EX_DATAERR)
            mds = parse_metadata(out.stdout)
            self.assertEqual(len(mds), 3)

            self.assertEqual(mds[0].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-08.grib"))
            self.assertEqual(mds[1].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-07.grib"))
            self.assertEqual(mds[2].to_python("source")["file"], os.path.abspath("testenv/error/2007/10-09.grib"))

            self.assertTrue(os.path.exists("inbound/test.grib1"))
            self.assertTrue(os.path.exists("testenv/copyok/test.grib1"))
            self.assertEqual(os.path.getsize("testenv/copyok/test.grib1"), 42178)
            self.assertTrue(os.path.exists("testenv/copyko/test.grib1"))
            self.assertEqual(os.path.getsize("testenv/copyko/test.grib1"), 2234)

    def test_dispatch_copyok(self):
        with datasets(filter="origin:GRIB1"):
            out = CatchOutput()
            with out.redirect():
                res = self.runcmd(
                    "--copyok=testenv/copyok",
                    "--copyko=testenv/copyko",
                    "--dispatch=testenv/config",
                    "inbound/test.grib1"
                )
            self.assertEqual(out.stderr, b"")
            self.assertIsNone(res)
            mds = parse_metadata(out.stdout)
            self.assertEqual(len(mds), 3)

            self.assertEqual(mds[0].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-08.grib"))
            self.assertEqual(mds[1].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-07.grib"))
            self.assertEqual(mds[2].to_python("source")["file"], os.path.abspath("testenv/testds/2007/10-09.grib"))

            self.assertTrue(os.path.exists("inbound/test.grib1"))
            self.assertTrue(os.path.exists("testenv/copyok/test.grib1"))
            self.assertEqual(os.path.getsize("testenv/copyok/test.grib1"), 44412)
            self.assertFalse(os.path.exists("testenv/copyko/test.grib1"))

    def test_dispatch_copyko(self):
        with datasets(filter="origin:GRIB2"):
            out = CatchOutput()
            with out.redirect():
                res = self.runcmd(
                    "--copyok=testenv/copyok",
                    "--copyko=testenv/copyko",
                    "--dispatch=testenv/config",
                    "inbound/test.grib1"
                )
            self.assertEqual(out.stderr, b"")
            self.assertEqual(res, posix.EX_DATAERR)
            mds = parse_metadata(out.stdout)
            self.assertEqual(len(mds), 3)

            self.assertEqual(mds[0].to_python("source")["file"], os.path.abspath("testenv/error/2007/07-08.grib"))
            self.assertEqual(mds[1].to_python("source")["file"], os.path.abspath("testenv/error/2007/07-07.grib"))
            self.assertEqual(mds[2].to_python("source")["file"], os.path.abspath("testenv/error/2007/10-09.grib"))

            self.assertTrue(os.path.exists("inbound/test.grib1"))
            self.assertFalse(os.path.exists("testenv/copyok/test.grib1"))
            self.assertTrue(os.path.exists("testenv/copyko/test.grib1"))
            self.assertEqual(os.path.getsize("testenv/copyko/test.grib1"), 44412)

    def test_dispatch_moveok(self):
        with datasets(filter="origin:GRIB1"):
            shutil.copyfile("inbound/test.grib1", "testenv/test.grib1")
            out = CatchOutput()
            with out.redirect():
                res = self.runcmd(
                    "--moveok=testenv/copyok",
                    "--moveko=testenv/copyko",
                    "--dispatch=testenv/config",
                    "testenv/test.grib1"
                )
            self.assertEqual(out.stderr, b"")
            self.assertIsNone(res)
            mds = parse_metadata(out.stdout)
            self.assertEqual(len(mds), 3)

            self.assertEqual(mds[0].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-08.grib"))
            self.assertEqual(mds[1].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-07.grib"))
            self.assertEqual(mds[2].to_python("source")["file"], os.path.abspath("testenv/testds/2007/10-09.grib"))

            self.assertFalse(os.path.exists("testenv/test.grib1"))
            self.assertTrue(os.path.exists("testenv/copyok/test.grib1"))
            self.assertEqual(os.path.getsize("testenv/copyok/test.grib1"), 44412)
            self.assertFalse(os.path.exists("testenv/copyko/test.grib1"))

    def test_dispatch_moveko(self):
        with datasets(filter="origin:GRIB2"):
            shutil.copyfile("inbound/test.grib1", "testenv/test.grib1")
            out = CatchOutput()
            with out.redirect():
                res = self.runcmd(
                    "--moveok=testenv/copyok",
                    "--moveko=testenv/copyko",
                    "--dispatch=testenv/config",
                    "testenv/test.grib1"
                )
            self.assertEqual(out.stderr, b"")
            self.assertEqual(res, posix.EX_DATAERR)
            mds = parse_metadata(out.stdout)
            self.assertEqual(len(mds), 3)

            self.assertEqual(mds[0].to_python("source")["file"], os.path.abspath("testenv/error/2007/07-08.grib"))
            self.assertEqual(mds[1].to_python("source")["file"], os.path.abspath("testenv/error/2007/07-07.grib"))
            self.assertEqual(mds[2].to_python("source")["file"], os.path.abspath("testenv/error/2007/10-09.grib"))

            self.assertFalse(os.path.exists("testenv/test.grib1"))
            self.assertFalse(os.path.exists("testenv/copyok/test.grib1"))
            self.assertTrue(os.path.exists("testenv/copyko/test.grib1"))
            self.assertEqual(os.path.getsize("testenv/copyko/test.grib1"), 44412)

    def test_dispatch_moveok_moveko(self):
        with datasets(filter="origin:GRIB1,200 or GRIB1,80"):
            shutil.copyfile("inbound/test.grib1", "testenv/test.grib1")
            out = CatchOutput()
            with out.redirect():
                res = self.runcmd(
                    "--moveok=testenv/copyok",
                    "--moveko=testenv/copyko",
                    "--dispatch=testenv/config",
                    "testenv/test.grib1"
                )
            self.assertEqual(out.stderr, b"")
            self.assertEqual(res, posix.EX_DATAERR)
            mds = parse_metadata(out.stdout)
            self.assertEqual(len(mds), 3)

            self.assertEqual(mds[0].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-08.grib"))
            self.assertEqual(mds[1].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-07.grib"))
            self.assertEqual(mds[2].to_python("source")["file"], os.path.abspath("testenv/error/2007/10-09.grib"))

            self.assertFalse(os.path.exists("testenv/test.grib1"))
            self.assertFalse(os.path.exists("testenv/copyok/test.grib1"))
            self.assertTrue(os.path.exists("testenv/copyko/test.grib1"))
            self.assertEqual(os.path.getsize("testenv/copyko/test.grib1"), 44412)

    def test_dispatch_issue68(self):
        skip_unless_vm2()

        with datasets(filter="area:VM2,1", format="vm2"):
            out = CatchOutput()
            with out.redirect():
                res = self.runcmd(
                    "--copyok=testenv/copyok",
                    "--copyko=testenv/copyko",
                    "--dispatch=testenv/config",
                    "inbound/issue68.vm2",
                )
            self.assertEqual(out.stderr, b"")
            self.assertEqual(res, posix.EX_DATAERR)
            mds = parse_metadata(out.stdout)
            self.assertEqual(len(mds), 5)

            self.assertTrue(os.path.exists("testenv/copyok/issue68.vm2"))
            self.assertTrue(os.path.exists("testenv/copyko/issue68.vm2"))

            with open("testenv/copyok/issue68.vm2", "rt") as fd:
                self.assertEqual(fd.readlines(), [
                    "198710310000,1,227,1.2,,,000000000\n",
                    "19871031000030,1,228,.5,,,000000000\n",
                ])

            with open("testenv/copyko/issue68.vm2", "rt") as fd:
                self.assertEqual(fd.readlines(), [
                    "201101010000,12,1,800,,,000000000\n",
                    "201101010100,12,2,50,,,000000000\n",
                    "201101010300,12,2,50,,,000000000\n",
                ])

    def test_dispatch_issue154(self):
        skip_unless_vm2()

        with datasets(filter="area:VM2,42", format="vm2"):
            out = CatchOutput()
            with out.redirect():
                res = self.runcmd(
                    "--copyok=testenv/copyok",
                    "--copyko=testenv/copyko",
                    "--dispatch=testenv/config",
                    "inbound/issue68.vm2",
                )
            self.assertEqual(out.stderr, b"")
            self.assertEqual(res, posix.EX_DATAERR)
            mds = parse_metadata(out.stdout)
            self.assertEqual(len(mds), 5)

            self.assertFalse(os.path.exists("testenv/copyok/issue68.vm2"))
            self.assertTrue(os.path.exists("testenv/copyko/issue68.vm2"))

            with open("testenv/copyko/issue68.vm2", "rt") as fd:
                self.assertEqual(fd.readlines(), [
                    "198710310000,1,227,1.2,,,000000000\n",
                    "19871031000030,1,228,.5,,,000000000\n",
                    "201101010000,12,1,800,,,000000000\n",
                    "201101010100,12,2,50,,,000000000\n",
                    "201101010300,12,2,50,,,000000000\n",
                ])

    def test_files(self):
        # Reproduce https://github.com/ARPA-SIMC/arkimet/issues/19
        with datasets():
            with open("testenv/import.lst", "wt") as fd:
                print("grib:inbound/test.grib1", file=fd)
            with open("testenv/config", "wt") as fd:
                print("[error]\ntype=discard", file=fd)
            out = CatchOutput()
            with out.redirect():
                res = self.runcmd(
                    "--dispatch=testenv/config",
                    "--dump", "--status", "--summary",
                    "--files=testenv/import.lst",
                )
            self.assertRegex(out.stderr, br"inbound/test.grib1:"
                                         br" serious problems: 0 ok, 0 duplicates, 0 in error dataset,"
                                         br" 3 NOT imported in [0-9.]+ seconds\n")
            self.assertRegex(out.stdout, b"^SummaryItem:")
            self.assertEqual(res, posix.EX_DATAERR)
