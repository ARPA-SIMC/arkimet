import arkimet as arki
import unittest
import shutil
import os
import posix
from contextlib import contextmanager
from arkimet.cmdline.scan import Scan
from arkimet.test import CmdlineTestMixin, skip_unless_vm2


class Env(arki.test.Env):
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        os.mkdir("testenv/error")
        os.mkdir("testenv/copyok")
        os.mkdir("testenv/copyko")

    def build_config(self):
        config = super().build_config()

        error_cfg = arki.cfg.Section(
            {
                "name": "error",
                "path": os.path.abspath("testenv/error"),
                "type": "error",
                "step": "daily",
            }
        )
        config["error"] = error_cfg

        with open("testenv/testds/error", "wt") as fd:
            error_cfg.write(fd)

        return config


def parse_metadata(buf):
    mds = []

    def on_metadata(md):
        mds.append(md)

    arki.Metadata.read_bundle(buf, dest=on_metadata)

    return mds


class TestArkiScan(CmdlineTestMixin, unittest.TestCase):
    command = Scan

    @contextmanager
    def datasets(self, **kw):
        kw.setdefault("format", "grib")
        kw.setdefault("filter", "origin:GRIB1")
        with Env(**kw) as env:
            yield env

    def read(self, fname):
        with open(fname, "rb") as fd:
            return fd.read()

    def test_stdin1(self):
        with open("inbound/fixture.grib1") as stdin:
            out = self.call_output_success("--yaml", "--stdin=grib", binary=True, input=stdin)
        self.assertRegex(out, b"\nOrigin:")

    def test_stdin2(self):
        out, err, res = self.call_output("--yaml", "--stdin=grib", "inbound/fixture.grib1")
        self.assertRegex(err, "you cannot specify input files or datasets when using --stdin")
        self.assertEqual(out, "")
        self.assertEqual(res, posix.EX_USAGE)

    def test_stdin3(self):
        out, err, res = self.call_output("--files=inbound/fixture.grib1", "--stdin=grib")
        self.assertRegex(err, "you cannot specify input files or datasets when using --stdin")
        self.assertEqual(out, "")
        self.assertEqual(res, posix.EX_USAGE)

    def test_stdin4(self):
        out, err, res = self.call_output("--dispatch=/dev/null", "--stdin=grib")
        self.assertRegex(err, "--stdin cannot be used together with --dispatch")
        self.assertEqual(out, "")
        self.assertEqual(res, posix.EX_USAGE)

    def test_scan_grib(self):
        out = self.call_output_success("--yaml", "inbound/fixture.grib1", binary=True)
        self.assertRegex(out, b"\nOrigin:")

    def test_scan_bufr(self):
        out = self.call_output_success("--yaml", "inbound/fixture.bufr", binary=True)
        self.assertRegex(out, b"\nOrigin:")

    def test_scan_bufr_multiple(self):
        out = self.call_output_success("--yaml", "inbound/fixture.bufr", "inbound/ship.bufr", binary=True)
        self.assertRegex(out, b"\nOrigin:")

    def test_scan_metadata(self):
        out = self.call_output_success("--yaml", "inbound/test.grib1.arkimet", binary=True)
        self.assertRegex(out, b"\nOrigin:")

        shutil.copyfile("inbound/test.grib1.arkimet", "test.foo")
        out = self.call_output_success("--yaml", "metadata:test.foo", binary=True)
        self.assertRegex(out, b"\nOrigin:")

    def test_scan_dash(self):
        out, err, res = self.call_output("--yaml", "-")
        self.assertRegex(err, "use --stdin to read data from standard input")
        self.assertEqual(out, "")
        self.assertEqual(res, posix.EX_USAGE)

        with self.assertRaises(RuntimeError) as e:
            out, err, res = self.call_output("--json", "bufr:-")
        self.assertRegex(str(e.exception), 'file "-" does not exist')

        out, err, res = self.call_output("--dispatch=/dev/null", "-")
        self.assertRegex(err, "use --stdin to read data from standard input")
        self.assertEqual(out, "")
        self.assertEqual(res, posix.EX_USAGE)

    def test_dispatch_plain(self):
        with self.datasets():
            arki.counters.acquire_single_count.reset()
            arki.counters.acquire_batch_count.reset()

            out = self.call_output_success("--dispatch=testenv/config", "inbound/test.grib1", binary=True)
            mds = parse_metadata(out)
            self.assertEqual(len(mds), 3)

            self.assertEqual(mds[0].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-08.grib"))
            self.assertEqual(mds[1].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-07.grib"))
            self.assertEqual(mds[2].to_python("source")["file"], os.path.abspath("testenv/testds/2007/10-09.grib"))

            self.assertEqual(arki.counters.acquire_single_count.value, 0)
            self.assertEqual(arki.counters.acquire_batch_count.value, 1)

    def test_dispatch_flush_threshold(self):
        with self.datasets():
            arki.counters.acquire_single_count.reset()
            arki.counters.acquire_batch_count.reset()

            out = self.call_output_success(
                "--dispatch=testenv/config", "--flush-threshold=8k", "inbound/test.grib1", binary=True
            )
            mds = parse_metadata(out)
            self.assertEqual(len(mds), 3)

            self.assertEqual(mds[0].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-08.grib"))
            self.assertEqual(mds[1].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-07.grib"))
            self.assertEqual(mds[2].to_python("source")["file"], os.path.abspath("testenv/testds/2007/10-09.grib"))

            self.assertEqual(arki.counters.acquire_single_count.value, 0)
            self.assertEqual(arki.counters.acquire_batch_count.value, 2)

    def test_dispatch_copyok_copyko(self):
        with self.datasets(filter="origin:GRIB1,200 or GRIB1,80"):
            out = self.call_output_success(
                "--copyok=testenv/copyok",
                "--copyko=testenv/copyko",
                "--dispatch=testenv/config",
                "inbound/test.grib1",
                returncode=posix.EX_DATAERR,
                binary=True,
            )
            mds = parse_metadata(out)
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
        with self.datasets(filter="origin:GRIB1"):
            out = self.call_output_success(
                "--copyok=testenv/copyok",
                "--copyko=testenv/copyko",
                "--dispatch=testenv/config",
                "inbound/test.grib1",
                binary=True,
            )
            mds = parse_metadata(out)
            self.assertEqual(len(mds), 3)

            self.assertEqual(mds[0].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-08.grib"))
            self.assertEqual(mds[1].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-07.grib"))
            self.assertEqual(mds[2].to_python("source")["file"], os.path.abspath("testenv/testds/2007/10-09.grib"))

            self.assertTrue(os.path.exists("inbound/test.grib1"))
            self.assertTrue(os.path.exists("testenv/copyok/test.grib1"))
            self.assertEqual(os.path.getsize("testenv/copyok/test.grib1"), 44412)
            self.assertFalse(os.path.exists("testenv/copyko/test.grib1"))

    def test_dispatch_copyko(self):
        with self.datasets(filter="origin:GRIB2"):
            out = self.call_output_success(
                "--copyok=testenv/copyok",
                "--copyko=testenv/copyko",
                "--dispatch=testenv/config",
                "inbound/test.grib1",
                returncode=posix.EX_DATAERR,
                binary=True,
            )
            mds = parse_metadata(out)
            self.assertEqual(len(mds), 3)

            self.assertEqual(mds[0].to_python("source")["file"], os.path.abspath("testenv/error/2007/07-08.grib"))
            self.assertEqual(mds[1].to_python("source")["file"], os.path.abspath("testenv/error/2007/07-07.grib"))
            self.assertEqual(mds[2].to_python("source")["file"], os.path.abspath("testenv/error/2007/10-09.grib"))

            self.assertTrue(os.path.exists("inbound/test.grib1"))
            self.assertFalse(os.path.exists("testenv/copyok/test.grib1"))
            self.assertTrue(os.path.exists("testenv/copyko/test.grib1"))
            self.assertEqual(os.path.getsize("testenv/copyko/test.grib1"), 44412)

    def test_dispatch_moveok(self):
        with self.datasets(filter="origin:GRIB1"):
            shutil.copyfile("inbound/test.grib1", "testenv/test.grib1")
            out = self.call_output_success(
                "--moveok=testenv/copyok",
                "--moveko=testenv/copyko",
                "--dispatch=testenv/config",
                "testenv/test.grib1",
                binary=True,
            )
            mds = parse_metadata(out)
            self.assertEqual(len(mds), 3)

            self.assertEqual(mds[0].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-08.grib"))
            self.assertEqual(mds[1].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-07.grib"))
            self.assertEqual(mds[2].to_python("source")["file"], os.path.abspath("testenv/testds/2007/10-09.grib"))

            self.assertFalse(os.path.exists("testenv/test.grib1"))
            self.assertTrue(os.path.exists("testenv/copyok/test.grib1"))
            self.assertEqual(os.path.getsize("testenv/copyok/test.grib1"), 44412)
            self.assertFalse(os.path.exists("testenv/copyko/test.grib1"))

    def test_dispatch_moveko(self):
        with self.datasets(filter="origin:GRIB2"):
            shutil.copyfile("inbound/test.grib1", "testenv/test.grib1")
            out = self.call_output_success(
                "--moveok=testenv/copyok",
                "--moveko=testenv/copyko",
                "--dispatch=testenv/config",
                "testenv/test.grib1",
                binary=True,
                returncode=posix.EX_DATAERR,
            )
            mds = parse_metadata(out)
            self.assertEqual(len(mds), 3)

            self.assertEqual(mds[0].to_python("source")["file"], os.path.abspath("testenv/error/2007/07-08.grib"))
            self.assertEqual(mds[1].to_python("source")["file"], os.path.abspath("testenv/error/2007/07-07.grib"))
            self.assertEqual(mds[2].to_python("source")["file"], os.path.abspath("testenv/error/2007/10-09.grib"))

            self.assertFalse(os.path.exists("testenv/test.grib1"))
            self.assertFalse(os.path.exists("testenv/copyok/test.grib1"))
            self.assertTrue(os.path.exists("testenv/copyko/test.grib1"))
            self.assertEqual(os.path.getsize("testenv/copyko/test.grib1"), 44412)

    def test_dispatch_moveok_moveko(self):
        with self.datasets(filter="origin:GRIB1,200 or GRIB1,80"):
            shutil.copyfile("inbound/test.grib1", "testenv/test.grib1")
            out = self.call_output_success(
                "--moveok=testenv/copyok",
                "--moveko=testenv/copyko",
                "--dispatch=testenv/config",
                "testenv/test.grib1",
                binary=True,
                returncode=posix.EX_DATAERR,
            )
            mds = parse_metadata(out)
            self.assertEqual(len(mds), 3)

            self.assertEqual(mds[0].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-08.grib"))
            self.assertEqual(mds[1].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-07.grib"))
            self.assertEqual(mds[2].to_python("source")["file"], os.path.abspath("testenv/error/2007/10-09.grib"))

            self.assertFalse(os.path.exists("testenv/test.grib1"))
            self.assertFalse(os.path.exists("testenv/copyok/test.grib1"))
            self.assertTrue(os.path.exists("testenv/copyko/test.grib1"))
            self.assertEqual(os.path.getsize("testenv/copyko/test.grib1"), 44412)

    def test_dispatch_writefail(self):
        with self.datasets(filter="origin:GRIB1,200 or GRIB1,80"):
            shutil.rmtree("testenv/error")
            with open("testenv/error", "wt") as fd:
                fd.write("this is not a directory")
            shutil.copyfile("inbound/test.grib1", "testenv/test.grib1")
            with self.assertLogs():
                out = self.call_output_success(
                    "--moveok=testenv/copyok",
                    "--moveko=testenv/copyko",
                    "--dispatch=testenv/config",
                    "testenv/test.grib1",
                    binary=True,
                    returncode=posix.EX_CANTCREAT,
                )
            mds = parse_metadata(out)
            self.assertEqual(len(mds), 3)

            self.assertEqual(mds[0].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-08.grib"))
            self.assertEqual(mds[1].to_python("source")["file"], os.path.abspath("testenv/testds/2007/07-07.grib"))
            self.assertEqual(mds[2].to_python("source")["file"], os.path.abspath("testenv/test.grib1"))

            self.assertFalse(os.path.exists("testenv/test.grib1"))
            self.assertFalse(os.path.exists("testenv/copyok/test.grib1"))
            self.assertTrue(os.path.exists("testenv/copyko/test.grib1"))
            self.assertEqual(os.path.getsize("testenv/copyko/test.grib1"), 44412)

    def test_dispatch_issue68(self):
        skip_unless_vm2()

        with self.datasets(filter="area:VM2,1", format="vm2"):
            out = self.call_output_success(
                "--copyok=testenv/copyok",
                "--copyko=testenv/copyko",
                "--dispatch=testenv/config",
                "inbound/issue68.vm2",
                binary=True,
                returncode=posix.EX_DATAERR,
            )
            mds = parse_metadata(out)
            self.assertEqual(len(mds), 5)

            self.assertTrue(os.path.exists("testenv/copyok/issue68.vm2"))
            self.assertTrue(os.path.exists("testenv/copyko/issue68.vm2"))

            with open("testenv/copyok/issue68.vm2", "rt") as fd:
                self.assertEqual(
                    fd.readlines(),
                    [
                        "198710310000,1,227,1.2,,,000000000\n",
                        "19871031000030,1,228,.5,,,000000000\n",
                    ],
                )

            with open("testenv/copyko/issue68.vm2", "rt") as fd:
                self.assertEqual(
                    fd.readlines(),
                    [
                        "201101010000,12,1,800,,,000000000\n",
                        "201101010100,12,2,50,,,000000000\n",
                        "201101010300,12,2,50,,,000000000\n",
                    ],
                )

    def test_dispatch_issue154(self):
        skip_unless_vm2()

        with self.datasets(filter="area:VM2,42", format="vm2"):
            out = self.call_output_success(
                "--copyok=testenv/copyok",
                "--copyko=testenv/copyko",
                "--dispatch=testenv/config",
                "inbound/issue68.vm2",
                binary=True,
                returncode=posix.EX_DATAERR,
            )
            mds = parse_metadata(out)
            self.assertEqual(len(mds), 5)

            self.assertFalse(os.path.exists("testenv/copyok/issue68.vm2"))
            self.assertTrue(os.path.exists("testenv/copyko/issue68.vm2"))

            with open("testenv/copyko/issue68.vm2", "rt") as fd:
                self.assertEqual(
                    fd.readlines(),
                    [
                        "198710310000,1,227,1.2,,,000000000\n",
                        "19871031000030,1,228,.5,,,000000000\n",
                        "201101010000,12,1,800,,,000000000\n",
                        "201101010100,12,2,50,,,000000000\n",
                        "201101010300,12,2,50,,,000000000\n",
                    ],
                )

    def test_dispatch_issue237(self):
        # Test VM2 normalisation in dispatching
        skip_unless_vm2()
        with self.datasets(filter="area:VM2", format="vm2", smallfiles="no"):
            out = self.call_output_success(
                "--dispatch=testenv/config",
                "inbound/issue237.vm2",
                binary=True,
            )
            mds = parse_metadata(out)
            self.assertEqual(len(mds), 1)

            # After import, the size shrinks from 36 to 34
            self.assertEqual(
                mds[0].to_python("source"),
                {
                    "type": "source",
                    "style": "BLOB",
                    "basedir": "",
                    "file": os.path.abspath("testenv/testds/2020/10-31.vm2"),
                    "format": "vm2",
                    "offset": 0,
                    "size": 34,
                },
            )

            # In the dataset, the extra 00 seconds are gone
            with open("testenv/testds/2020/10-31.vm2", "rt") as fd:
                self.assertEqual(fd.readlines(), ["202010312300,12865,158,9.409990,,,\n"])

    def test_files(self):
        # Reproduce https://github.com/ARPA-SIMC/arkimet/issues/19
        with self.datasets():
            with open("testenv/import.lst", "wt") as fd:
                print("grib:inbound/test.grib1", file=fd)
            with open("testenv/config", "wt") as fd:
                print("[error]\ntype=discard\nname=error\n", file=fd)
            with self.assertLogs() as log:
                out = self.call_output_success(
                    "--dispatch=testenv/config",
                    "--dump",
                    "--status",
                    "--summary",
                    "--files=testenv/import.lst",
                    binary=True,
                    returncode=posix.EX_DATAERR,
                )
            self.assertRegex(
                log.output[0],
                r"WARNING:arkimet:inbound/test.grib1:"
                r" some problems: 0 ok, 0 duplicates, 3 in error dataset"
                r" in [0-9.]+ seconds",
            )
            self.assertEqual(log.output[1:], [])

            self.assertRegex(out, b"^SummaryItem:")
