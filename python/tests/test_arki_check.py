import arkimet as arki
import unittest
import shutil
import os
import subprocess
from contextlib import contextmanager
# from arkimet.cmdline.check import Check
from arkimet.test import CatchOutput


class StatsReporter:
    def __init__(self):
        self.op_progress = []
        self.op_manual_intervention = []
        self.op_aborted = []
        self.op_report = []
        self.seg_info = []
        self.seg_repack = []
        self.seg_archive = []
        self.seg_delete = []
        self.seg_deindex = []
        self.seg_rescan = []
        self.seg_tar = []
        self.seg_compress = []
        self.seg_issue52 = []

    def operation_progress(self, ds, operation, message):
        self.op_progress.append((ds, operation, message))

    def operation_manual_intervention(self, ds, operation, message):
        self.op_manual_intervention.append((ds, operation, message))

    def operation_aborted(self, ds, operation, message):
        self.op_aborted.append((ds, operation, message))

    def operation_report(self, ds, operation, message):
        self.op_report.append((ds, operation, message))

    def segment_info(self, ds, relpath, message):
        self.seg_info.append((ds, relpath, message))

    def segment_repack(self, ds, relpath, message):
        self.seg_repack.append((ds, relpath, message))

    def segment_archive(self, ds, relpath, message):
        self.seg_archive.append((ds, relpath, message))

    def segment_delete(self, ds, relpath, message):
        self.seg_delete.append((ds, relpath, message))

    def segment_deindex(self, ds, relpath, message):
        self.seg_deindex.append((ds, relpath, message))

    def segment_rescan(self, ds, relpath, message):
        self.seg_rescan.append((ds, relpath, message))

    def segment_tar(self, ds, relpath, message):
        self.seg_tar.append((ds, relpath, message))

    def segment_compress(self, ds, relpath, message):
        self.seg_compress.append((ds, relpath, message))

    def segment_issue51(self, ds, relpath, message):
        self.seg_issue51.append((ds, relpath, message))


class Env:
    def __init__(self, **kw):
        try:
            shutil.rmtree("testenv")
        except FileNotFoundError:
            pass
        os.mkdir("testenv")
        os.mkdir("testenv/testds")

        self.ds_cfg = arki.cfg.Section(**kw)

        self.config = arki.cfg.Sections()
        self.config["testds"] = self.ds_cfg

        with open("testenv/config", "wt") as fd:
            self.config.write(fd)

        with open("testenv/testds/config", "wt") as fd:
            self.ds_cfg.write(fd)

    def update_config(self, **kw):
        for k, v in kw.items():
            if v is None:
                del self.ds_cfg[k]
            else:
                self.ds_cfg[k] = v
        self.config["testds"] = self.ds_cfg

        with open("testenv/config", "wt") as fd:
            self.config.write(fd)

        with open("testenv/testds/config", "wt") as fd:
            self.ds_cfg.write(fd)

    def import_file(self, pathname):
        dest = arki.dataset.Writer(self.ds_cfg)

        source = arki.dataset.Reader({
            "format": self.ds_cfg["format"],
            "name": os.path.basename(pathname),
            "path": pathname,
            "type": "file",
        })

        def do_import(md):
            dest.acquire(md)

        source.query_data(on_metadata=do_import)
        dest.flush()

    def cleanup(self):
        shutil.rmtree("testenv")

    def query(self, *args, **kw):
        res = []

        def on_metadata(md):
            res.append(md)

        kw["on_metadata"] = on_metadata
        source = arki.dataset.Reader(self.ds_cfg)
        source.query_data(**kw)
        return res

    def repack(self, **kw):
        checker = arki.dataset.Checker(self.ds_cfg)
        checker.repack(**kw)

    def check(self, **kw):
        checker = arki.dataset.Checker(self.ds_cfg)
        checker.check(**kw)

    def segment_state(self, **kw):
        checker = arki.dataset.Checker(self.ds_cfg)
        return checker.segment_state(**kw)

    def inspect(self):
        subprocess.run([os.environ.get("SHELL", "bash")], cwd="testenv")


class ArkiCheckTestsBase:
    @contextmanager
    def datasets(self, **kw):
        kw["name"] = "testds"
        kw["path"] = os.path.abspath("testenv/testds")
        kw.setdefault("format", "grib")
        kw.setdefault("step", "daily")

        env = Env(**self.dataset_config(**kw))
        yield env

    def runcmd(self, *args):
        arkiscan = arki.ArkiCheck()
        res = arkiscan.run(args=("arki-check",) + args)
        if res == 0:
            return None
        return res
        # try:
        #     return Check.main(args)
        # except SystemExit as e:
        #     return e.args[0]

    def call_output(self, *args):
        out = CatchOutput()
        with out.redirect():
            res = self.runcmd(*args)
        return out, res

    def call_output_success(self, *args):
        out, res = self.call_output(*args)
        self.assertEqual(out.stderr, b"")
        self.assertIsNone(res)
        return out

    def assertCheckClean(self, env, files=None, items=None, **kw):
        reporter = StatsReporter()

        state = env.segment_state(**kw)
        if files is not None:
            self.assertEqual(len(state), files)

        not_ok = [x for x in state.items() if x[1] != "OK"]
        self.assertEqual(not_ok, [])

        env.check(reporter=reporter, **kw)
        self.assertEqual(reporter.op_manual_intervention, [])
        self.assertEqual(reporter.op_aborted, [])
        self.assertEqual(reporter.seg_repack, [])
        self.assertEqual(reporter.seg_archive, [])
        self.assertEqual(reporter.seg_delete, [])
        self.assertEqual(reporter.seg_deindex, [])
        self.assertEqual(reporter.seg_rescan, [])
        self.assertEqual(reporter.seg_tar, [])
        self.assertEqual(reporter.seg_compress, [])
        self.assertEqual(reporter.seg_issue52, [])

        if items is not None:
            mdc = env.query()
            self.assertEqual(len(mdc), items)

    def assertSegmentExists(self, pathname, extensions=None):
        if extensions is None:
            extensions = ("", ".gz", ".tar", ".zip")
            if not any(os.path.exists(pathname + ext) for ext in extensions):
                self.fail("Segment {} does not exist (also tried .gz, .tar, and .zip)".format(pathname))
        else:
            all_extensions = frozenset(("", ".gz", ".tar", ".zip", ".gz.idx", ".metadata", ".summary"))
            for ext in extensions:
                if not os.path.exists(pathname + ext):
                    self.fail("Segment {}{} does not exist but it should".format(pathname, ext))

            for ext in all_extensions - frozenset(extensions):
                if os.path.exists(pathname + ext):
                    self.fail("Segment {}{} exists but it should not".format(pathname, ext))

    def assertSegmentNotExists(self, pathname):
        all_extensions = ("", ".gz", ".tar", ".zip", ".gz.idx", ".metadata", ".summary")
        for ext in all_extensions:
            if os.path.exists(pathname + ext):
                self.fail("Segment {}{} exists but it should not".format(pathname, ext))

    def test_clean(self):
        with self.datasets() as env:
            env.import_file("inbound/fixture.grib1")

            out = self.call_output_success("testenv/testds")
            self.assertEqual(out.stdout, b"testds: check 3 files ok\n")

            out = self.call_output_success("testenv/testds", "--fix")
            self.assertEqual(out.stdout, b"testds: check 3 files ok\n")

            out = self.call_output_success("testenv/testds", "--repack")
            self.assertEqual(out.stdout, b"testds: repack 3 files ok\n")

            out = self.call_output_success("testenv/testds", "--repack", "--fix")
            self.assertRegex(
                    out.stdout,
                    rb"(testds: repack: running VACUUM ANALYZE on the dataset index(, if applicable)?\n)?"
                    rb"(testds: repack: rebuilding the summary cache\n)?"
                    rb"testds: repack 3 files ok\n"
            )

    def test_clean_filtered(self):
        with self.datasets() as env:
            env.import_file("inbound/fixture.grib1")

            out = self.call_output_success("testenv/testds", "--filter=reftime:>=2007-07-08")
            self.assertEqual(out.stdout, b"testds: check 2 files ok\n")

            out = self.call_output_success("testenv/testds", "--fix", "--filter=reftime:>=2007-07-08")
            self.assertEqual(out.stdout, b"testds: check 2 files ok\n")

            out = self.call_output_success("testenv/testds", "--repack", "--filter=reftime:>=2007-07-08")
            self.assertEqual(out.stdout, b"testds: repack 2 files ok\n")

            out = self.call_output_success("testenv/testds", "--repack", "--fix", "--filter=reftime:>=2007-07-08")
            self.assertRegex(
                    out.stdout,
                    rb"(testds: repack: running VACUUM ANALYZE on the dataset index(, if applicable)?\n)?"
                    rb"(testds: repack: rebuilding the summary cache\n)?"
                    rb"testds: repack 2 files ok\n"
            )

    def test_remove_all(self):
        with self.datasets() as env:
            env.import_file("inbound/fixture.grib1")

            self.assertTrue(os.path.exists("testenv/testds/2007/07-08.grib"))
            self.assertTrue(os.path.exists("testenv/testds/2007/07-07.grib"))
            self.assertTrue(os.path.exists("testenv/testds/2007/10-09.grib"))

            out = self.call_output_success("testenv/testds", "--remove-all")
            self.assertEqual(
                    out.stdout,
                    b"testds:2007/07-07.grib: should be deleted\n"
                    b"testds:2007/07-08.grib: should be deleted\n"
                    b"testds:2007/10-09.grib: should be deleted\n"
            )

            self.assertTrue(os.path.exists("testenv/testds/2007/07-08.grib"))
            self.assertTrue(os.path.exists("testenv/testds/2007/07-07.grib"))
            self.assertTrue(os.path.exists("testenv/testds/2007/10-09.grib"))

            out = self.call_output_success("testenv/testds", "--remove-all", "-f")
            self.assertRegex(
                    out.stdout,
                    rb"testds:2007/07-07.grib: deleted \(\d+ freed\)\n"
                    rb"testds:2007/07-08.grib: deleted \(\d+ freed\)\n"
                    rb"testds:2007/10-09.grib: deleted \(\d+ freed\)\n"
            )

            self.assertFalse(os.path.exists("testenv/testds/2007/07-08.grib"))
            self.assertFalse(os.path.exists("testenv/testds/2007/07-07.grib"))
            self.assertFalse(os.path.exists("testenv/testds/2007/10-09.grib"))

            self.assertCheckClean(env, files=0, items=0)
            self.assertEqual(env.query(), [])

    def test_remove_all_filtered(self):
        with self.datasets() as env:
            env.import_file("inbound/fixture.grib1")

            self.assertTrue(os.path.exists("testenv/testds/2007/07-08.grib"))
            self.assertTrue(os.path.exists("testenv/testds/2007/07-07.grib"))
            self.assertTrue(os.path.exists("testenv/testds/2007/10-09.grib"))

            out = self.call_output_success("testenv/testds", "--remove-all", "--filter=reftime:=2007-07-08")
            self.assertEqual(
                    out.stdout,
                    b"testds:2007/07-08.grib: should be deleted\n"
            )

            self.assertTrue(os.path.exists("testenv/testds/2007/07-08.grib"))
            self.assertTrue(os.path.exists("testenv/testds/2007/07-07.grib"))
            self.assertTrue(os.path.exists("testenv/testds/2007/10-09.grib"))

            out = self.call_output_success("testenv/testds", "--remove-all", "-f", "--filter=reftime:=2007-07-08")
            self.assertRegex(
                    out.stdout,
                    rb"testds:2007/07-08.grib: deleted \(\d+ freed\)\n"
            )

            self.assertFalse(os.path.exists("testenv/testds/2007/07-08.grib"))
            self.assertTrue(os.path.exists("testenv/testds/2007/07-07.grib"))
            self.assertTrue(os.path.exists("testenv/testds/2007/10-09.grib"))

            self.assertCheckClean(env, files=2, items=2)
            # TODO wassert(f.query_results({1, 2}));

    def test_archive(self):
        with self.datasets() as env:
            env.import_file("inbound/fixture.grib1")
            env.update_config(**{"archive age": "1"})

            out = self.call_output_success("testenv/testds")
            self.assertEqual(
                    out.stdout,
                    b"testds:2007/07-07.grib: segment old enough to be archived\n"
                    b"testds:2007/07-08.grib: segment old enough to be archived\n"
                    b"testds:2007/10-09.grib: segment old enough to be archived\n"
                    b"testds: check 0 files ok\n"
            )

            out = self.call_output_success("testenv/testds", "--fix")
            self.assertEqual(
                    out.stdout,
                    b"testds:2007/07-07.grib: segment old enough to be archived\n"
                    b"testds:2007/07-08.grib: segment old enough to be archived\n"
                    b"testds:2007/10-09.grib: segment old enough to be archived\n"
                    b"testds: check 0 files ok\n"
            )

            out = self.call_output_success("testenv/testds", "--repack")
            self.assertEqual(
                    out.stdout,
                    b"testds:2007/07-07.grib: segment old enough to be archived\n"
                    b"testds:2007/07-07.grib: should be archived\n"
                    b"testds:2007/07-08.grib: segment old enough to be archived\n"
                    b"testds:2007/07-08.grib: should be archived\n"
                    b"testds:2007/10-09.grib: segment old enough to be archived\n"
                    b"testds:2007/10-09.grib: should be archived\n"
                    b"testds: repack 0 files ok, 3 files should be archived\n"
            )

            out = self.call_output_success("testenv/testds", "--repack", "--fix", "--online", "--offline")
            self.assertRegex(
                    out.stdout,
                    rb"testds:2007/07-07.grib: segment old enough to be archived\n"
                    rb"testds:2007/07-07.grib: archived\n"
                    rb"testds:2007/07-08.grib: segment old enough to be archived\n"
                    rb"testds:2007/07-08.grib: archived\n"
                    rb"testds:2007/10-09.grib: segment old enough to be archived\n"
                    rb"testds:2007/10-09.grib: archived\n"
                    rb"(testds: repack: running VACUUM ANALYZE on the dataset index(, if applicable)?\n)?"
                    rb"(testds: repack: rebuilding the summary cache\n)?"
                    rb"testds: repack 0 files ok, 3 files archived\n"
                    rb"testds.archives.last: repack: running VACUUM ANALYZE on the dataset index, if applicable\n"
                    rb"testds.archives.last: repack 3 files ok\n"
            )

            self.assertTrue(os.path.exists("testenv/testds/.archive/last/2007/07-08.grib"))
            self.assertFalse(os.path.exists("testenv/testds/2007/07-08.grib"))

            mdc = env.query(matcher="reftime:=2007-07-08")
            self.assertEqual(len(mdc), 1)
            blob = mdc[0].to_python("source")
            self.assertEqual(blob["file"], "2007/07-08.grib")
            self.assertEqual(blob["b"], os.path.abspath("testenv/testds/.archive/last"))

            out = self.call_output_success("testenv/testds", "--unarchive=2007/07-08.grib")
            self.assertEqual(out.stdout, b"")

            self.assertFalse(os.path.exists("testenv/testds/.archive/last/2007/07-08.grib"))
            self.assertTrue(os.path.exists("testenv/testds/2007/07-08.grib"))

            mdc = env.query(matcher="reftime:=2007-07-08")
            self.assertEqual(len(mdc), 1)
            blob = mdc[0].to_python("source")
            self.assertEqual(blob["file"], "2007/07-08.grib")
            self.assertEqual(blob["b"], os.path.abspath("testenv/testds"))

    def test_tar(self):
        with self.datasets(format="odimh5") as env:
            env.import_file("inbound/fixture.odimh5/00.odimh5")
            env.import_file("inbound/fixture.odimh5/01.odimh5")
            env.import_file("inbound/fixture.odimh5/02.odimh5")

            env.update_config(**{"archive age": "1"})
            with arki.dataset.SessionTimeOverride(1184018400):  # date +%s --date="2007-07-10"
                env.repack(readonly=False)

                self.assertTrue(os.path.exists("testenv/testds/.archive/last/2007/07-07.odimh5"))
                self.assertTrue(os.path.exists("testenv/testds/2007/07-08.odimh5"))
                self.assertTrue(os.path.exists("testenv/testds/2007/10-09.odimh5"))

                out = self.call_output_success("testenv/testds", "--tar")
                self.assertEqual(out.stdout, b"testds.archives.last:2007/07-07.odimh5: should be tarred\n")

                out = self.call_output_success("testenv/testds", "--tar", "--offline")
                self.assertEqual(out.stdout, b"testds.archives.last:2007/07-07.odimh5: should be tarred\n")

                out = self.call_output_success("testenv/testds", "--tar", "--online")
                self.assertEqual(
                        out.stdout,
                        b"testds:2007/07-08.odimh5: should be tarred\n"
                        b"testds:2007/10-09.odimh5: should be tarred\n")

                out = self.call_output_success("testenv/testds", "--tar", "--fix")
                self.assertEqual(out.stdout, b"testds.archives.last:2007/07-07.odimh5: tarred\n")

                self.assertSegmentExists("testenv/testds/.archive/last/2007/07-07.odimh5",
                                         extensions=[".tar", ".metadata", ".summary"])
                self.assertSegmentExists("testenv/testds/2007/07-08.odimh5")
                self.assertSegmentExists("testenv/testds/2007/10-09.odimh5")

                self.assertCheckClean(env, files=3, items=3)
                self.assertCheckClean(env, files=3, items=3, accurate=True)
                # TODO: wassert(f.query_results({1, 0, 2}));

    def test_zip(self):
        arki.test.skip_unless_libzip()
        arki.test.skip_unless_libarchive()
        with self.datasets(format="odimh5") as env:
            env.import_file("inbound/fixture.odimh5/00.odimh5")
            env.import_file("inbound/fixture.odimh5/01.odimh5")
            env.import_file("inbound/fixture.odimh5/02.odimh5")

            env.update_config(**{"archive age": "1"})
            with arki.dataset.SessionTimeOverride(1184018400):  # date +%s --date="2007-07-10"
                env.repack(readonly=False)

                self.assertTrue(os.path.exists("testenv/testds/.archive/last/2007/07-07.odimh5"))
                self.assertTrue(os.path.exists("testenv/testds/2007/07-08.odimh5"))
                self.assertTrue(os.path.exists("testenv/testds/2007/10-09.odimh5"))

                out = self.call_output_success("testenv/testds", "--zip")
                self.assertEqual(out.stdout, b"testds.archives.last:2007/07-07.odimh5: should be zipped\n")

                out = self.call_output_success("testenv/testds", "--zip", "--offline")
                self.assertEqual(out.stdout, b"testds.archives.last:2007/07-07.odimh5: should be zipped\n")

                out = self.call_output_success("testenv/testds", "--zip", "--online")
                self.assertEqual(
                        out.stdout,
                        b"testds:2007/07-08.odimh5: should be zipped\n"
                        b"testds:2007/10-09.odimh5: should be zipped\n")

                out = self.call_output_success("testenv/testds", "--zip", "--fix")
                self.assertEqual(out.stdout, b"testds.archives.last:2007/07-07.odimh5: zipped\n")

                self.assertSegmentExists("testenv/testds/.archive/last/2007/07-07.odimh5",
                                         extensions=[".zip", ".metadata", ".summary"])
                self.assertSegmentExists("testenv/testds/2007/07-08.odimh5")
                self.assertSegmentExists("testenv/testds/2007/10-09.odimh5")

                self.assertCheckClean(env, files=3, items=3)
                self.assertCheckClean(env, files=3, items=3, accurate=True)
                # TODO: wassert(f.query_results({1, 0, 2}));

    def test_compress(self):
        with self.datasets() as env:
            env.import_file("inbound/fixture.grib1")
            env.update_config(**{"archive age": "1", "gz group size": "0"})
            with arki.dataset.SessionTimeOverride(1184018400):  # date +%s --date="2007-07-10"
                env.repack(readonly=False)

                self.assertTrue(os.path.exists("testenv/testds/.archive/last/2007/07-07.grib"))
                self.assertTrue(os.path.exists("testenv/testds/2007/07-08.grib"))
                self.assertTrue(os.path.exists("testenv/testds/2007/10-09.grib"))

                out = self.call_output_success("testenv/testds", "--compress")
                self.assertEqual(out.stdout, b"testds.archives.last:2007/07-07.grib: should be compressed\n")

                out = self.call_output_success("testenv/testds", "--compress", "--offline")
                self.assertEqual(out.stdout, b"testds.archives.last:2007/07-07.grib: should be compressed\n")

                out = self.call_output_success("testenv/testds", "--compress", "--online")
                self.assertEqual(
                        out.stdout,
                        b"testds:2007/07-08.grib: should be compressed\n"
                        b"testds:2007/10-09.grib: should be compressed\n")

                out = self.call_output_success("testenv/testds", "--compress", "--fix")
                self.assertRegex(out.stdout, rb"testds.archives.last:2007/07-07.grib: compressed \(\d+ freed\)\n")

                self.assertSegmentExists("testenv/testds/.archive/last/2007/07-07.grib",
                                         extensions=[".gz", ".metadata", ".summary"])
                self.assertSegmentExists("testenv/testds/2007/07-08.grib")
                self.assertSegmentExists("testenv/testds/2007/10-09.grib")

                self.assertCheckClean(env, files=3, items=3)
                self.assertCheckClean(env, files=3, items=3, accurate=True)
                # TODO: wassert(f.query_results({1, 0, 2}));

    def test_scan(self):
        with self.datasets(format="odimh5") as env:
            env.import_file("inbound/fixture.odimh5/00.odimh5")
            env.import_file("inbound/fixture.odimh5/01.odimh5")
            env.import_file("inbound/fixture.odimh5/02.odimh5")

            env.update_config(**{"archive age": "1"})
            with arki.dataset.SessionTimeOverride(1184018400):  # date +%s --date="2007-07-10"
                env.repack(readonly=False)

                self.assertTrue(os.path.exists("testenv/testds/.archive/last/2007/07-07.odimh5"))
                self.assertTrue(os.path.exists("testenv/testds/2007/07-08.odimh5"))
                self.assertTrue(os.path.exists("testenv/testds/2007/10-09.odimh5"))

                out = self.call_output_success("testenv/testds", "--state")
                self.assertEqual(
                        out.stdout,
                        b"testds:2007/07-08.odimh5: OK 2007-07-08 00:00:00Z to 2007-07-08 23:59:59Z\n"
                        b"testds:2007/10-09.odimh5: OK 2007-10-09 00:00:00Z to 2007-10-09 23:59:59Z\n"
                        b"testds.archives.last:2007/07-07.odimh5: OK 2007-07-01 00:00:00Z to 2007-07-31 23:59:59Z\n")

                out = self.call_output_success("testenv/testds", "--state", "--offline")
                self.assertEqual(
                        out.stdout,
                        b"testds.archives.last:2007/07-07.odimh5: OK 2007-07-01 00:00:00Z to 2007-07-31 23:59:59Z\n")

                out = self.call_output_success("testenv/testds", "--state", "--online")
                self.assertEqual(
                        out.stdout,
                        b"testds:2007/07-08.odimh5: OK 2007-07-08 00:00:00Z to 2007-07-08 23:59:59Z\n"
                        b"testds:2007/10-09.odimh5: OK 2007-10-09 00:00:00Z to 2007-10-09 23:59:59Z\n")

    def test_remove_old(self):
        with self.datasets(format="odimh5") as env:
            env.import_file("inbound/fixture.odimh5/00.odimh5")
            env.import_file("inbound/fixture.odimh5/01.odimh5")
            env.import_file("inbound/fixture.odimh5/02.odimh5")

            with arki.dataset.SessionTimeOverride(1184104800):  # date +%s --date="2007-07-11"
                env.update_config(**{"archive age": "2"})
                env.repack(readonly=False)
                env.update_config(**{"delete age": "1"})

                self.assertTrue(os.path.exists("testenv/testds/.archive/last/2007/07-07.odimh5"))
                self.assertTrue(os.path.exists("testenv/testds/2007/07-08.odimh5"))
                self.assertTrue(os.path.exists("testenv/testds/2007/10-09.odimh5"))

                out = self.call_output_success("testenv/testds", "--remove-old")
                self.assertEqual(
                        out.stdout,
                        b"testds:2007/07-08.odimh5: segment old enough to be deleted\n"
                        b"testds:2007/07-08.odimh5: should be deleted\n")

                out = self.call_output_success("testenv/testds", "--remove-old", "--offline")
                self.assertEqual(
                        out.stdout,
                        # b"testds.archives.last:2007/07-07.odimh5: segment old enough to be deleted\n"
                        b"testds.archives.last:2007/07-07.odimh5: should be deleted\n")

                out = self.call_output_success("testenv/testds", "--remove-old", "--online")
                self.assertEqual(
                        out.stdout,
                        b"testds:2007/07-08.odimh5: segment old enough to be deleted\n"
                        b"testds:2007/07-08.odimh5: should be deleted\n")

                out = self.call_output_success("testenv/testds", "--remove-old", "--online", "--offline")
                self.assertEqual(
                        out.stdout,
                        b"testds:2007/07-08.odimh5: segment old enough to be deleted\n"
                        b"testds:2007/07-08.odimh5: should be deleted\n"
                        b"testds.archives.last:2007/07-07.odimh5: should be deleted\n")

                out = self.call_output_success("testenv/testds", "--remove-old", "--fix")
                self.assertRegex(
                        out.stdout,
                        rb"^testds:2007/07-08.odimh5: segment old enough to be deleted\n"
                        rb"testds:2007/07-08.odimh5: deleted \(\d+ freed\)\n")

                self.assertSegmentExists("testenv/testds/.archive/last/2007/07-07.odimh5",
                                         extensions=["", ".metadata", ".summary"])
                self.assertSegmentNotExists("testenv/testds/2007/07-08.odimh5")
                self.assertSegmentExists("testenv/testds/2007/10-09.odimh5", extensions=[""])

                self.assertCheckClean(env, files=2, items=2)
                # TODO: wassert(f.query_results({1, 2}));


class ArkiCheckNonSimpleTestsMixin:
    def test_issue57(self):
        arki.test.skip_unless_vm2()
        with self.datasets(format="vm2", unique="reftime, area, product") as env:
            os.makedirs("testenv/testds/2016")
            with open("testenv/testds/2016/10-05.vm2", "wt") as fd:
                fd.write("201610050000,12626,139,70,,,000000000")

            out = self.call_output_success("testenv/testds", "--fix")
            self.assertEqual(
                    out.stdout,
                    b"testds:2016/10-05.vm2: segment found on disk with no associated index data\n"
                    b"testds:2016/10-05.vm2: rescanned\n"
                    b"testds: check 0 files ok, 1 file rescanned\n"
            )

            # arki-query '' issue57 > issue57/todelete.md
            to_delete = env.query()
            # test -s issue57/todelete.md
            self.assertEqual(len(to_delete), 1)
            with open("testenv/testds/todelete.md", "wb") as fd:
                arki.Metadata.write_bundle(to_delete, file=fd)

            # runtest "arki-check --remove=issue57/todelete.md issue57"
            out = self.call_output_success("testenv/testds", "--remove=testenv/testds/todelete.md")
            self.assertEqual(out.stdout, b"testds: 1 data would be deleted\n")

            out = self.call_output_success("testenv/testds", "--remove=testenv/testds/todelete.md", "--fix")
            self.assertEqual(out.stdout, b"")

            mds = env.query()
            self.assertEqual(len(mds), 0)


class TestArkiCheckOndisk2(ArkiCheckNonSimpleTestsMixin, ArkiCheckTestsBase, unittest.TestCase):
    def dataset_config(self, **kw):
        kw["type"] = "ondisk2"
        return kw


class TestArkiCheckIseg(ArkiCheckNonSimpleTestsMixin, ArkiCheckTestsBase, unittest.TestCase):
    def dataset_config(self, **kw):
        kw["type"] = "iseg"
        return kw


class TestArkiCheckSimplePlain(ArkiCheckTestsBase, unittest.TestCase):
    def dataset_config(self, **kw):
        kw["type"] = "simple"
        kw["index_type"] = "plain"
        return kw


class TestArkiCheckSimpleSqlite(ArkiCheckTestsBase, unittest.TestCase):
    def dataset_config(self, **kw):
        kw["type"] = "simple"
        kw["index_type"] = "sqlite"
        return kw
