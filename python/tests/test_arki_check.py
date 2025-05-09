# python 3.7+ from __future__ import annotations
import arkimet as arki
import datetime
import shutil
import unittest
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Tuple
from arkimet.cmdline.check import Check
from arkimet.test import Env, CmdlineTestMixin, LogCapture


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

    def segment_remove(self, ds, relpath, message):
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


class ArkiCheckTestsBase(CmdlineTestMixin, unittest.TestCase):
    command = Check

    @contextmanager
    def datasets(self, **kw):
        kw.setdefault("format", "grib")
        with Env(**self.dataset_config(**kw)) as env:
            yield env

    def assertCheckClean(self, env, files=None, items=None, **kw):
        """
        Check that the dataset results clean to a check run
        """
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

    def assertSegmentExists(self, env, pathname, extensions=None):
        """
        Check that the given segment exists, optionally verifying that it only
        exists with the given extensions
        """
        if extensions is None:
            extensions = ("", ".gz", ".tar", ".zip")
            if not any(os.path.exists(pathname + ext) for ext in extensions):
                self.fail("Segment {} does not exist (also tried .gz, .tar, and .zip)".format(pathname))
        else:
            all_extensions = frozenset(("", ".gz", ".tar", ".zip", ".gz.idx", ".metadata", ".summary"))
            if env.ds_cfg["type"] == "simple" or ".archive/" in pathname:
                extensions = list(extensions) + [".metadata", ".summary"]

            for ext in extensions:
                if not os.path.exists(pathname + ext):
                    self.fail("Segment {}{} does not exist but it should".format(pathname, ext))

            for ext in all_extensions - frozenset(extensions):
                if os.path.exists(pathname + ext):
                    self.fail("Segment {}{} exists but it should not".format(pathname, ext))

    def assertSegmentNotExists(self, env, pathname):
        """
        Check that the given segment does not exist in any possible version
        """
        all_extensions = ("", ".gz", ".tar", ".zip", ".gz.idx", ".metadata", ".summary")
        for ext in all_extensions:
            if os.path.exists(pathname + ext):
                self.fail("Segment {}{} exists but it should not".format(pathname, ext))

    def assertQueryResults(self, env, imported, results):
        """
        Compare the query results of the dataset against what was imported
        """
        mds = env.query(with_data=True)
        expected = [imported[i].to_python("reftime") for i in results]
        mds = [x.to_python("reftime") for x in mds]
        self.assertEqual(mds, expected)

    def test_clean(self):
        with self.datasets() as env:
            env.import_file("inbound/fixture.grib1")

            out = self.call_output_success("testenv/testds")
            self.assertEqual(out, "testds: check 3 files ok\n")

            out = self.call_output_success("testenv/testds", "--fix")
            self.assertEqual(out, "testds: check 3 files ok\n")

            out = self.call_output_success("testenv/testds", "--repack")
            self.assertEqual(out, "testds: repack 3 files ok\n")

            out = self.call_output_success("testenv/testds", "--repack", "--fix")
            self.assertRegex(
                out,
                r"(testds: repack: running VACUUM ANALYZE on the dataset index(, if applicable)?\n)?"
                r"(testds: repack: rebuilding the summary cache\n)?"
                r"testds: repack 3 files ok\n",
            )

    def test_clean_filtered(self):
        with self.datasets() as env:
            env.import_file("inbound/fixture.grib1")

            out = self.call_output_success("testenv/testds", "--filter=reftime:>=2007-07-08")
            self.assertEqual(out, "testds: check 2 files ok\n")

            out = self.call_output_success("testenv/testds", "--fix", "--filter=reftime:>=2007-07-08")
            self.assertEqual(out, "testds: check 2 files ok\n")

            out = self.call_output_success("testenv/testds", "--repack", "--filter=reftime:>=2007-07-08")
            self.assertEqual(out, "testds: repack 2 files ok\n")

            out = self.call_output_success("testenv/testds", "--repack", "--fix", "--filter=reftime:>=2007-07-08")
            self.assertRegex(
                out,
                r"(testds: repack: running VACUUM ANALYZE on the dataset index(, if applicable)?\n)?"
                r"(testds: repack: rebuilding the summary cache\n)?"
                r"testds: repack 2 files ok\n",
            )

    def test_remove_all(self):
        with self.datasets() as env:
            env.import_file("inbound/fixture.grib1")

            self.assertTrue(os.path.exists("testenv/testds/2007/07-08.grib"))
            self.assertTrue(os.path.exists("testenv/testds/2007/07-07.grib"))
            self.assertTrue(os.path.exists("testenv/testds/2007/10-09.grib"))

            out = self.call_output_success("testenv/testds", "--remove-all")
            self.assertEqual(
                out,
                "testds:2007/07-07.grib: should be deleted\n"
                "testds:2007/07-08.grib: should be deleted\n"
                "testds:2007/10-09.grib: should be deleted\n",
            )

            self.assertTrue(os.path.exists("testenv/testds/2007/07-08.grib"))
            self.assertTrue(os.path.exists("testenv/testds/2007/07-07.grib"))
            self.assertTrue(os.path.exists("testenv/testds/2007/10-09.grib"))

            out = self.call_output_success("testenv/testds", "--remove-all", "-f")
            self.assertRegex(
                out,
                r"testds:2007/07-07.grib: deleted \(\d+ freed\)\n"
                r"testds:2007/07-08.grib: deleted \(\d+ freed\)\n"
                r"testds:2007/10-09.grib: deleted \(\d+ freed\)\n",
            )

            self.assertFalse(os.path.exists("testenv/testds/2007/07-08.grib"))
            self.assertFalse(os.path.exists("testenv/testds/2007/07-07.grib"))
            self.assertFalse(os.path.exists("testenv/testds/2007/10-09.grib"))

            self.assertCheckClean(env, files=0, items=0)
            self.assertEqual(env.query(), [])

    def test_remove_all_filtered(self):
        with self.datasets() as env:
            imported = env.import_file("inbound/fixture.grib1")

            self.assertTrue(os.path.exists("testenv/testds/2007/07-08.grib"))
            self.assertTrue(os.path.exists("testenv/testds/2007/07-07.grib"))
            self.assertTrue(os.path.exists("testenv/testds/2007/10-09.grib"))

            out = self.call_output_success("testenv/testds", "--remove-all", "--filter=reftime:=2007-07-08")
            self.assertEqual(out, "testds:2007/07-08.grib: should be deleted\n")

            self.assertTrue(os.path.exists("testenv/testds/2007/07-08.grib"))
            self.assertTrue(os.path.exists("testenv/testds/2007/07-07.grib"))
            self.assertTrue(os.path.exists("testenv/testds/2007/10-09.grib"))

            out = self.call_output_success("testenv/testds", "--remove-all", "-f", "--filter=reftime:=2007-07-08")
            self.assertRegex(out, r"testds:2007/07-08.grib: deleted \(\d+ freed\)\n")

            self.assertFalse(os.path.exists("testenv/testds/2007/07-08.grib"))
            self.assertTrue(os.path.exists("testenv/testds/2007/07-07.grib"))
            self.assertTrue(os.path.exists("testenv/testds/2007/10-09.grib"))

            self.assertCheckClean(env, files=2, items=2)
            self.assertQueryResults(env, imported, [1, 2])

    def test_archive(self):
        with self.datasets() as env:
            env.import_file("inbound/fixture.grib1")
            env.update_config(**{"archive age": "1"})

            out = self.call_output_success("testenv/testds")
            self.assertEqual(
                out,
                "testds:2007/07-07.grib: segment old enough to be archived\n"
                "testds:2007/07-08.grib: segment old enough to be archived\n"
                "testds:2007/10-09.grib: segment old enough to be archived\n"
                "testds: check 0 files ok\n",
            )

            out = self.call_output_success("testenv/testds", "--fix")
            self.assertEqual(
                out,
                "testds:2007/07-07.grib: segment old enough to be archived\n"
                "testds:2007/07-08.grib: segment old enough to be archived\n"
                "testds:2007/10-09.grib: segment old enough to be archived\n"
                "testds: check 0 files ok\n",
            )

            out = self.call_output_success("testenv/testds", "--repack")
            self.assertEqual(
                out,
                "testds:2007/07-07.grib: segment old enough to be archived\n"
                "testds:2007/07-07.grib: should be archived\n"
                "testds:2007/07-08.grib: segment old enough to be archived\n"
                "testds:2007/07-08.grib: should be archived\n"
                "testds:2007/10-09.grib: segment old enough to be archived\n"
                "testds:2007/10-09.grib: should be archived\n"
                "testds: repack 0 files ok, 3 files should be archived\n",
            )

            out = self.call_output_success("testenv/testds", "--repack", "--fix", "--online", "--offline")
            self.assertRegex(
                out,
                r"testds:2007/07-07.grib: segment old enough to be archived\n"
                r"testds:2007/07-07.grib: archived\n"
                r"testds:2007/07-08.grib: segment old enough to be archived\n"
                r"testds:2007/07-08.grib: archived\n"
                r"testds:2007/10-09.grib: segment old enough to be archived\n"
                r"testds:2007/10-09.grib: archived\n"
                r"(testds: repack: running VACUUM ANALYZE on the dataset index(, if applicable)?\n)?"
                r"(testds: repack: rebuilding the summary cache\n)?"
                r"testds: repack 0 files ok, 3 files archived\n"
                r"testds.archives.last: repack 3 files ok\n",
            )

            self.assertTrue(os.path.exists("testenv/testds/.archive/last/2007/07-08.grib"))
            self.assertFalse(os.path.exists("testenv/testds/2007/07-08.grib"))

            mdc = env.query(matcher="reftime:=2007-07-08")
            self.assertEqual(len(mdc), 1)
            blob = mdc[0].to_python("source")
            self.assertEqual(blob["file"], "2007/07-08.grib")
            self.assertEqual(blob["basedir"], os.path.abspath("testenv/testds/.archive/last"))

            out = self.call_output_success("testenv/testds", "--unarchive=2007/07-08.grib")
            self.assertEqual(out, "")

            self.assertFalse(os.path.exists("testenv/testds/.archive/last/2007/07-08.grib"))
            self.assertTrue(os.path.exists("testenv/testds/2007/07-08.grib"))

            mdc = env.query(matcher="reftime:=2007-07-08")
            self.assertEqual(len(mdc), 1)
            blob = mdc[0].to_python("source")
            self.assertEqual(blob["file"], "2007/07-08.grib")
            self.assertEqual(blob["basedir"], os.path.abspath("testenv/testds"))

    def test_tar(self):
        with self.datasets(format="odimh5") as env:
            imported = []
            imported += env.import_file("inbound/fixture.odimh5/00.odimh5")
            imported += env.import_file("inbound/fixture.odimh5/01.odimh5")
            imported += env.import_file("inbound/fixture.odimh5/02.odimh5")

            env.update_config(**{"archive age": "1"})
            with arki.dataset.SessionTimeOverride(1184018400):  # date +%s --date="2007-07-10"
                env.repack(readonly=False)

                self.assertTrue(os.path.exists("testenv/testds/.archive/last/2007/07-07.odimh5"))
                self.assertTrue(os.path.exists("testenv/testds/2007/07-08.odimh5"))
                self.assertTrue(os.path.exists("testenv/testds/2007/10-09.odimh5"))

                out = self.call_output_success("testenv/testds", "--tar")
                self.assertEqual(out, "testds.archives.last:2007/07-07.odimh5: should be tarred\n")

                out = self.call_output_success("testenv/testds", "--tar", "--offline")
                self.assertEqual(out, "testds.archives.last:2007/07-07.odimh5: should be tarred\n")

                out = self.call_output_success("testenv/testds", "--tar", "--online")
                self.assertEqual(
                    out, "testds:2007/07-08.odimh5: should be tarred\n" "testds:2007/10-09.odimh5: should be tarred\n"
                )

                out = self.call_output_success("testenv/testds", "--tar", "--fix")
                self.assertEqual(out, "testds.archives.last:2007/07-07.odimh5: tarred\n")

                self.assertSegmentExists(env, "testenv/testds/.archive/last/2007/07-07.odimh5", extensions=[".tar"])
                self.assertSegmentExists(env, "testenv/testds/2007/07-08.odimh5")
                self.assertSegmentExists(env, "testenv/testds/2007/10-09.odimh5")

                self.assertCheckClean(env, files=3, items=3)
                self.assertCheckClean(env, files=3, items=3, accurate=True)
                self.assertQueryResults(env, imported, [1, 0, 2])

    def test_zip(self):
        arki.test.skip_unless_libzip()
        arki.test.skip_unless_libarchive()
        with self.datasets(format="odimh5") as env:
            imported = []
            imported += env.import_file("inbound/fixture.odimh5/00.odimh5")
            imported += env.import_file("inbound/fixture.odimh5/01.odimh5")
            imported += env.import_file("inbound/fixture.odimh5/02.odimh5")

            env.update_config(**{"archive age": "1"})
            with arki.dataset.SessionTimeOverride(1184018400):  # date +%s --date="2007-07-10"
                env.repack(readonly=False)

                self.assertTrue(os.path.exists("testenv/testds/.archive/last/2007/07-07.odimh5"))
                self.assertTrue(os.path.exists("testenv/testds/2007/07-08.odimh5"))
                self.assertTrue(os.path.exists("testenv/testds/2007/10-09.odimh5"))

                out = self.call_output_success("testenv/testds", "--zip")
                self.assertEqual(out, "testds.archives.last:2007/07-07.odimh5: should be zipped\n")

                out = self.call_output_success("testenv/testds", "--zip", "--offline")
                self.assertEqual(out, "testds.archives.last:2007/07-07.odimh5: should be zipped\n")

                out = self.call_output_success("testenv/testds", "--zip", "--online")
                self.assertEqual(
                    out, "testds:2007/07-08.odimh5: should be zipped\n" "testds:2007/10-09.odimh5: should be zipped\n"
                )

                out = self.call_output_success("testenv/testds", "--zip", "--fix")
                self.assertEqual(out, "testds.archives.last:2007/07-07.odimh5: zipped\n")

                self.assertSegmentExists(env, "testenv/testds/.archive/last/2007/07-07.odimh5", extensions=[".zip"])
                self.assertSegmentExists(env, "testenv/testds/2007/07-08.odimh5")
                self.assertSegmentExists(env, "testenv/testds/2007/10-09.odimh5")

                self.assertCheckClean(env, files=3, items=3)
                self.assertCheckClean(env, files=3, items=3, accurate=True)
                self.assertQueryResults(env, imported, [1, 0, 2])

    def test_compress(self):
        with self.datasets() as env:
            imported = env.import_file("inbound/fixture.grib1")
            env.update_config(**{"archive age": "1", "gz group size": "0"})
            with arki.dataset.SessionTimeOverride(1184018400):  # date +%s --date="2007-07-10"
                env.repack(readonly=False)

                self.assertTrue(os.path.exists("testenv/testds/.archive/last/2007/07-07.grib"))
                self.assertTrue(os.path.exists("testenv/testds/2007/07-08.grib"))
                self.assertTrue(os.path.exists("testenv/testds/2007/10-09.grib"))

                out = self.call_output_success("testenv/testds", "--compress")
                self.assertEqual(out, "testds.archives.last:2007/07-07.grib: should be compressed\n")

                out = self.call_output_success("testenv/testds", "--compress", "--offline")
                self.assertEqual(out, "testds.archives.last:2007/07-07.grib: should be compressed\n")

                out = self.call_output_success("testenv/testds", "--compress", "--online")
                self.assertEqual(
                    out,
                    "testds:2007/07-08.grib: should be compressed\n" "testds:2007/10-09.grib: should be compressed\n",
                )

                out = self.call_output_success("testenv/testds", "--compress", "--fix")
                self.assertRegex(out, r"testds.archives.last:2007/07-07.grib: compressed \(\d+ freed\)\n")

                self.assertSegmentExists(env, "testenv/testds/.archive/last/2007/07-07.grib", extensions=[".gz"])
                self.assertSegmentExists(env, "testenv/testds/2007/07-08.grib")
                self.assertSegmentExists(env, "testenv/testds/2007/10-09.grib")

                self.assertCheckClean(env, files=3, items=3)
                self.assertCheckClean(env, files=3, items=3, accurate=True)
                self.assertQueryResults(env, imported, [1, 0, 2])

    def test_scan(self):
        self.maxDiff = None
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
                    out,
                    "testds:2007/07-08.odimh5: OK 2007-07-08 00:00:00Z to 2007-07-09 00:00:00Z\n"
                    "testds:2007/10-09.odimh5: OK 2007-10-09 00:00:00Z to 2007-10-10 00:00:00Z\n"
                    "testds.archives.last:2007/07-07.odimh5: OK 2007-07-07 00:00:00Z to 2007-07-08 00:00:00Z\n",
                )

                out = self.call_output_success("testenv/testds", "--state", "--offline")
                self.assertEqual(
                    out, "testds.archives.last:2007/07-07.odimh5: OK 2007-07-07 00:00:00Z to 2007-07-08 00:00:00Z\n"
                )

                out = self.call_output_success("testenv/testds", "--state", "--online")
                self.assertEqual(
                    out,
                    "testds:2007/07-08.odimh5: OK 2007-07-08 00:00:00Z to 2007-07-09 00:00:00Z\n"
                    "testds:2007/10-09.odimh5: OK 2007-10-09 00:00:00Z to 2007-10-10 00:00:00Z\n",
                )

    def test_remove_old(self):
        with self.datasets(format="odimh5") as env:
            imported = []
            imported += env.import_file("inbound/fixture.odimh5/00.odimh5")
            imported += env.import_file("inbound/fixture.odimh5/01.odimh5")
            imported += env.import_file("inbound/fixture.odimh5/02.odimh5")

            with arki.dataset.SessionTimeOverride(1184104800):  # date +%s --date="2007-07-11"
                env.update_config(**{"archive age": "2"})
                env.repack(readonly=False)
                env.update_config(**{"delete age": "1"})

                self.assertTrue(os.path.exists("testenv/testds/.archive/last/2007/07-07.odimh5"))
                self.assertTrue(os.path.exists("testenv/testds/2007/07-08.odimh5"))
                self.assertTrue(os.path.exists("testenv/testds/2007/10-09.odimh5"))

                out = self.call_output_success("testenv/testds", "--remove-old")
                self.assertEqual(
                    out,
                    "testds:2007/07-08.odimh5: segment old enough to be deleted\n"
                    "testds:2007/07-08.odimh5: should be deleted\n",
                )

                out = self.call_output_success("testenv/testds", "--remove-old", "--offline")
                self.assertEqual(
                    out,
                    # "testds.archives.last:2007/07-07.odimh5: segment old enough to be deleted\n"
                    "testds.archives.last:2007/07-07.odimh5: should be deleted\n",
                )

                out = self.call_output_success("testenv/testds", "--remove-old", "--online")
                self.assertEqual(
                    out,
                    "testds:2007/07-08.odimh5: segment old enough to be deleted\n"
                    "testds:2007/07-08.odimh5: should be deleted\n",
                )

                out = self.call_output_success("testenv/testds", "--remove-old", "--online", "--offline")
                self.assertEqual(
                    out,
                    "testds:2007/07-08.odimh5: segment old enough to be deleted\n"
                    "testds:2007/07-08.odimh5: should be deleted\n"
                    "testds.archives.last:2007/07-07.odimh5: should be deleted\n",
                )

                out = self.call_output_success("testenv/testds", "--remove-old", "--fix")
                self.assertRegex(
                    out,
                    r"^testds:2007/07-08.odimh5: segment old enough to be deleted\n"
                    r"testds:2007/07-08.odimh5: deleted \(\d+ freed\)\n",
                )

                self.assertSegmentExists(env, "testenv/testds/.archive/last/2007/07-07.odimh5", extensions=[""])
                self.assertSegmentNotExists(env, "testenv/testds/2007/07-08.odimh5")
                self.assertSegmentExists(env, "testenv/testds/2007/10-09.odimh5", extensions=[""])

                self.assertCheckClean(env, files=2, items=2)
                self.assertQueryResults(env, imported, [1, 2])

    def _summary_range(self, env: Env, cfg: Optional[arki.cfg.Section] = None) -> Tuple[datetime.date, datetime.date]:
        """Get the reftime range of the dataset according to summary."""
        with env.session.dataset_reader(cfg=cfg) as source:
            summary = source.query_summary()
            reftime = summary.reference_time
            return reftime[0].date(), reftime[1].date()

    def assertDeleteUpdatesSummary(self, env: Env, repo: Path) -> None:
        """Check that deleting items from a dataset affects its summary."""
        cfg = arki.dataset.read_config(repo)

        queried = env.query(cfg)
        self.assertEqual(
            [md["reftime"] for md in queried],
            ["2007-07-07T00:00:00Z", "2007-07-08T13:00:00Z", "2007-10-09T00:00:00Z"],
        )
        self.assertEqual(self._summary_range(env, cfg), (datetime.date(2007, 7, 7), datetime.date(2007, 10, 9)))

        to_delete = [queried[0]]
        to_delete[0].make_absolute()
        with open("testenv/todelete.md", "wb") as fd:
            arki.Metadata.write_bundle(to_delete, file=fd)
        out = self.call_output_success(repo.as_posix(), "--remove=testenv/todelete.md", "--fix")
        self.assertEqual(out, "")

        post_delete = env.query(cfg)
        self.assertEqual(
            [md["reftime"] for md in post_delete],
            ["2007-07-08T13:00:00Z", "2007-10-09T00:00:00Z"],
        )
        self.assertEqual(self._summary_range(env, cfg), (datetime.date(2007, 7, 8), datetime.date(2007, 10, 9)))

    def test_delete_from_online(self) -> None:
        with self.datasets() as env:
            env.import_file("inbound/fixture.grib1")
            self.assertDeleteUpdatesSummary(env, env.dsroot)

    def test_delete_from_last(self) -> None:
        with self.datasets() as env:
            env.import_file("inbound/fixture.grib1")
            env.update_config(**{"archive age": "1"})
            env.repack(readonly=False)

            last = env.dsroot / ".archive" / "last"
            self.assertDeleteUpdatesSummary(env, last)

    def test_delete_from_archived(self) -> None:
        with self.datasets() as env:
            env.import_file("inbound/fixture.grib1")
            env.update_config(**{"archive age": "1"})
            env.repack(readonly=False)

            last = env.dsroot / ".archive" / "last"
            archive = env.dsroot / ".archive" / "test"
            last.rename(archive)
            self.assertDeleteUpdatesSummary(env, archive)

    def test_delete_from_offline(self) -> None:
        with self.datasets() as env:
            env.import_file("inbound/fixture.grib1")
            env.update_config(**{"archive age": "1"})
            env.repack(readonly=False)

            last = env.dsroot / ".archive" / "last"
            archive = env.dsroot / ".archive" / "test"
            last.rename(archive)
            shutil.copyfile(archive / "summary", env.dsroot / ".archive" / "test.summary")
            self.assertDeleteUpdatesSummary(env, archive)


class TestArkiCheckIseg(ArkiCheckTestsBase, unittest.TestCase):
    def dataset_config(self, **kw):
        kw["type"] = "iseg"
        return kw

    def test_issue57(self):
        arki.test.skip_unless_vm2()
        with self.datasets(format="vm2", unique="reftime, area, product", skip_initial_check=True) as env:
            os.makedirs("testenv/testds/2016")
            with open("testenv/testds/2016/10-05.vm2", "wt") as fd:
                fd.write("201610050000,12626,139,70,,,000000000")

            out = self.call_output_success("testenv/testds", "--fix")
            self.assertEqual(
                out,
                "testds:2016/10-05.vm2: segment found on disk with no associated index\n"
                "testds:2016/10-05.vm2: rescanned\n"
                "testds: check 0 files ok, 1 file rescanned\n",
            )

            # arki-query '' issue57 > issue57/todelete.md
            to_delete = env.query()
            # test -s issue57/todelete.md
            self.assertEqual(len(to_delete), 1)
            with open("testenv/testds/todelete.md", "wb") as fd:
                arki.Metadata.write_bundle(to_delete, file=fd)

            # runtest "arki-check --remove=issue57/todelete.md issue57"
            with LogCapture() as log:
                out = self.call_output_success("testenv/testds", "--remove=testenv/testds/todelete.md")
            self.assertEqual(len(log), 1)
            self.assertEqual(log[0].name, "arkimet")
            self.assertEqual(log[0].levelname, "WARNING")
            self.assertEqual(log[0].getMessage(), "testds: 1 data would be deleted")
            self.assertEqual(out, "")

            out = self.call_output_success("testenv/testds", "--remove=testenv/testds/todelete.md", "--fix")
            self.assertEqual(out, "")

            mds = env.query()
            self.assertEqual(len(mds), 0)

    def test_remove(self):
        with self.datasets() as env:
            imported = env.import_file("inbound/fixture.grib1")

            imported[0].make_absolute()
            with open("testenv/remove.md", "wb") as fd:
                arki.Metadata.write_bundle(imported[0:1], file=fd)

            with LogCapture() as log:
                out = self.call_output_success("testenv/testds", "--remove=testenv/remove.md")
            self.assertEqual(len(log), 1)
            self.assertEqual(log[0].name, "arkimet")
            self.assertEqual(log[0].levelname, "WARNING")
            self.assertEqual(log[0].getMessage(), "testds: 1 data would be deleted")

            self.assertCheckClean(env, files=3, items=3)
            self.assertQueryResults(env, imported, [1, 0, 2])

            with LogCapture() as log:
                out = self.call_output_success("testenv/testds", "--remove=testenv/remove.md", "--verbose", "--fix")
            self.assertEqual(log[0].name, "arkimet")
            self.assertEqual(log[0].levelname, "INFO")
            self.assertEqual(log[0].getMessage(), "testds: 2007/07-08.grib: 1 data marked as deleted")
            self.assertEqual(log[1].name, "arkimet")
            self.assertEqual(log[1].levelname, "INFO")
            self.assertEqual(log[1].getMessage(), "testds: 1 data marked as deleted")
            self.assertEqual(log[2:], [])
            self.assertEqual(out, "")

            self.assertQueryResults(env, imported, [1, 2])

            state = env.segment_state()
            self.assertEqual(len(state), 3)

            self.assertEqual(state["testds:2007/07-07.grib"], "OK")
            self.assertEqual(state["testds:2007/07-08.grib"], "DELETED")
            self.assertEqual(state["testds:2007/10-09.grib"], "OK")

    def test_tar_archives(self):
        with self.datasets(format="odimh5") as env:
            imported = []
            imported += env.import_file("inbound/fixture.odimh5/00.odimh5")
            imported += env.import_file("inbound/fixture.odimh5/01.odimh5")
            imported += env.import_file("inbound/fixture.odimh5/02.odimh5")

            with arki.dataset.SessionTimeOverride(1184018400):  # date +%s --date="2007-07-10"
                env.update_config(**{"archive age": "1"})
                env.repack(readonly=False)

                os.rename("testenv/testds/.archive/last", "testenv/testds/.archive/2007")

                self.assertTrue(os.path.exists("testenv/testds/.archive/2007/2007/07-07.odimh5"))
                self.assertTrue(os.path.exists("testenv/testds/2007/07-08.odimh5"))
                self.assertTrue(os.path.exists("testenv/testds/2007/10-09.odimh5"))

                out = self.call_output_success("testenv/testds", "--tar")
                self.assertEqual(out, "testds.archives.2007:2007/07-07.odimh5: should be tarred\n")

                out = self.call_output_success("testenv/testds", "--tar", "--offline")
                self.assertEqual(out, "testds.archives.2007:2007/07-07.odimh5: should be tarred\n")

                out = self.call_output_success("testenv/testds", "--tar", "--online")
                self.assertEqual(
                    out, "testds:2007/07-08.odimh5: should be tarred\n" "testds:2007/10-09.odimh5: should be tarred\n"
                )

                out = self.call_output_success("testenv/testds", "--tar", "--fix")
                self.assertEqual(out, "testds.archives.2007:2007/07-07.odimh5: tarred\n")

                self.assertSegmentExists(env, "testenv/testds/.archive/2007/2007/07-07.odimh5", extensions=[".tar"])
                self.assertSegmentExists(env, "testenv/testds/2007/07-08.odimh5")
                self.assertSegmentExists(env, "testenv/testds/2007/10-09.odimh5")

                self.assertCheckClean(env, files=3, items=3)
                self.assertCheckClean(env, files=3, items=3, accurate=True)
                self.assertQueryResults(env, imported, [1, 0, 2])

    def test_zip_archives(self):
        arki.test.skip_unless_libzip()
        arki.test.skip_unless_libarchive()

        with self.datasets(format="odimh5") as env:
            imported = []
            imported += env.import_file("inbound/fixture.odimh5/00.odimh5")
            imported += env.import_file("inbound/fixture.odimh5/01.odimh5")
            imported += env.import_file("inbound/fixture.odimh5/02.odimh5")

            with arki.dataset.SessionTimeOverride(1184018400):  # date +%s --date="2007-07-10"
                env.update_config(**{"archive age": "1"})
                env.repack(readonly=False)

                os.rename("testenv/testds/.archive/last", "testenv/testds/.archive/2007")

                self.assertTrue(os.path.exists("testenv/testds/.archive/2007/2007/07-07.odimh5"))
                self.assertTrue(os.path.exists("testenv/testds/2007/07-08.odimh5"))
                self.assertTrue(os.path.exists("testenv/testds/2007/10-09.odimh5"))

                out = self.call_output_success("testenv/testds", "--zip")
                self.assertEqual(out, "testds.archives.2007:2007/07-07.odimh5: should be zipped\n")

                out = self.call_output_success("testenv/testds", "--zip", "--offline")
                self.assertEqual(out, "testds.archives.2007:2007/07-07.odimh5: should be zipped\n")

                out = self.call_output_success("testenv/testds", "--zip", "--online")
                self.assertEqual(
                    out, "testds:2007/07-08.odimh5: should be zipped\n" "testds:2007/10-09.odimh5: should be zipped\n"
                )

                out = self.call_output_success("testenv/testds", "--zip", "--fix")
                self.assertEqual(out, "testds.archives.2007:2007/07-07.odimh5: zipped\n")

                self.assertSegmentExists(env, "testenv/testds/.archive/2007/2007/07-07.odimh5", extensions=[".zip"])
                self.assertSegmentExists(env, "testenv/testds/2007/07-08.odimh5")
                self.assertSegmentExists(env, "testenv/testds/2007/10-09.odimh5")

                self.assertCheckClean(env, files=3, items=3)
                self.assertCheckClean(env, files=3, items=3, accurate=True)
                self.assertQueryResults(env, imported, [1, 0, 2])


class TestArkiCheckSimple(ArkiCheckTestsBase, unittest.TestCase):
    def dataset_config(self, **kw):
        kw["type"] = "simple"
        return kw


del ArkiCheckTestsBase
