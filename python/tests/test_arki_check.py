import arkimet as arki
import unittest
import shutil
import os
from contextlib import contextmanager
# from arkimet.cmdline.check import Check
from arkimet.test import CatchOutput


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

    def import_file(self, pathname):
        dest = arki.dataset.Writer(self.ds_cfg)

        source = arki.dataset.Reader({
            "format": "grib",
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


class ArkiCheckTestsBase:
    @contextmanager
    def datasets(self, **kw):
        env = Env(**self.dataset_config(
                format="grib",
                name="testds",
                path=os.path.abspath("testenv/testds"),
                step="daily",
        ))

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

    def test_clean(self):
        with self.datasets() as env:
            env.import_file("inbound/fixture.grib1")

            out = CatchOutput()
            with out.redirect():
                res = self.runcmd("testenv/testds")
            self.assertEqual(out.stderr, b"")
            self.assertEqual(out.stdout, b"testds: check 3 files ok\n")
            self.assertIsNone(res)

            out = CatchOutput()
            with out.redirect():
                res = self.runcmd("testenv/testds", "--fix")
            self.assertEqual(out.stderr, b"")
            self.assertEqual(out.stdout, b"testds: check 3 files ok\n")
            self.assertIsNone(res)

            out = CatchOutput()
            with out.redirect():
                res = self.runcmd("testenv/testds", "--repack")
            self.assertEqual(out.stderr, b"")
            self.assertEqual(out.stdout, b"testds: repack 3 files ok\n")
            self.assertIsNone(res)

            out = CatchOutput()
            with out.redirect():
                res = self.runcmd("testenv/testds", "--repack", "--fix")
            self.assertEqual(out.stderr, b"")
            self.assertRegex(
                    out.stdout,
                    rb"(testds: repack: running VACUUM ANALYZE on the dataset index(, if applicable)?\n)?"
                    rb"(testds: repack: rebuilding the summary cache\n)?"
                    rb"testds: repack 3 files ok\n"
            )
            self.assertIsNone(res)


class TestArkiCheckOndisk2(ArkiCheckTestsBase, unittest.TestCase):
    def dataset_config(self, **kw):
        kw["type"] = "ondisk2"
        return kw


class TestArkiCheckIseg(ArkiCheckTestsBase, unittest.TestCase):
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
