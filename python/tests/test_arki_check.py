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

    def query(self, *args, **kw):
        res = []

        def on_metadata(md):
            res.append(md)

        kw["on_metadata"] = on_metadata
        source = arki.dataset.Reader(self.ds_cfg)
        source.query_data(**kw)
        return res


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

            # TODO wassert(f.ensure_localds_clean(0, 0));
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

            # TODO wassert(f.ensure_localds_clean(2, 2));
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
