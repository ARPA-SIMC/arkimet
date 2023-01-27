# python 3.7+ from __future__ import annotations

import os
import unittest
from contextlib import contextmanager

from arkimet.cmdline.maint import Maint
from arkimet.test import CmdlineTestMixin, Env


class ArkiMaintTestsBase(CmdlineTestMixin):
    command = Maint

    @contextmanager
    def datasets(self, **kw):
        kw.setdefault("format", "grib")
        with Env(**self.dataset_config(**kw)) as env:
            yield env

    def test_archive(self):
        with self.datasets() as env:
            env.import_file("inbound/fixture.grib1")
            env.update_config(**{"archive age": "1"})
            env.repack(readonly=False)

            self.assertTrue(os.path.exists("testenv/testds/.archive/last/2007/07-08.grib"))
            self.assertFalse(os.path.exists("testenv/testds/2007/07-08.grib"))

            mdc = env.query(matcher="reftime:=2007-07-08")
            self.assertEqual(len(mdc), 1)
            blob = mdc[0].to_python("source")
            self.assertEqual(blob["file"], "2007/07-08.grib")
            self.assertEqual(blob["basedir"], os.path.abspath("testenv/testds/.archive/last"))

            out = self.call_output_success("unarchive", "testenv/testds/.archive/last/2007/07-08.grib")
            self.assertEqual(out, "")

            self.assertFalse(os.path.exists("testenv/testds/.archive/last/2007/07-08.grib"))
            self.assertTrue(os.path.exists("testenv/testds/2007/07-08.grib"))

            mdc = env.query(matcher="reftime:=2007-07-08")
            self.assertEqual(len(mdc), 1)
            blob = mdc[0].to_python("source")
            self.assertEqual(blob["file"], "2007/07-08.grib")
            self.assertEqual(blob["basedir"], os.path.abspath("testenv/testds"))


class TestArkiMaintIseg(ArkiMaintTestsBase, unittest.TestCase):
    def dataset_config(self, **kw):
        kw["type"] = "iseg"
        return kw


class TestArkiMaintSimplePlain(ArkiMaintTestsBase, unittest.TestCase):
    def dataset_config(self, **kw):
        kw["type"] = "simple"
        kw["index_type"] = "plain"
        return kw


class TestArkiMaintSimpleSqlite(ArkiMaintTestsBase, unittest.TestCase):
    def dataset_config(self, **kw):
        kw["type"] = "simple"
        kw["index_type"] = "sqlite"
        return kw
