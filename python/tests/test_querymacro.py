import unittest
import arkimet as arki
from contextlib import contextmanager
from arkimet.test import Env


class QmacroTestMixin:
    @contextmanager
    def datasets(self, **kw):
        kw.setdefault("format", "grib")
        kw.setdefault("unique", "origin,reftime")
        env = Env(**kw)

        dest = arki.dataset.Writer(env.ds_cfg)

        source = arki.dataset.Reader({
            "format": "grib",
            "name": "test.grib1",
            "path": "inbound/test.grib1",
            "type": "file",
        })
        mds = source.query_data()
        for md in mds:
            for i in range(7, 10):
                md["reftime"] = "2009-08-{:02d}T00:00:00Z".format(i)
                dest.acquire(md)

        dest.flush()

        yield env

        env.cleanup()


class TestNoop(QmacroTestMixin, unittest.TestCase):
    def test_noop(self):
        # Python script that simply passes through the queries
        with self.datasets() as env:
            reader = arki.make_qmacro_dataset(arki.cfg.Section(), env.config, "noop", "testds")
            mdc = reader.query_data()
            self.assertEqual(len(mdc), 9)

            self.assertTrue(mdc[0].has_source())
            self.assertTrue(mdc[1].has_source())
            self.assertTrue(mdc[2].has_source())

            s = reader.query_summary()
            self.assertEqual(s.count, 9)

    def test_noopcopy(self):
        # Lua script that simply passes through the queries, making temporary copies of data
        with self.datasets() as env:
            reader = arki.make_qmacro_dataset(arki.cfg.Section(), env.config, "noopcopy", "testds")
            mdc = reader.query_data()
            self.assertEqual(len(mdc), 9)

            self.assertTrue(mdc[0].has_source())
            self.assertTrue(mdc[1].has_source())
            self.assertTrue(mdc[2].has_source())

            s = reader.query_summary()
            self.assertEqual(s.count, 9)


class TestExpa(QmacroTestMixin, unittest.TestCase):
    def test_expa(self):
        with self.datasets() as env:
            reader = arki.make_qmacro_dataset(
                    arki.cfg.Section(), env.config, "expa",
                    "ds:testds. d:2009-08-07. t:0000. s:AN. l:G00. v:GRIB1/200/140/229.\n"
                    "ds:testds. d:2009-08-07. t:0000. s:GRIB1/1. l:MSL. v:GRIB1/80/2/2.\n")
            mdc = reader.query_data()
            self.assertEqual(len(mdc), 2)

            self.assertTrue(mdc[0].has_source())
            self.assertTrue(mdc[1].has_source())

            s = reader.query_summary()
            self.assertEqual(s.count, 2)

    def test_expa_arg(self):
        # Try "expa" matchers with parameter
        with self.datasets() as env:
            reader = arki.make_qmacro_dataset(
                    arki.cfg.Section(), env.config, "expa 2009-08-08",
                    "ds:testds. d:@. t:0000. s:AN. l:G00. v:GRIB1/200/140/229.\n"
                    "ds:testds. d:@-1. t:0000. s:GRIB1/1. l:MSL. v:GRIB1/80/2/2.\n")
            mdc = reader.query_data()
            self.assertEqual(len(mdc), 2)

            self.assertTrue(mdc[0].has_source())
            self.assertTrue(mdc[1].has_source())

            s = reader.query_summary()
            self.assertEqual(s.count, 2)

    def test_expa_inline(self):
        # Try "expa" matchers with inline option
        with self.datasets() as env:
            reader = arki.make_qmacro_dataset(
                    arki.cfg.Section(), env.config, "expa",
                    "ds:testds. d:2009-08-07. t:0000. s:AN. l:G00. v:GRIB1/200/140/229.\n"
                    "ds:testds. d:2009-08-07. t:0000. s:GRIB1/1. l:MSL. v:GRIB1/80/2/2.\n")
            mdc = reader.query_data(with_data=True)
            self.assertEqual(len(mdc), 2)

            # Ensure that data is reachable
            self.assertEqual(len(mdc[0].data), mdc[0].data_size)
            self.assertEqual(len(mdc[1].data), mdc[1].data_size)

            s = reader.query_summary()
            self.assertEqual(s.count, 2)

    def test_expa_sort(self):
        with self.datasets() as env:
            # Try "expa" matchers with sort option
            reader = arki.make_qmacro_dataset(
                    arki.cfg.Section(), env.config, "expa",
                    "ds:testds. d:2009-08-07. t:0000. s:AN. l:G00. v:GRIB1/200/140/229.\n"
                    "ds:testds. d:2009-08-08. t:0000. s:GRIB1/1. l:MSL. v:GRIB1/80/2/2.\n")
            mdc = reader.query_data(sort="month:-reftime")
            self.assertEqual(len(mdc), 2)

            # TODO: ensure sorting
            self.assertTrue(mdc[0].has_source())
            self.assertEqual(mdc[0]["reftime"], "2009-08-08T00:00:00Z")
            self.assertTrue(mdc[1].has_source())
            self.assertEqual(mdc[1]["reftime"], "2009-08-07T00:00:00Z")

            s = reader.query_summary()
            self.assertEqual(s.count, 2)
