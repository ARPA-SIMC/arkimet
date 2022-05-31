# python 3.7+ from __future__ import annotations
import unittest
import contextlib
import os
import io
import requests
import arkimet as arki
import arkimet.test
from arkimet.cmdline.server import make_server


class Progress:
    def __init__(self):
        self.start_called = 0
        self.update_called = 0
        self.done_called = 0
        self.total_count = 0
        self.total_bytes = 0

    def start(self, expected_count=0, expected_bytes=0):
        self.start_called += 1

    def update(self, count, bytes):
        self.update_called += 1

    def done(self, total_count, total_bytes):
        self.done_called += 1
        self.total_count = total_count
        self.total_bytes = total_bytes


class ExpectedFailure(Exception):
    pass


class ProgressFailUpdate(Progress):
    def update(self, *args):
        raise ExpectedFailure()


class Env(arkimet.test.Env):
    def build_config(self):
        config = super().build_config()

        with self.session.dataset_reader(
                    cfg=arki.dataset.read_config("inbound/fixture.grib1")
                ) as reader:
            mds = reader.query_data()

        for idx, name in enumerate(("test200", "test80")):
            cfg = arki.cfg.Section({
                "name": name,
                "path": os.path.abspath(os.path.join("testenv", name)),
                "type": "iseg",
                "format": "grib",
                "step": "daily",
                "postprocess": "cat,echo,say,checkfiles,error,outthenerr",
            })
            config[name] = cfg

            os.makedirs(os.path.join("testenv", name))
            with open(os.path.join("testenv", name, "config"), "wt") as fd:
                cfg.write(fd)

            with self.session.dataset_checker(cfg=cfg) as checker:
                checker.check(readonly=False)

            with self.session.dataset_writer(cfg=cfg) as writer:
                writer.acquire(mds[idx])
                writer.flush()

        error_cfg = arki.cfg.Section({
            "name": "error",
            "path": os.path.abspath("testenv/error"),
            "type": "error",
            "step": "daily",
        })
        config["error"] = error_cfg

        with open("testenv/testds/error", "wt") as fd:
            error_cfg.write(fd)

        return config


class TestArkiServer(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.ctx = contextlib.ExitStack().__enter__()
        self.env = self.ctx.enter_context(Env(format="grib"))
        # TODO: randomly allocate a port
        self.server = self.ctx.enter_context(make_server("localhost", 0, self.env.config))
        self.server_url = self.server.url
        self.server_process = self.ctx.enter_context(arkimet.test.ServerProcess(self.server))

    def tearDown(self):
        self.ctx.__exit__(None, None, None)

    def test_config(self):
        """
        Query the configuration
        """
        config = arki.dataset.http.load_cfg_sections(self.server_url)

        self.assertIn("test200", config)
        self.assertEqual(config["test200"]["server"], self.server_url)

        self.assertIn("test80", config)
        self.assertEqual(config["test80"]["server"], self.server_url)

        self.assertIn("error", config)
        self.assertEqual(config["error"]["server"], self.server_url)

    def test_metadata(self):
        """
        Test querying the datasets, metadata only
        """
        config = arki.dataset.http.load_cfg_sections(self.server_url)
        with self.env.session.dataset_reader(cfg=config["test200"]) as ds:
            mdc = ds.query_data("origin:GRIB1,200")
            self.assertEqual(len(mdc), 1)
            self.assertEqual(mdc[0].to_python("source"), {
                'type': 'source', 'style': 'URL', 'format': 'grib',
                'url': self.server_url + '/dataset/test200',
            })

            mdc = ds.query_data("origin:GRIB1,80")
            self.assertFalse(mdc)

            mdc = ds.query_data("origin:GRIB1,98")
            self.assertFalse(mdc)

    def test_inline(self):
        """
        Test querying the datasets, with inline data
        """
        config = arki.dataset.http.load_cfg_sections(self.server_url)
        with self.env.session.dataset_reader(cfg=config["test200"]) as ds:
            mdc = ds.query_data("origin:GRIB1,200", True)
            self.assertEqual(len(mdc), 1)
            self.assertEqual(mdc[0].to_python("source"), {
                'type': 'source', 'style': 'INLINE', 'format': 'grib',
                'size': 7218
            })
            self.assertEqual(len(mdc[0].data), 7218)

            mdc = ds.query_data("origin:GRIB1,80", True)
            self.assertFalse(mdc)

    def test_summary(self):
        """
        Test querying the summary
        """
        config = arki.dataset.http.load_cfg_sections(self.server_url)
        with self.env.session.dataset_reader(cfg=config["test200"]) as ds:
            summary = ds.query_summary("origin:GRIB1,200")
            self.assertEqual(summary.count, 1)

    def test_postprocess(self):
        """
        Test querying with postprocessing
        """
        config = arki.dataset.http.load_cfg_sections(self.server_url)
        with self.env.session.dataset_reader(cfg=config["test200"]) as ds:
            data = ds.query_bytes("origin:GRIB1,200", postprocess="say ciao")
            self.assertEqual(data, b"ciao\n")

    def test_issue289(self):
        """
        Test that queries also work over GET (#289)
        """
        res = requests.get(self.server_url + "/query", data={
            "query": "origin:GRIB1,200",
        })
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.content[:2], b"MD")

    def test_error(self):
        """
        Test the server giving an error
        """
        # Try it on metadata
        res = requests.post(self.server_url + "/dataset/test200/query", data={
            "query": "origin:GRIB1,200;MISCHIEF",
        })
        self.assertEqual(res.status_code, 500)
        self.assertIn("subexpression 'MISCHIEF' does not contain a colon", res.headers["Arkimet-Exception"])

        # Try it on summaries
        res = requests.post(self.server_url + "/dataset/test200/summary", data={
            "query": "origin:GRIB1,200;MISCHIEF",
        })
        self.assertEqual(res.status_code, 500)
        self.assertIn("subexpression 'MISCHIEF' does not contain a colon", res.headers["Arkimet-Exception"])

        # Try it on streams
        res = requests.post(self.server_url + "/dataset/test200/query", data={
            "query": "origin:GRIB1,200;MISCHIEF",
            "style": "data",
        })
        self.assertEqual(res.status_code, 500)
        self.assertIn("subexpression 'MISCHIEF' does not contain a colon", res.headers["Arkimet-Exception"])

    def test_qexpand(self):
        """
        Test expanding a query
        """
        config = arki.dataset.http.load_cfg_sections(self.server_url)
        query = arki.dataset.http.expand_remote_query(config, "origin:GRIB1,200;product:t")
        self.assertEqual(
                query,
                "origin:GRIB1,200; product:GRIB1,200,2,11 or GRIB1,98,128,130 or GRIB1,98,128,167"
                " or GRIB1,200,200,11 or GRIB2,200,0,200,11")

        # Expanding a query that fails returns the query itself: if some
        # servers expanded it consistently and all others errored out, then we
        # would send it expanded to all of them and we are ok
        query = arki.dataset.http.expand_remote_query(config, "origin:GRIB1,200;product:pippo")
        self.assertEqual(query, "origin:GRIB1,200;product:pippo")

        res = requests.post(self.server_url + "/qexpand", data={
            "query": "origin:GRIB1,200;product:pippo",
        })
        self.assertEqual(res.status_code, 500)
        self.assertIn("cannot parse Product style 'pippo'", res.headers["Arkimet-Exception"])

    def test_qmacro_noop(self):
        """
        Test querying the datasets via macro
        """
        cfg = arki.cfg.Section(
            name="noop",
            type="remote",
            path=self.server_url,
            qmacro="test200")
        with self.env.session.dataset_reader(cfg=cfg) as ds:
            mdc = ds.query_data()
            self.assertEqual(len(mdc), 1)
            # Check that the source record that comes out is ok
            self.assertEqual(mdc[0].to_python("source"), {
                'type': 'source', 'style': 'URL', 'format': 'grib',
                'url': self.server_url + '/query',
            })

    def test_qmacro_expa(self):
        """
        Test querying the datasets via macro
        """
        cfg = arki.cfg.Section(
            name="expa 2007-07-08",
            type="remote",
            path=self.server_url,
            qmacro="ds:test200. d:@. t:1300. s:GRIB1/0/0h/0h. l:GRIB1/1. v:GRIB1/200/140/229.",
        )
        with self.env.session.dataset_reader(cfg=cfg) as ds:
            mdc = ds.query_data()
            self.assertEqual(len(mdc), 1)
            # Check that the source record that comes out is ok
            self.assertEqual(mdc[0].to_python("source"), {
                'type': 'source', 'style': 'URL', 'format': 'grib',
                'url': self.server_url + '/query',
            })

    def test_qmacro_summary(self):
        """
        Test querying the summary
        """
        cfg = arki.cfg.Section(
            name="noop",
            type="remote",
            path=self.server_url,
            qmacro="test200")
        with self.env.session.dataset_reader(cfg=cfg) as ds:
            summary = ds.query_summary()
            self.assertEqual(summary.count, 1)

    # This is not testable anymore: the subprocess filter now is run after the
    # server has already sent headers.
    #
    # The test can stay here in case future refactorings make a later trigger
    # of data_start_hook possible again, but the real solution to this is
    # https://github.com/ARPA-SIMC/arkimet/issues/71
    #
    # def test_postproc_error(self):
    #     """
    #     Test a postprocessor that outputs data and then exits with error
    #     """
    #     config = arki.dataset.http.load_cfg_sections(self.server_url)
    #     ds = arki.dataset.Reader(config["test200"])

    #     # Querying it should get an error
    #     with self.assertRaises(RuntimeError) as e:
    #         ds.query_bytes(postprocess="error")
    #     self.assertIn("FAIL", str(e.exception))

    def test_postproc_outthenerr(self):
        """
        Test a postprocessor that outputs data and then exits with error
        """
        config = arki.dataset.http.load_cfg_sections(self.server_url)
        with self.env.session.dataset_reader(cfg=config["test200"]) as ds:
            # Querying it should get the partial output and no error
            data = ds.query_bytes(postprocess="outthenerr")
            self.assertEqual(data, b"So far, so good\n")

    def test_postproc_error1(self):
        """
        Test data integrity of postprocessed queries through a server (used to fail
        after offset 0xc00)
        """
        # Get the normal data
        with self.env.session.dataset_reader(cfg=arki.dataset.read_config("testenv/test80")) as ds:
            mdc = ds.query_data(with_data=True)

        with io.BytesIO() as out:
            for md in mdc:
                md.make_inline()
                md.write(out, format="binary")
            plain = out.getvalue()

        # Capture the data after going through the postprocessor
        config = arki.dataset.http.load_cfg_sections(self.server_url)
        with self.env.session.dataset_reader(cfg=config["test80"]) as ds:
            postprocessed = ds.query_bytes("origin:GRIB1,80", postprocess="cat")

        self.assertEqual(len(plain), len(postprocessed))

        md1 = arki.Metadata.read_bundle(plain)[0]
        md2 = arki.Metadata.read_bundle(postprocessed)[0]

        # Remove those metadata that contain test-dependent timestamps
        del md1["assigneddataset"]
        del md2["assigneddataset"]

        self.assertEqual(md1, md2)

        # Compare data
        d1 = md1.data
        d2 = md2.data
        self.assertEqual(d1, d2)

    def test_aliases(self):
        """
        Test downloading the server alias database
        """
        config = arki.dataset.http.get_alias_database(self.server_url)
        self.assertTrue(config.section("origin"))

    def test_postproc_data(self):
        """
        Test uploading postprocessor data
        """
        config = arki.dataset.http.load_cfg_sections(self.server_url)
        with self.env.session.dataset_reader(cfg=config["test200"]) as ds:
            try:
                # Files should be uploaded and notified to the postprocessor script
                os.environ["ARKI_POSTPROC_FILES"] = "inbound/test.grib1:inbound/padded.grib1"
                data = ds.query_bytes(postprocess="checkfiles")

                self.assertEqual(data, b"padded.grib1:test.grib1\n")
            finally:
                del os.environ["ARKI_POSTPROC_FILES"]

    def test_query_global_summary_style_binary(self):
        """
        Test style=binary summary queries
        """
        res = requests.post(self.server_url + "/summary?style=binary", data={
            "query": arki.Matcher().expanded,
        })
        res.raise_for_status()
        self.assertEqual(res.content[:2], b"SU")

    def test_query_summary_style_binary(self):
        """
        Test style=binary summary queries
        """
        res = requests.post(self.server_url + "/dataset/test200/summary?style=binary", data={
            "query": arki.Matcher().expanded,
        })
        res.raise_for_status()
        self.assertEqual(res.content[:2], b"SU")

    def test_query_summary_style_json(self):
        """
        Test style=json summary queries
        """
        res = requests.post(self.server_url + "/dataset/test200/summary?style=json", data={
            "query": arki.Matcher().expanded,
        })
        res.raise_for_status()
        self.assertIn("items", res.json())

    def test_query_summary_style_yaml(self):
        """
        Test style=yaml summary queries
        """
        res = requests.post(self.server_url + "/dataset/test200/summary?style=yaml", data={
            "query": arki.Matcher().expanded,
        })
        self.assertEqual(res.text[:11], "SummaryItem")

    def test_query_summaryshort_style_json(self):
        """
        Test style=json summary short queries
        """
        res = requests.post(self.server_url + "/dataset/test200/summaryshort?style=json", data={
            "query": arki.Matcher().expanded,
        })
        res.raise_for_status()
        self.assertIn("items", res.json())

    def test_query_summaryshort_style_yaml(self):
        """
        Test style=yaml summary queries
        """
        res = requests.post(self.server_url + "/dataset/test200/summaryshort?style=yaml", data={
            "query": arki.Matcher().expanded,
        })
        res.raise_for_status()
        self.assertEqual(res.text[:12], "SummaryStats")

    def test_issue202(self):
        """
        Test querying the datasets, with data, sorted
        """
        config = arki.dataset.http.load_cfg_sections(self.server_url)
        with self.env.session.dataset_reader(cfg=config["test200"]) as reader:
            res = reader.query_bytes("origin:GRIB1,200", sort="day:timerange")
            self.assertEqual(len(res), 7218)
            self.assertTrue(res.startswith(b"GRIB"))
            self.assertTrue(res.endswith(b"7777"))

    def test_progress(self):
        """
        Test querying the datasets, metadata only
        """
        config = arki.dataset.http.load_cfg_sections(self.server_url)
        with self.env.session.dataset_reader(cfg=config["test200"]) as ds:
            progress = Progress()
            mdc = ds.query_data(progress=progress)
            self.assertEqual(len(mdc), 1)
            self.assertEqual(progress.total_count, 1)
            # This is 0 because we do not request data, and we get URL sources that
            # do not carry length information
            self.assertEqual(progress.total_bytes, 0)
            self.assertEqual(progress.start_called, 1)
            self.assertGreaterEqual(progress.update_called, 1)
            self.assertEqual(progress.done_called, 1)

            progress = Progress()
            buf = ds.query_bytes(progress=progress)
            self.assertEqual(len(buf), 7218)
            self.assertEqual(progress.total_count, 0)
            self.assertEqual(progress.total_bytes, 7218)
            self.assertEqual(progress.start_called, 1)
            self.assertGreaterEqual(progress.update_called, 1)
            self.assertEqual(progress.done_called, 1)

            with self.assertRaises(ExpectedFailure):
                ds.query_data(progress=ProgressFailUpdate())
            with self.assertRaises(ExpectedFailure):
                ds.query_bytes(progress=ProgressFailUpdate())

    def test_query_get(self):
        # See #144
        res = requests.get(self.server_url + "/query")
        self.assertEqual(res.status_code, 405)
