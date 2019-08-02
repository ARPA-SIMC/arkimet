import unittest
import multiprocessing
import os
import arkimet as arki
import arkimet.test
from arkimet.cmdline.server import make_server


class ServerThread(multiprocessing.Process):
    def __init__(self, server):
        super().__init__()
        self.server = server
        self.server_exception = None

    def run(self):
        try:
            self.server.serve_forever()
        except Exception as e:
            self.server_exception = e
            self.server.shutdown()


class Env(arkimet.test.Env):
    def build_config(self):
        config = super().build_config()

        ds = arki.dataset.Reader("inbound/fixture.grib1")
        mds = ds.query_data()

        for idx, name in enumerate(("test200", "test80")):
            cfg = arki.cfg.Section({
                "name": name,
                "path": os.path.abspath(os.path.join("testenv", name)),
                "type": "iseg",
                "format": "grib",
                "step": "daily",
            })
            config[name] = cfg

            with open(os.path.join("testenv", "testds", name), "wt") as fd:
                cfg.write(fd)

            checker = arki.dataset.Checker(cfg)
            checker.check(readonly=False)

            writer = arki.dataset.Writer(cfg)
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
        self.env = Env(format="grib")
        # TODO: randomly allocate a port
        self.server_url = "http://localhost:7117"
        self.server = make_server("localhost", 7117, self.env.config, self.server_url)
        self.server_thread = ServerThread(self.server)
        self.server_thread.start()

    def tearDown(self):
        self.server_thread.terminate()
        self.server_thread.join()
        if self.server_thread.server_exception is not None:
            raise self.server_thread.server_exception

    def test_config(self):
        """
        Query the configuration
        """
        config = arki.dataset.http.load_cfg_sections(self.server_url)

        self.assertIn("test200", config)
        self.assertEqual(config["test200"]["server"], "http://localhost:7117")

        self.assertIn("test80", config)
        self.assertEqual(config["test80"]["server"], "http://localhost:7117")

        self.assertIn("error", config)
        self.assertEqual(config["error"]["server"], "http://localhost:7117")

    def test_metadata(self):
        """
        Test querying the datasets, metadata only
        """
        config = arki.dataset.http.load_cfg_sections(self.server_url)

        ds = arki.dataset.Reader(config["test200"])
        mdc = ds.query_data("origin:GRIB1,200")
        self.assertEqual(len(mdc), 1)

        # Check that the source record that comes out is ok
        self.assertEqual(mdc[0].to_python("source"), {
            't': 'source', 's': 'URL', 'f': 'grib',
            'url': 'http://localhost:7117/dataset/test200',
        })

        mdc = ds.query_data("origin:GRIB1,80")
        self.assertFalse(mdc)

        mdc = ds.query_data("origin:GRIB1,98")
        self.assertFalse(mdc)
