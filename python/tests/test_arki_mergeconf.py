import unittest
from arkimet.cmdline.mergeconf import Mergeconf
import os
import tempfile
from arkimet.test import daemon, CatchOutput


class TestArkiMergeconf(unittest.TestCase):
    def test_http(self):
        with daemon(os.path.join(os.environ["TOP_SRCDIR"], "arki/dataset/http-redirect-daemon")) as url:
            out = CatchOutput()
            with out.redirect():
                res = Mergeconf.main(args=[url])
            self.assertEqual(out.stderr.decode(), "")
            self.assertEqual(out.stdout.decode().splitlines(), [
                "[error]",
                "name = error",
                "path = http://foo.bar/foo/dataset/error",
                "type = remote",
                "",
                "[test200]",
                "name = test200",
                "path = http://foo.bar/foo/dataset/test200",
                "restrict = test",
                "type = remote",
                "",
                "[test80]",
                "name = test80",
                "path = http://foo.bar/foo/dataset/test80",
                "type = remote",
            ])
            self.assertIsNone(res)

    def test_ignore_system_datasets(self):
        with daemon(os.path.join(os.environ["TOP_SRCDIR"], "arki/dataset/http-redirect-daemon")) as url:
            out = CatchOutput()
            with out.redirect():
                res = Mergeconf.main(args=["--ignore-system-datasets", url])
            self.assertEqual(out.stderr.decode(), "")
            self.assertEqual(out.stdout.decode().splitlines(), [
                "[test200]",
                "name = test200",
                "path = http://foo.bar/foo/dataset/test200",
                "restrict = test",
                "type = remote",
                "",
                "[test80]",
                "name = test80",
                "path = http://foo.bar/foo/dataset/test80",
                "type = remote",
            ])
            self.assertIsNone(res)

    def test_restrict(self):
        with daemon(os.path.join(os.environ["TOP_SRCDIR"], "arki/dataset/http-redirect-daemon")) as url:
            out = CatchOutput()
            with out.redirect():
                res = Mergeconf.main(args=["--restrict=test,test1", url])
            self.assertEqual(out.stderr.decode(), "")
            self.assertEqual(out.stdout.decode().splitlines(), [
                "[test200]",
                "name = test200",
                "path = http://foo.bar/foo/dataset/test200",
                "restrict = test",
                "type = remote",
            ])
            self.assertIsNone(res)

    def test_validation(self):
        with tempfile.NamedTemporaryFile("w+t") as tf:
            tf.write("""
[name]
filter=invalid
""")
            tf.flush()

            out = CatchOutput()
            with out.redirect():
                res = Mergeconf.main(args=["-C", tf.name])
            self.assertEqual(out.stderr.decode().splitlines(), [
                "name: cannot parse matcher subexpression 'invalid' does not contain a colon (':')",
                "Some input files did not validate.",
            ])
            self.assertEqual(out.stdout.decode().splitlines(), [])
            self.assertEqual(res, 1)

    def test_load_configs(self):
        with tempfile.NamedTemporaryFile("wt") as conf1:
            with tempfile.NamedTemporaryFile("wt") as conf2:
                conf1.write("[ds1]\npath=/tmp/ds1\n")
                conf1.flush()
                conf2.write("[ds2]\npath=/tmp/ds2\n")
                conf2.flush()

                out = CatchOutput()
                with out.redirect():
                    res = Mergeconf.main(args=["--config=" + conf1.name, "--config=" + conf2.name])
                self.assertIsNone(res)
                self.assertEqual(out.stderr.decode(), "")
                self.assertEqual(out.stdout.decode().splitlines(), [
                    "[ds1]",
                    "name = ds1",
                    "path = /tmp/ds1",
                    "",
                    "[ds2]",
                    "name = ds2",
                    "path = /tmp/ds2",
                ])

    def test_extra(self):
        src = os.path.abspath("inbound/test.grib1")
        out = CatchOutput()
        with out.redirect():
            res = Mergeconf.main(args=[src, "--extra"])
        self.assertIsNone(res)
        self.assertEqual(out.stderr.decode(), "")
        self.assertEqual(out.stdout.decode().splitlines(), [
            "[" + src + "]",
            'bounding = POLYGON ((-60.0000000000000000 30.0000000000000000, '
            '-60.0000000000000000 72.0000000000000000, 42.0000000000000000 '
            '72.0000000000000000, 42.0000000000000000 30.0000000000000000, '
            '-60.0000000000000000 30.0000000000000000))',
            'format = grib',
            'name = ' + src,
            'path = ' + src,
            'type = file',
        ])
