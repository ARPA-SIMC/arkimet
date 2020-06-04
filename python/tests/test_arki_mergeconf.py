import unittest
from arkimet.cmdline.mergeconf import Mergeconf
import os
import tempfile
from arkimet.test import daemon, CmdlineTestMixin


class TestArkiMergeconf(CmdlineTestMixin, unittest.TestCase):
    command = Mergeconf

    def test_http(self):
        with daemon(
                os.path.join(os.environ["TOP_SRCDIR"], "arki/dataset/http-test-daemon"), "--action=redirect") as url:
            out = self.call_output_success(url)
            self.assertEqual(out.splitlines(), [
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

    def test_ignore_system_datasets(self):
        with daemon(
                os.path.join(os.environ["TOP_SRCDIR"], "arki/dataset/http-test-daemon"), "--action=redirect") as url:
            out = self.call_output_success("--ignore-system-datasets", url)
            self.assertEqual(out.splitlines(), [
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

    def test_restrict(self):
        with daemon(
                os.path.join(os.environ["TOP_SRCDIR"], "arki/dataset/http-test-daemon"), "--action=redirect") as url:
            out = self.call_output_success("--restrict=test,test1", url)
            self.assertEqual(out.splitlines(), [
                "[test200]",
                "name = test200",
                "path = http://foo.bar/foo/dataset/test200",
                "restrict = test",
                "type = remote",
            ])

    def test_validation(self):
        with tempfile.NamedTemporaryFile("w+t") as tf:
            tf.write("""
[name]
filter=invalid
type=discard
""")
            tf.flush()

            out, err, res = self.call_output("-C", tf.name)
            self.assertEqual(err.splitlines(), [
                "name: cannot parse matcher subexpression 'invalid' does not contain a colon (':')",
                "Some input files did not validate.",
            ])
            self.assertEqual(out.splitlines(), [])
            self.assertEqual(res, 1)

    def test_load_configs(self):
        with tempfile.NamedTemporaryFile("wt") as conf1:
            with tempfile.NamedTemporaryFile("wt") as conf2:
                conf1.write("[ds1]\npath=/tmp/ds1\ntype=discard\n[common]\npath=/tmp/common1\ntype=discard\n")
                conf1.flush()
                conf2.write("[ds2]\npath=/tmp/ds2\ntype=discard\n[common]\npath=/tmp/common2\ntype=discard\n")
                conf2.flush()

                with self.assertLogs() as log:
                    out = self.call_output_success("--config=" + conf1.name, "--config=" + conf2.name)
                self.assertEqual(out.splitlines(), [
                    "[common]",
                    "name = common",
                    "path = /tmp/common1",
                    "type = discard",
                    "",
                    "[ds1]",
                    "name = ds1",
                    "path = /tmp/ds1",
                    "type = discard",
                    "",
                    "[ds2]",
                    "name = ds2",
                    "path = /tmp/ds2",
                    "type = discard",
                ])

                self.assertEqual(log.output, [
                    'WARNING:arkimet:dataset "common" in "/tmp/common2" already loaded from '
                    '"/tmp/common1": keeping only the first one',
                ])

    def test_extra(self):
        src = os.path.abspath("inbound/test.grib1")
        out = self.call_output_success(src, "--extra")
        self.assertEqual(out.splitlines(), [
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
