import unittest
import subprocess
from contextlib import contextmanager
from arkimet.cmdline.mergeconf import Mergeconf
import os
import tempfile


@contextmanager
def daemon(*cmd):
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
    try:
        init = proc.stdout.readline()
        if not init.startswith("OK "):
            raise RuntimeError("Process startup line was {} instead of OK".format(init))
        yield "http://localhost:" + init[3:].strip()
    finally:
        proc.terminate()
        proc.wait()


class CatchOutput:
    def __init__(self):
        self.stdout = None
        self.stderr = None

    @contextmanager
    def redirect(self):
        saved_stdout = os.dup(1)
        saved_stderr = os.dup(2)
        with tempfile.TemporaryFile() as tmp_stdout:
            with tempfile.TemporaryFile() as tmp_stderr:
                try:
                    os.dup2(tmp_stdout.fileno(), 1)
                    os.dup2(tmp_stderr.fileno(), 2)
                    yield
                    tmp_stdout.seek(0)
                    self.stdout = tmp_stdout.read()
                    tmp_stderr.seek(0)
                    self.stderr = tmp_stderr.read()
                finally:
                    os.dup2(saved_stdout, 1)
                    os.dup2(saved_stderr, 2)


class TestArkiMergeconf(unittest.TestCase):
    def test_http(self):
        with daemon(os.path.join(os.environ["TOP_SRCDIR"], "arki/dataset/http-redirect-daemon")) as url:
            out = CatchOutput()
            with out.redirect():
                res = Mergeconf.main(args=[url])
            self.assertIsNone(res)
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
                "type = remote",
                "",
                "[test80]",
                "name = test80",
                "path = http://foo.bar/foo/dataset/test80",
                "type = remote",
            ])

    def test_ignore_system_datasets(self):
        with daemon(os.path.join(os.environ["TOP_SRCDIR"], "arki/dataset/http-redirect-daemon")) as url:
            out = CatchOutput()
            with out.redirect():
                res = Mergeconf.main(args=["--ignore-system-datasets", url])
            self.assertIsNone(res)
            self.assertEqual(out.stderr.decode(), "")
            self.assertEqual(out.stdout.decode().splitlines(), [
                "[test200]",
                "name = test200",
                "path = http://foo.bar/foo/dataset/test200",
                "type = remote",
                "",
                "[test80]",
                "name = test80",
                "path = http://foo.bar/foo/dataset/test80",
                "type = remote",
            ])

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
