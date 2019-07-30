import arkimet as arki
from contextlib import contextmanager
import subprocess
import shutil
import os
import tempfile


def skip_unless_vm2():
    import unittest
    if "vm2" not in arki.features:
        raise unittest.TestSkipped("vm2 support not available")


def skip_unless_libzip():
    import unittest
    if "libzip" not in arki.features:
        raise unittest.TestSkipped("libzip support not available")


def skip_unless_libarchive():
    import unittest
    if "libarchive" not in arki.features:
        raise unittest.TestSkipped("libarchive support not available")


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
    def redirect(self, input=None):
        saved_stdin = os.dup(0)
        saved_stdout = os.dup(1)
        saved_stderr = os.dup(2)
        with tempfile.TemporaryFile("w+b") as tmp_stdin:
            if input is not None:
                tmp_stdin.write(input)
                tmp_stdin.flush()
                tmp_stdin.seek(0)
            with tempfile.TemporaryFile() as tmp_stdout:
                with tempfile.TemporaryFile() as tmp_stderr:
                    try:
                        os.dup2(tmp_stdin.fileno(), 0)
                        os.dup2(tmp_stdout.fileno(), 1)
                        os.dup2(tmp_stderr.fileno(), 2)
                        yield
                    finally:
                        os.dup2(saved_stdin, 0)
                        os.dup2(saved_stdout, 1)
                        os.dup2(saved_stderr, 2)

                        tmp_stdout.seek(0)
                        self.stdout = tmp_stdout.read()
                        tmp_stderr.seek(0)
                        self.stderr = tmp_stderr.read()


class Env:
    def __init__(self, skip_initial_check=False, **kw):
        try:
            shutil.rmtree("testenv")
        except FileNotFoundError:
            pass
        os.mkdir("testenv")
        os.mkdir("testenv/testds")

        kw["name"] = "testds"
        kw["path"] = os.path.abspath("testenv/testds")
        kw.setdefault("step", "daily")
        kw.setdefault("type", "iseg")
        self.ds_cfg = arki.cfg.Section(**kw)

        self.config = self.build_config()

        with open("testenv/config", "wt") as fd:
            self.config.write(fd)

        with open("testenv/testds/config", "wt") as fd:
            self.ds_cfg.write(fd)

        if not skip_initial_check:
            self.check(readonly=False)

    def build_config(self):
        """
        Build the merged dataset config
        """
        config = arki.cfg.Sections()
        config["testds"] = self.ds_cfg
        return config

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
            "format": self.ds_cfg["format"],
            "name": os.path.basename(pathname),
            "path": pathname,
            "type": "file",
        })

        res = source.query_data()
        for md in res:
            dest.acquire(md)

        dest.flush()

        return res

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

    def repack(self, **kw):
        checker = arki.dataset.Checker(self.ds_cfg)
        checker.repack(**kw)

    def check(self, **kw):
        checker = arki.dataset.Checker(self.ds_cfg)
        checker.check(**kw)

    def segment_state(self, **kw):
        checker = arki.dataset.Checker(self.ds_cfg)
        return checker.segment_state(**kw)

    def inspect(self):
        subprocess.run([os.environ.get("SHELL", "bash")], cwd="testenv")
