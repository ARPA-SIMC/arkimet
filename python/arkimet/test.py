import arkimet as arki
from contextlib import contextmanager
import multiprocessing
import subprocess
import shutil
import os
import io
import sys
import tempfile
import logging


def skip_unless_vm2():
    import unittest
    if "vm2" not in arki.features:
        raise unittest.SkipTest("VM2 support not available")


def skip_unless_libzip():
    import unittest
    if "libzip" not in arki.features:
        raise unittest.SkipTest("libzip support not available")


def skip_unless_libarchive():
    import unittest
    if "libarchive" not in arki.features:
        raise unittest.SkipTest("libarchive support not available")


def skip_unless_geos():
    import unittest
    if "geos" not in arki.features:
        raise unittest.SkipTest("GEOS support not available")


def skip_unless_arpae_tests():
    import unittest
    if "arpae_tests" not in arki.features:
        raise unittest.SkipTest("ARPAE tests disabled")


@contextmanager
def daemon(*cmd):
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
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


class CmdlineTestMixin:
    # Class of the Cmdline subclass to be tested
    command = None

    def runcmd(self, *args):
        try:
            res = self.command.main(args)
        except SystemExit as e:
            res = e.args[0]
        if res == 0:
            return None
        return res

    def call_output(self, *args, binary=False, input=None):
        orig_stdin = sys.stdin
        orig_stdout = sys.stdout
        orig_stderr = sys.stderr
        if input is not None:
            if isinstance(input, bytes):
                sys.stdin = io.BytesIO(input)
            elif isinstance(input, str):
                sys.stdin = io.StringIO(input)
            else:
                sys.stdin = input
        if binary:
            sys.stdout = io.BytesIO()
        else:
            sys.stdout = io.StringIO()
        sys.stderr = io.StringIO()
        try:
            res = self.runcmd(*args)
            out = sys.stdout.getvalue()
            err = sys.stderr.getvalue()
        finally:
            sys.stdin = orig_stdin
            sys.stdout = orig_stdout
            sys.stderr = orig_stderr
        return out, err, res

    def call_output_success(self, *args, binary=False, returncode=None, input=None):
        out, err, res = self.call_output(*args, binary=binary, input=input)
        self.assertEqual(err, "")
        if returncode is None:
            self.assertIsNone(res)
        else:
            self.assertEqual(res, returncode)
        return out


# Adapted from testfixtures.logcapture, which cannot be used in Fedora
class LogCapture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.old_levels = None
        self.old_handlers = None
        self.old_disabled = None
        self.old_propagate = None
        self.clear()

    def clear(self):
        "Clear any entries that have been captured."
        self.records = []

    def emit(self, record):
        self.records.append(record)

    def install(self):
        """
        Install this :class:`LogHandler` into the Python logging
        framework for the named loggers.

        This will remove any existing handlers for those loggers and
        drop their level to that specified on this :class:`LogCapture` in order
        to capture all logging.
        """
        logger = logging.getLogger(None)
        self.old_levels = logger.level
        self.old_handlers = logger.handlers
        self.old_disabled = logger.disabled
        self.old_progagate = logger.propagate
        logger.setLevel(1)
        logger.handlers = [self]
        logger.disabled = False

    def uninstall(self):
        """
        Un-install this :class:`LogHandler` from the Python logging
        framework for the named loggers.

        This will re-instate any existing handlers for those loggers
        that were removed during installation and retore their level
        that prior to installation.
        """
        logger = logging.getLogger(None)
        logger.setLevel(self.old_levels)
        logger.handlers = self.old_handlers
        logger.disabled = self.old_disabled
        logger.propagate = self.old_progagate

    def __enter__(self):
        self.install()
        return self.records

    def __exit__(self, type, value, traceback):
        self.uninstall()


class ServerProcess(multiprocessing.Process):
    def __init__(self, server):
        super().__init__()
        self.server = server
        self.server_exception = None

    def run(self):
        with arki.test.LogCapture():
            try:
                self.server.serve_forever()
            except Exception as e:
                self.server_exception = e
                self.server.shutdown()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        self.terminate()
        self.join()
        # FIXME: this is never found, given that we're using a multiprocessing.Process
        if self.server_exception is not None:
            raise self.server_exception
