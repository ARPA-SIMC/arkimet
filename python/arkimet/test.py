from contextlib import contextmanager
import subprocess
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
    def redirect(self, input=None):
        saved_stdin = None
        saved_stdin = os.dup(0)
        saved_stdout = os.dup(1)
        saved_stderr = os.dup(2)
        with tempfile.TemporaryFile() as tmp_stdin:
            if input:
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
                        tmp_stdout.seek(0)
                        self.stdout = tmp_stdout.read()
                        tmp_stderr.seek(0)
                        self.stderr = tmp_stderr.read()
                    finally:
                        os.dup2(saved_stdin, 0)
                        os.dup2(saved_stdout, 1)
                        os.dup2(saved_stderr, 2)
