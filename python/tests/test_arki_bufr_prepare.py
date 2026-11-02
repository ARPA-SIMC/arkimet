import datetime as dt
import io
import tempfile
import unittest
import shutil
import subprocess
from typing import ClassVar, Any

import wreport


class TestArkiBufrPrepare(unittest.TestCase):
    command: ClassVar[str]

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        command = shutil.which("arki-bufr-prepare")
        assert command is not None
        cls.command = command

    def invoke(self, *args: str, **kwargs: Any) -> subprocess.CompletedProcess:
        kwargs.setdefault("stdout", subprocess.PIPE)
        kwargs.setdefault("stderr", subprocess.PIPE)
        return subprocess.run([self.command, *args], **kwargs)

    def test_empty(self):
        res = self.invoke(input="")
        self.assertEqual(res.stdout, b"")
        print(res.stderr.decode())
        self.assertEqual(res.stderr, b"")

    def test_split(self):
        res = self.invoke("inbound/multisubset.bufr")
        self.assertEqual(res.stderr, b"")
        with io.BytesIO(res.stdout) as fd:
            output = list(wreport.read_bufr_headers(fd))

        self.assertEqual(len(output), 14)
        self.assertEqual(output[0].reftime, dt.datetime(2022, 2, 14, 9, 0, 3))
        self.assertEqual(output[1].reftime, dt.datetime(2022, 2, 14, 9, 0, 3))
        self.assertEqual(output[13].reftime, dt.datetime(2022, 2, 14, 9, 0, 3))

    def test_output_file(self):
        with tempfile.NamedTemporaryFile("w+b") as tf:
            res = self.invoke("inbound/multisubset.bufr", "-o", tf.name)
            self.assertEqual(res.stderr, b"")
            tf.seek(0)
            output = list(wreport.read_bufr_headers(tf))

        self.assertEqual(len(output), 14)
        self.assertEqual(output[0].reftime, dt.datetime(2022, 2, 14, 9, 0, 3))
        self.assertEqual(output[1].reftime, dt.datetime(2022, 2, 14, 9, 0, 3))
        self.assertEqual(output[13].reftime, dt.datetime(2022, 2, 14, 9, 0, 3))
