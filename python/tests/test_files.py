import io
import tempfile
import unittest
from typing import ClassVar, IO

import arkimet as arki


class TestFiles(unittest.TestCase):
    file: ClassVar[IO[bytes]]

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.file = tempfile.NamedTemporaryFile()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.file.close()
        super().tearDownClass()

    def set_contents(self, contents: bytes) -> None:
        self.file.seek(0)
        self.file.truncate()
        self.file.write(contents)
        self.file.seek(0)

    def test_fileno(self) -> None:
        f = arki.File(self.file.fileno(), self.file.name)
        self.assertEqual(f.fileno(), self.file.fileno())

    def test_name(self) -> None:
        f = arki.File(self.file.fileno(), self.file.name)
        self.assertEqual(f.name, self.file.name)

    def test_read_some(self) -> None:
        self.set_contents(b"1234567890")
        f = arki.File(self.file.fileno(), self.file.name)
        self.assertEqual(f.read(4), b"1234")
        self.assertEqual(f.read(3), b"567")
        self.assertEqual(f.read(), b"890")

    def test_read_all(self) -> None:
        with io.BytesIO() as buf:
            buf.write(b"0" * (4096 * 16))
            buf.write(b"1" * (4096 * 16))
            buf.write(b"2" * (4096 * 16))
            buf.write(b"3" * (4096 * 8))
            sample = buf.getvalue()

        self.set_contents(sample)
        f = arki.File(self.file.fileno(), self.file.name)
        self.assertEqual(f.read(), sample)

    def test_read_negative(self) -> None:
        with io.BytesIO() as buf:
            buf.write(b"0" * (4096 * 16))
            buf.write(b"1" * (4096 * 16))
            buf.write(b"2" * (4096 * 16))
            buf.write(b"3" * (4096 * 8))
            sample = buf.getvalue()

        self.set_contents(sample)

        for val in (-1, -10, -1000):
            with self.subTest(val=val):
                self.file.seek(0)
                f = arki.File(self.file.fileno(), self.file.name)
                self.assertEqual(f.read(val), sample)
