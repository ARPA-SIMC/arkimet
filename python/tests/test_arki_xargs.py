import unittest
import os
import tempfile
from contextlib import contextmanager
from arkimet.cmdline.xargs import Xargs
from arkimet.test import CatchOutput


@contextmanager
def script(content):
    with open("testscript.sh", "wt") as fd:
        fd.write(content)
        fd.flush()
        os.fchmod(fd.fileno(), 0o755)
        fd.close()
        yield os.path.abspath(fd.name)
        os.unlink(fd.name)


class TestArkiBufrPrepare(unittest.TestCase):
    def test_default_size(self):
        with script("""#!/bin/sh -e
test "$ARKI_XARGS_FORMAT" = GRIB
test "$ARKI_XARGS_COUNT" -eq 1
test -n "$ARKI_XARGS_TIME_START"
test -n "$ARKI_XARGS_TIME_END"
test -r "$1"
test -s "$1"
""") as s:
            out = CatchOutput()
            with out.redirect():
                res = Xargs.main(args=["-i", "inbound/test.grib1.arkimet.inline", "-n", "1", s])

            self.assertEqual(out.stderr, b"")
            self.assertEqual(out.stdout, b"")
            self.assertIsNone(res)

    def test_large_size(self):
        with script("""#!/bin/sh -e
test "$ARKI_XARGS_FORMAT" = GRIB
test "$ARKI_XARGS_COUNT" -ge 1
test -n "$ARKI_XARGS_TIME_START"
test -n "$ARKI_XARGS_TIME_END"
test -r "$1"
test -s "$1"
FILESIZE=$(stat -c%s "$1")
test "$FILESIZE" -le 100000
""") as s:
            out = CatchOutput()
            with out.redirect():
                res = Xargs.main(args=["-i", "inbound/test.grib1.arkimet.inline", "-s", "100000", s])

            self.assertEqual(out.stderr, b"")
            self.assertEqual(out.stdout, b"")
            self.assertIsNone(res)
