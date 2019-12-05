import unittest
import tempfile
from arkimet.cmdline.bufr_prepare import BufrPrepare
from arkimet.test import CmdlineTestMixin

# Bufr support uses dballe, which uses libc's FILE* I/O, which doesn't
# cooperate well with being called multiple times with multiple
# redirect/freopen functions, which we currently cannot access from python.
# Therefore, we skip stdin/stdout tests here.


class TestArkiBufrPrepare(CmdlineTestMixin, unittest.TestCase):
    command = BufrPrepare

    def test_empty(self):
        out = self.call_output_success("/dev/null")
        self.assertEqual(out, "")

#     def test_passthrough(self):
#         with open("inbound/test.bufr", "rb") as fd:
#             data = fd.read()
#
#         out = CatchOutput()
#         with out.redirect(input=data):
#             res = BufrPrepare.main(args=[])
#         self.assertEqual(out.stderr, b"")
#         self.assertEqual(out.stdout, data)
#         self.assertIsNone(res)

    def test_files(self):
        with tempfile.NamedTemporaryFile("w+b") as tf:
            out = self.call_output_success("-o", tf.name, "inbound/test.bufr")
            tf.seek(0)
            dest = tf.read()
        self.assertEqual(out, "")

        self.assertEqual(len(dest), 634)
        with open("inbound/test.bufr", "rb") as fd:
            src = fd.read()

        self.assertEqual(src, dest)
