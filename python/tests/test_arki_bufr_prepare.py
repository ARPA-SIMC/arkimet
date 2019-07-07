import unittest
from arkimet.cmdline.bufr_prepare import BufrPrepare
from arkimet.test import CatchOutput


class TestArkiBufrPrepare(unittest.TestCase):
    def test_empty(self):
        out = CatchOutput()
        with out.redirect():
            res = BufrPrepare.main(args=[])
        self.assertEqual(out.stderr, b"")
        self.assertEqual(out.stdout, b"")
        self.assertIsNone(res)

    def test_passthrough(self):
        with open("inbound/test.bufr", "rb") as fd:
            data = fd.read()

        out = CatchOutput()
        with out.redirect(input=data):
            res = BufrPrepare.main(args=[])
        self.assertEqual(out.stderr, b"")
        self.assertEqual(out.stdout, data)
        self.assertIsNone(res)
