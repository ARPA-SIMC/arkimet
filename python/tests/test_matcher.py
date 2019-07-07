import unittest
import arkimet as arki


class TestMatcher(unittest.TestCase):
    def test_empty(self):
        matcher = arki.Matcher()
        self.assertEqual(str(matcher), "")

    def test_parse(self):
        matcher = arki.Matcher("reftime:=today")
        self.assertEqual(str(matcher), "reftime:=today")

    def test_parseerror(self):
        with self.assertRaises(ValueError):
            arki.Matcher("invalid")
