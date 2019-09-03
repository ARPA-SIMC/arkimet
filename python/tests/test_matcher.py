import unittest
import glob
import re
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

    def test_docs(self):
        """
        Run the doc/matcher/*.txt as doctests
        """
        re_given = re.compile(r"^Given (\w+ `[^`]+`(?:\s*,\s*\w+ `[^`]+`)*)")
        re_given_item = re.compile(r"(\w+) `([^`]+)`")
        re_matches = re.compile(r"^\* `(.+)` matches\b")
        re_not_matches = re.compile(r"^\* `(.+)` does not match\b")
        re_expected = re.compile(r"^\[//\]: # \(matched: (\d+), not matched: (\d+)\)\s*$")

        for pathname in glob.glob("matcher/*.md"):
            md = None
            expected_matches = 0
            expected_not_matches = 0
            count_matches = 0
            count_not_matches = 0
            with open(pathname, "rt") as fd:
                for lineno, line in enumerate(fd):
                    mo = re_given.match(line)
                    if mo:
                        md = arki.Metadata()
                        for m in re_given_item.finditer(mo.group(1)):
                            try:
                                md[m.group(1)] = m.group(2)
                            except Exception as e:
                                self.fail("{}:{}: cannot parse metadata in line {}: {}".format(
                                    pathname, lineno, line.rstrip(), e))
                        continue

                    mo = re_matches.match(line)
                    if mo:
                        if md is None:
                            self.fail("{}:{}: `matches` line found before Given line".format(
                                pathname, lineno))

                        matcher = arki.Matcher(mo.group(1))
                        self.assertTrue(matcher.match(md))
                        count_matches += 1
                        continue

                    mo = re_not_matches.match(line)
                    if mo:
                        if md is None:
                            self.fail("{}:{}: `does not match` line found before Given line".format(
                                pathname, lineno))

                        matcher = arki.Matcher(mo.group(1))
                        self.assertFalse(matcher.match(md))
                        count_not_matches += 1
                        continue

                    mo = re_expected.match(line)
                    if mo:
                        expected_matches = int(mo.group(1))
                        expected_not_matches = int(mo.group(2))
                        continue

                if (expected_matches != count_matches
                   or expected_not_matches != count_not_matches):
                    self.fail("{}: test count expectations failed: matches {}/{}, not matches {}/{}".format(
                        pathname, count_matches, expected_matches, count_not_matches, expected_not_matches))
