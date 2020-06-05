import unittest
import arkimet.cmdline
import arkimet.cmdline.base


class TestRestrict(unittest.TestCase):
    def _restrict(self, restrict_pattern, restrict_value):
        from arkimet.cmdline.base import RestrictSectionFilter
        f = RestrictSectionFilter(restrict_pattern)
        config = arkimet.cfg.Section()
        config["restrict"] = restrict_value
        return f.is_allowed(config)

    def assertRestrictPasses(self, restrict, filter):
        if not self._restrict(filter, restrict):
            self.fail("restrict value of {} was not accepted by filter {}".format(restrict, filter))

    def assertRestrictNotPasses(self, restrict, filter):
        if self._restrict(filter, restrict):
            self.fail("restrict value of {} was unexpectedly accepted by filter {}".format(restrict, filter))

    def test_restrict(self):
        self.assertRestrictPasses("", "")
        self.assertRestrictPasses("a,b,c", "")
        self.assertRestrictPasses("c, d, e, f", "")

        self.assertRestrictNotPasses("", "a, b, c")
        self.assertRestrictPasses("a", "a, b, c")
        self.assertRestrictPasses("a, b", "a, b, c")
        self.assertRestrictPasses("c, d, e, f", "a, b, c")
        self.assertRestrictNotPasses("d, e, f", "a, b, c")


class TestParseSize(unittest.TestCase):
    def test_parse(self):
        parse = arkimet.cmdline.base.App.parse_size
        self.assertEqual(parse("0"), 0)
        self.assertEqual(parse("1"), 1)
        self.assertEqual(parse("1b"), 1)
        self.assertEqual(parse("1B"), 1)
        self.assertEqual(parse("2k"), 2000)
        self.assertEqual(parse("2Ki"), 2048)
        self.assertEqual(parse("2Ki"), 2048)
        self.assertEqual(parse("10M"), 10000000)
        self.assertEqual(parse("10Mi"), 10 * 1024 * 1024)
        with self.assertRaises(ValueError):
            parse("10v")
        with self.assertRaises(ValueError):
            parse("10bb")
        with self.assertRaises(ValueError):
            parse("10cb")
        with self.assertRaises(ValueError):
            parse("10bc")
        with self.assertRaises(ValueError):
            parse("10kib")
        with self.assertRaises(ValueError):
            parse("foo")

        # Compatibility with old parser
        self.assertEqual(parse("1c"), 1)
        self.assertEqual(parse("1kB"), 1000)
        self.assertEqual(parse("1K"), 1000)
        self.assertEqual(parse("1MB"), 1000 * 1000)
        self.assertEqual(parse("1M"), 1000 * 1000)
        self.assertEqual(parse("1GB"), 1000 * 1000 * 1000)
        self.assertEqual(parse("1G"), 1000 * 1000 * 1000)
        self.assertEqual(parse("1TB"), 1000 * 1000 * 1000 * 1000)
        self.assertEqual(parse("1T"), 1000 * 1000 * 1000 * 1000)
        self.assertEqual(parse("1PB"), 1000 * 1000 * 1000 * 1000 * 1000)
        self.assertEqual(parse("1P"), 1000 * 1000 * 1000 * 1000 * 1000)
        self.assertEqual(parse("1EB"), 1000 * 1000 * 1000 * 1000 * 1000 * 1000)
        self.assertEqual(parse("1E"), 1000 * 1000 * 1000 * 1000 * 1000 * 1000)
        self.assertEqual(parse("1ZB"), 1000 * 1000 * 1000 * 1000 * 1000 * 1000 * 1000)
        self.assertEqual(parse("1Z"), 1000 * 1000 * 1000 * 1000 * 1000 * 1000 * 1000)
        self.assertEqual(parse("1YB"), 1000 * 1000 * 1000 * 1000 * 1000 * 1000 * 1000 * 1000)
        self.assertEqual(parse("1Y"), 1000 * 1000 * 1000 * 1000 * 1000 * 1000 * 1000 * 1000)
