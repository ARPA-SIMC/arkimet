import unittest
import arkimet as arki


class RestrictTester(arki.cmdline.base.AppConfigMixin):
    def __init__(self, restrict):
        super().__init__()
        self.config.obtain("testds")["restrict"] = restrict


class TestRestrict(unittest.TestCase):
    def assertRestrictPasses(self, restrict, filter):
        tester = RestrictTester(restrict)
        tester.filter_restrict(filter)
        if not tester.config:
            self.fail("restrict value of {} was not accepted by filter {}".format(restrict, filter))

    def assertRestrictNotPasses(self, restrict, filter):
        tester = RestrictTester(restrict)
        tester.filter_restrict(filter)
        if tester.config:
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
