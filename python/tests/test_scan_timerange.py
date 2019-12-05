import unittest
from arkimet.scan.timedef import (
        UNIT_SECOND, UNIT_MINUTE, UNIT_HOUR, UNIT_12HOURS, UNIT_DAY,
        UNIT_MONTH, UNIT_YEAR, UNIT_DECADE, UNIT_NORMAL, UNIT_CENTURY,
        UNIT_MISSING,
        restrict_unit, enlarge_unit, compare_units, make_same_units)


class TestScanTimerange(unittest.TestCase):
    def test_restrict_unit(self):
        self.assertEqual(restrict_unit(60, UNIT_MINUTE), (3600, UNIT_SECOND))
        self.assertEqual(restrict_unit(1, UNIT_SECOND), (1, UNIT_SECOND))
        self.assertEqual(restrict_unit(10, UNIT_YEAR), (120, UNIT_MONTH))
        self.assertEqual(restrict_unit(1, UNIT_MONTH), (1, UNIT_MONTH))
        self.assertEqual(restrict_unit(1, UNIT_MISSING), (1, UNIT_MISSING))

    def test_enlarge_unit(self):
        self.assertEqual(enlarge_unit(600, UNIT_SECOND), (10, UNIT_MINUTE))
        self.assertEqual(enlarge_unit(601, UNIT_SECOND), (601, UNIT_SECOND))
        self.assertEqual(enlarge_unit(1, UNIT_DAY), (1, UNIT_DAY))
        self.assertEqual(enlarge_unit(601, UNIT_MONTH), (601, UNIT_MONTH))
        self.assertEqual(enlarge_unit(1, UNIT_NORMAL), (1, UNIT_NORMAL))
        self.assertEqual(enlarge_unit(1, UNIT_CENTURY), (1, UNIT_CENTURY))
        self.assertEqual(enlarge_unit(1, UNIT_MISSING), (1, UNIT_MISSING))

    def test_compare_units(self):
        self.assertEqual(compare_units(UNIT_SECOND, UNIT_HOUR), -1)
        self.assertEqual(compare_units(UNIT_HOUR, UNIT_SECOND), 1)
        self.assertEqual(compare_units(UNIT_HOUR, UNIT_HOUR), 0)

    def assert_make_same_unit_equals(self, val1, unit1, val2, unit2,
                                     eval1, eunit1, eval2, eunit2):
        tr = {
            "step_len": val1,
            "step_unit": unit1,
            "stat_len": val2,
            "stat_unit": unit2,
        }
        make_same_units(tr)
        val1 = tr["step_len"]
        unit1 = tr["step_unit"]
        val2 = tr["stat_len"]
        unit2 = tr["stat_unit"]

        self.assertEqual(
                (val1, unit1, val2, unit2),
                (eval1, eunit1, eval2, eunit2))

    def test_make_same_units(self):
        self.assert_make_same_unit_equals(
                60, UNIT_MINUTE, 1, UNIT_DAY,
                1, UNIT_HOUR, 24, UNIT_HOUR)

        self.assert_make_same_unit_equals(
                60, UNIT_12HOURS, 1, UNIT_SECOND,
                60*12*3600, UNIT_SECOND, 1, UNIT_SECOND)

        self.assert_make_same_unit_equals(
                60, UNIT_SECOND, 1, UNIT_SECOND,
                60, UNIT_SECOND, 1, UNIT_SECOND)

        self.assert_make_same_unit_equals(
                60, UNIT_MONTH, 1, UNIT_DECADE,
                5, UNIT_YEAR, 10, UNIT_YEAR)

        self.assert_make_same_unit_equals(
                1, UNIT_NORMAL, 1, UNIT_CENTURY,
                3, UNIT_DECADE, 10, UNIT_DECADE)

        with self.assertRaises(ValueError) as e:
            self.assert_make_same_unit_equals(
                1, UNIT_SECOND, 1, UNIT_MONTH,
                None, None, None, None)
        self.assertEqual(str(e.exception), "cannot compare two different kinds of time units (s and mo)")
