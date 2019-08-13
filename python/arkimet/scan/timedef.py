from typing import Tuple, Dict, Any

UNIT_MINUTE = 0
UNIT_HOUR = 1
UNIT_DAY = 2
UNIT_MONTH = 3
UNIT_YEAR = 4
UNIT_DECADE = 5
UNIT_NORMAL = 6
UNIT_CENTURY = 7
UNIT_3HOURS = 10
UNIT_6HOURS = 11
UNIT_12HOURS = 12
UNIT_SECOND = 13
UNIT_MISSING = 255

UNIT_SUFFIXES = {
    UNIT_MINUTE: "m",
    UNIT_HOUR: "h",
    UNIT_DAY: "d",
    UNIT_MONTH: "mo",
    UNIT_YEAR: "y",
    UNIT_DECADE: "de",
    UNIT_NORMAL: "no",
    UNIT_CENTURY: "ce",
    UNIT_3HOURS: "h3",
    UNIT_6HOURS: "h6",
    UNIT_12HOURS: "h12",
    UNIT_SECOND: "s",
}


def timeunit_suffix(unit: int) -> str:
    suffix = UNIT_SUFFIXES.get(unit)
    if suffix is not None:
        return suffix
    if unit == UNIT_MISSING:
        raise ValueError("cannot find suffix for MISSING (255) time unit")
    else:
        raise ValueError("cannot find find time unit suffix: time unit is unknown ({})".format(unit))


def unit_can_be_seconds(unit: int) -> bool:
    """
    Return true if the unit can be represented in seconds, false if the unit can
    be represented in months.
    """
    if unit in (UNIT_SECOND, UNIT_MINUTE, UNIT_HOUR, UNIT_3HOURS, UNIT_6HOURS, UNIT_12HOURS, UNIT_DAY):
        return True
    if unit in (UNIT_MONTH, UNIT_YEAR, UNIT_DECADE, UNIT_NORMAL, UNIT_CENTURY, UNIT_MISSING):
        return False
    raise ValueError("invalid timedef unit {}".format(unit))


def compare_units(unit1: int, unit2: int) -> int:
    """
    Return -1 if unit1 represent a smaller time lapse than unit2, 0 if they are
    the same, 1 if unit1 represents a bigger time lapse than unit1.

    Raises an exception if the two units cannot be compared (like seconds and
    months)
    """
    if unit1 == unit2:
        return 0

    if unit_can_be_seconds(unit1):
        if not unit_can_be_seconds(unit2):
            raise ValueError("cannot compare two different kinds of time units ({} and {})".format(
                timeunit_suffix(unit1), timeunit_suffix(unit2)))

        if unit1 == UNIT_SECOND:
            return -1
        elif unit1 == UNIT_MINUTE:
            if unit2 == UNIT_SECOND:
                return 1
            else:
                return -1
        elif unit1 == UNIT_HOUR:
            if unit2 in (UNIT_MINUTE, UNIT_SECOND):
                return 1
            else:
                return -1
        elif unit1 == UNIT_3HOURS:
            if unit2 in (UNIT_HOUR, UNIT_MINUTE, UNIT_SECOND):
                return 1
            else:
                return -1
        elif unit1 == UNIT_6HOURS:
            if unit2 in (UNIT_3HOURS, UNIT_HOUR, UNIT_MINUTE, UNIT_SECOND):
                return 1
            else:
                return -1
        elif unit1 == UNIT_12HOURS:
            if unit2 in (UNIT_6HOURS, UNIT_3HOURS, UNIT_HOUR, UNIT_MINUTE, UNIT_SECOND):
                return 1
            else:
                return -1
        elif unit1 == UNIT_DAY:
            return 1
        else:
            raise ValueError("unable to compare unknown unit type {}".format(timeunit_suffix(unit1)))
    else:
        if unit_can_be_seconds(unit2):
            raise ValueError("cannot compare two different kinds of time units ({} and {})".format(
                timeunit_suffix(unit1), timeunit_suffix(unit2)))
        if unit1 == UNIT_MONTH:
            return -1
        elif unit1 == UNIT_YEAR:
            if unit2 == UNIT_MONTH:
                return 1
            else:
                return -1
        elif unit1 == UNIT_DECADE:
            if unit2 in (UNIT_MONTH, UNIT_YEAR):
                return 1
            else:
                return -1
        elif unit1 == UNIT_NORMAL:
            if unit2 in (UNIT_MONTH, UNIT_YEAR, UNIT_DECADE):
                return 1
            else:
                return -1
        elif unit1 == UNIT_CENTURY:
            if unit2 in (UNIT_MONTH, UNIT_YEAR, UNIT_DECADE, UNIT_NORMAL):
                return 1
            else:
                return -1
        else:
            raise ValueError("unable to compare unknown unit type {}".format(timeunit_suffix(unit1)))


def restrict_unit(val: int, unit: int) -> Tuple[int, int]:
    """
    Convert the value/unit pair to an equivalent pair with a unit with a smaller
    granularity (e.g. years to months, hours to minutes), with no loss of
    precision.

    Return (val, unit) if the value was unchanged because no further change is
    possible, otherwise the new (val, unit) pair
    """
    if unit == UNIT_MINUTE:
        return val * 60, UNIT_SECOND
    elif unit == UNIT_HOUR:
        return val * 60, UNIT_MINUTE
    elif unit == UNIT_DAY:
        return val * 24, UNIT_HOUR
    elif unit == UNIT_MONTH:
        return val, unit
    elif unit == UNIT_YEAR:
        return val * 12, UNIT_MONTH
    elif unit == UNIT_DECADE:
        return val * 10, UNIT_YEAR
    elif unit == UNIT_NORMAL:
        return val * 3, UNIT_DECADE
    elif unit == UNIT_CENTURY:
        return val * 10, UNIT_DECADE
    elif unit == UNIT_3HOURS:
        return val * 3, UNIT_HOUR
    elif unit == UNIT_6HOURS:
        return val * 2, UNIT_3HOURS
    elif unit == UNIT_12HOURS:
        return val * 2, UNIT_6HOURS
    elif unit == UNIT_SECOND:
        return val, unit
    elif unit == UNIT_MISSING:
        return val, unit
    else:
        return val, unit


def enlarge_unit(val: int, unit: int) -> bool:
    """
    Convert the value/unit pair to an equivalent pair with a unit with a larger
    granularity (e.g. months to years, minutes to hours), with no loss of
    precision.

    Return (val, unit) if the value was unchanged because no further change is
    possible, otherwise the new (val, unit) tuple
    """
    if unit == UNIT_MINUTE:
        if (val % 60) == 0:
            return val / 60, UNIT_HOUR
    elif unit == UNIT_HOUR:
        if (val % 24) == 0:
            return val / 24, UNIT_DAY
    elif unit == UNIT_DAY:
        return val, unit
    elif unit == UNIT_MONTH:
        if (val % 12) == 0:
            return val / 12, UNIT_YEAR
    elif unit == UNIT_YEAR:
        if (val % 10) == 0:
            return val / 10, UNIT_DECADE
    elif unit == UNIT_DECADE:
        if (val % 100) == 0:
            return val / 100, UNIT_CENTURY
    elif unit == UNIT_NORMAL:
        return val, unit
    elif unit == UNIT_CENTURY:
        return val, unit
    elif unit == UNIT_3HOURS:
        if (val % 2) == 0:
            return val / 2, UNIT_6HOURS
    elif unit == UNIT_6HOURS:
        if (val % 2) == 0:
            return val / 2, UNIT_12HOURS
    elif unit == UNIT_12HOURS:
        if (val % 2) == 0:
            return val / 2, UNIT_DAY
    elif unit == UNIT_SECOND:
        if (val % 60) == 0:
            return val / 60, UNIT_MINUTE
    elif unit == UNIT_MISSING:
        return val, unit
    else:
        return val, unit

    return val, unit


def make_same_units(tr: Dict[str, Any]):
    """
    Make sure val1:unit1 and val2:unit2 are expressed using the same units.
    """
    unit1, unit2 = tr["step_unit"], tr["stat_unit"]
    val1, val2 = tr["step_len"], tr["stat_len"]
    if unit1 == unit2:
        return

    # Try enlarging the smallest
    while True:
        cmp = compare_units(unit1, unit2)
        if cmp < 0:
            tval1, tunit1 = enlarge_unit(val1, unit1)
            if tunit1 == unit1:
                break
            else:
                val1, unit1 = tval1, tunit1
        elif cmp == 0:
            tr["step_unit"], tr["stat_unit"] = unit1, unit2
            tr["step_len"], tr["stat_len"] = val1, val2
            return
        else:
            tval2, tunit2 = enlarge_unit(val2, unit2)
            if tunit2 == unit2:
                break
            else:
                val2, unit2 = tval2, tunit2

    # Still not matching, try restricting the largest
    while True:
        cmp = compare_units(unit1, unit2)
        if cmp < 0:
            tval2, tunit2 = restrict_unit(val2, unit2)
            if tunit2 == unit2:
                break
            else:
                val2, unit2 = tval2, tunit2
        elif cmp == 0:
            tr["step_unit"], tr["stat_unit"] = unit1, unit2
            tr["step_len"], tr["stat_len"] = val1, val2
            return
        else:
            tval1, tunit1 = restrict_unit(val1, unit1)
            if tunit1 == unit1:
                break
            else:
                val1, unit1 = tval1, tunit1

    raise ValueError("Cannot convert {} and {} to the same unit".format(
        timeunit_suffix(unit1), timeunit_suffix(unit2)))


__all__ = [
    UNIT_MINUTE,
    UNIT_HOUR,
    UNIT_DAY,
    UNIT_MONTH,
    UNIT_YEAR,
    UNIT_DECADE,
    UNIT_NORMAL,
    UNIT_CENTURY,
    UNIT_3HOURS,
    UNIT_6HOURS,
    UNIT_12HOURS,
    UNIT_SECOND,
    UNIT_MISSING,
    make_same_units,
]
