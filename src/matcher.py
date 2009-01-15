# matcher.py - Match metadata expressions
#
# Copyright (C) 2007  ARPA-SIM <urpsim@smr.arpa.emr.it>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
#
# Author: Enrico Zini <enrico@enricozini.com>

from metadata import Metadata
from datetime import datetime, timedelta
import re

_ends = (None, None, timedelta(days=1), timedelta(hours=1), timedelta(minutes=1), timedelta(seconds=1))
def _lowerbound(tt):
    value = [0, 1, 1, 0, 0, 0]
    for i, e in enumerate(tt):
        value[i] = e
    return datetime(*value)
def _upperbound(tt):
    value = [0, 1, 1, 0, 0, 0]
    for i, e in enumerate(tt):
        value[i] = e
    # Extend until the end of the given time
    if len(tt) == 1:
        value[0] += 1
        return datetime(*value)
    elif len(tt) == 2:
        value[1] += 1
        if value[1] == 13:
            value[1] = 1
            value[0] += 1
        return datetime(*value)
    else:
        return datetime(*value) + _ends[len(tt)-1]

class Matcher:
    """
    Match metadata expressions.

    A metadata expression is a sequence of expressions separated by newlines or
    semicolons.

    Every expression matches one part of a metadata.  It begins with the
    expression type, followed by colon, followed by the value to match.

    Every expression of the matcher must be easily translatable into a SQL
    query, in order to be able to efficiently use a Matcher with an index.

    Example:
      origin:GRIB,,21
      reftime:>=2007-04-01,<=2007-05-10
    """

    class MatchOrigin:
        def __init__(self, pattern):
            self.items = [None] * 4
            for idx, el in enumerate(re.split(r"\s*,\s*", pattern.strip())):
                if el != "":
                    self.items[idx] = el
        def match(self, md):
            "Return true if one of the origins in md matches"
            for o in md.get_origins():
                valid = True
                for i, m in enumerate(self.items):
                    if m == None: continue
                    if i >= len(o) or m != str(o[i]):
                        valid = False
                        break
                if valid: return True
            return False

    class MatchReftime:
        def _timetuple(self, string):
            "Split the elements of a partial time into a list"
            #print >>sys.stderr, string
            mo = re.match(r"(\d+)(?:-(\d+)(?:-(\d+)(?:(?:\s*|T)(\d+)(?::(\d+)(?::(\d+))?)?)?)?)?", string)
            return map(int, filter(lambda x: x!=None, mo.groups()))
            
        class LE:
            def __init__(self, tt):
                self.dt = _upperbound(tt)
            def match(self, dt):
                if isinstance(dt, datetime):
                    return dt < self.dt 
                else:
                    return dt[1] < self.dt 
        class LT:
            def __init__(self, tt):
                self.dt = _lowerbound(tt)
            def match(self, dt):
                if isinstance(dt, datetime):
                    return dt < self.dt 
                else:
                    return dt[1] < self.dt 
        class GT:
            def __init__(self, tt):
                self.dt = _upperbound(tt)
            def match(self, dt):
                if isinstance(dt, datetime):
                    return dt >= self.dt 
                else:
                    return dt[0] >= self.dt 
        class GE:
            def __init__(self, tt):
                self.dt = _lowerbound(tt)
            def match(self, dt):
                if isinstance(dt, datetime):
                    return dt >= self.dt 
                else:
                    return dt[0] >= self.dt 
        class EQ:
            def __init__(self, tt):
                self.ge = _lowerbound(tt)
                self.lt = _upperbound(tt)
            def match(self, dt):
                if isinstance(dt, datetime):
                    return dt >= self.ge and dt < self.lt
                else:
                    # If it's an interval, return true if we intersect it
                    return not (dt[1] < self.ge or dt[0] >= self.lt)

        def __init__(self, pattern):
            self.tests = []
            for idx, el in enumerate(re.split(r"\s*,\s*", pattern.strip())):
                if el.startswith(">="):
                    self.tests.append(self.GE(self._timetuple(el[2:])))
                elif el.startswith(">"):
                    self.tests.append(self.GT(self._timetuple(el[1:])))
                elif el.startswith("<="):
                    self.tests.append(self.LE(self._timetuple(el[2:])))
                elif el.startswith("<"):
                    self.tests.append(self.LT(self._timetuple(el[1:])))
                elif el.startswith("=="):
                    self.tests.append(self.EQ(self._timetuple(el[2:])))
                elif el.startswith("="):
                    self.tests.append(self.EQ(self._timetuple(el[1:])))
                else:
                    raise ValueError("Unrecognised comparison in: '%s'" % el)
        def match(self, md):
            "Return true if one of the reference times in md matches"
            for t in md.get_reference_time_info():
                valid = True
                for test in self.tests:
                    if not test.match(t):
                        valid = False
                        break
                if valid: return True
            return False

    def __init__(self, expr):
        """
        Initialise the matcher parsing the given expression
        """
        # Split on newlines or semicolons
        self.exprs = []
        for e in re.split(r"\s*[\n;]+\s*", expr):
            type, pattern = re.split(r"\s*:\s*", e, 1)
            type = type.lower()
            if type == 'origin':
                self.exprs.append(self.MatchOrigin(pattern))
            elif type == 'reftime':
                self.exprs.append(self.MatchReftime(pattern))
            else:
                raise ValueError("Unknown match type: '%s'" % type)

    def match(self, md):
        for e in self.exprs:
            if not e.match(md):
                return False
        return True

if __name__ == "__main__":
    import sys
    import unittest

    verbose = ("--verbose" in sys.argv)
    #print >>sys.stderr, verbose

    class Test(unittest.TestCase):
        def setUp(self):
            self.md = Metadata()
            self.md.create()
            self.md.set_origins([('GRIB', 1, 2, 3)])
            self.md.set_reference_time_info(datetime(2007, 1, 2, 3, 4, 5))

        def testOriginMatcher(self):
            m = Matcher.MatchOrigin('GRIB')
            self.assert_(m.match(self.md))
            m = Matcher.MatchOrigin('GRIB,,,')
            self.assert_(m.match(self.md))
            m = Matcher.MatchOrigin('GRIB,1,,')
            self.assert_(m.match(self.md))
            m = Matcher.MatchOrigin('GRIB,,2,')
            self.assert_(m.match(self.md))
            m = Matcher.MatchOrigin('GRIB,,,3')
            self.assert_(m.match(self.md))
            m = Matcher.MatchOrigin('GRIB,1,2,')
            self.assert_(m.match(self.md))
            m = Matcher.MatchOrigin('GRIB,1,,3')
            self.assert_(m.match(self.md))
            m = Matcher.MatchOrigin('GRIB,,2,3')
            self.assert_(m.match(self.md))
            m = Matcher.MatchOrigin('GRIB,1,2,3')
            self.assert_(m.match(self.md))
            m = Matcher.MatchOrigin('GRIB,2')
            self.assert_(not m.match(self.md))
            m = Matcher.MatchOrigin('GRIB,,3')
            self.assert_(not m.match(self.md))
            m = Matcher.MatchOrigin('GRIB,,,1')
            self.assert_(not m.match(self.md))
            m = Matcher.MatchOrigin('BUFR,1,2,3')
            self.assert_(not m.match(self.md))

        def testReftimeMatcher(self):
            m = Matcher.MatchReftime(">=2007")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime("<=2007")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime(">2006")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime("<2008")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime("==2007")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime("=2007")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime(">=2008")
            self.assert_(not m.match(self.md))
            m = Matcher.MatchReftime("<=2006")
            self.assert_(not m.match(self.md))
            m = Matcher.MatchReftime(">2007")
            self.assert_(not m.match(self.md))
            m = Matcher.MatchReftime("<2007")
            self.assert_(not m.match(self.md))
            m = Matcher.MatchReftime("==2006")
            self.assert_(not m.match(self.md))

            m = Matcher.MatchReftime(">=2007-01")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime("<=2007-01")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime(">2006-12")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime("<2007-02")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime("==2007-01")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime("=2007-01")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime(">=2007-02")
            self.assert_(not m.match(self.md))
            m = Matcher.MatchReftime("<=2006-12")
            self.assert_(not m.match(self.md))
            m = Matcher.MatchReftime(">2007-01")
            self.assert_(not m.match(self.md))
            m = Matcher.MatchReftime("<2007-01")
            self.assert_(not m.match(self.md))
            m = Matcher.MatchReftime("==2007-02")
            self.assert_(not m.match(self.md))

            m = Matcher.MatchReftime(">=2007-01-02")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime("<=2007-01-02")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime(">2007-01-01")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime("<2007-01-03")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime("==2007-01-02")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime("=2007-01-02")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime(">=2007-01-03")
            self.assert_(not m.match(self.md))
            m = Matcher.MatchReftime("<=2006-01-01")
            self.assert_(not m.match(self.md))
            m = Matcher.MatchReftime(">2007-01-02")
            self.assert_(not m.match(self.md))
            m = Matcher.MatchReftime("<2007-01-02")
            self.assert_(not m.match(self.md))
            m = Matcher.MatchReftime("==2007-01-01")
            self.assert_(not m.match(self.md))

            m = Matcher.MatchReftime(">=2007-01-02 03")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime("<=2007-01-02 03")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime(">2007-01-02 02")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime("<2007-01-02 04")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime("==2007-01-02 03")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime("=2007-01-02 03")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime(">=2007-01-02 04")
            self.assert_(not m.match(self.md))
            m = Matcher.MatchReftime("<=2006-01-02 02")
            self.assert_(not m.match(self.md))
            m = Matcher.MatchReftime(">2007-01-02 03")
            self.assert_(not m.match(self.md))
            m = Matcher.MatchReftime("<2007-01-02 03")
            self.assert_(not m.match(self.md))
            m = Matcher.MatchReftime("==2007-01-02 02")
            self.assert_(not m.match(self.md))

            m = Matcher.MatchReftime(">=2007-01-02 03:04")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime("<=2007-01-02 03:04")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime(">2007-01-02 03:03")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime("<2007-01-03 03:05")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime("==2007-01-02 03:04")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime("=2007-01-02 03:04")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime(">=2007-01-02 03:05")
            self.assert_(not m.match(self.md))
            m = Matcher.MatchReftime("<=2006-01-02 03:03")
            self.assert_(not m.match(self.md))
            m = Matcher.MatchReftime(">2007-01-02 03:04")
            self.assert_(not m.match(self.md))
            m = Matcher.MatchReftime("<2007-01-02 03:04")
            self.assert_(not m.match(self.md))
            m = Matcher.MatchReftime("==2007-01-02 03:03")
            self.assert_(not m.match(self.md))

            m = Matcher.MatchReftime(">=2007-01-02 03:04:05")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime("<=2007-01-02 03:04:05")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime(">2007-01-02 03:04:04")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime("<2007-01-03 03:04:06")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime("==2007-01-02 03:04:05")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime("=2007-01-02 03:04:05")
            self.assert_(m.match(self.md))
            m = Matcher.MatchReftime(">=2007-01-02 03:04:06")
            self.assert_(not m.match(self.md))
            m = Matcher.MatchReftime("<=2006-01-02 03:04:04")
            self.assert_(not m.match(self.md))
            m = Matcher.MatchReftime(">2007-01-02 03:04:05")
            self.assert_(not m.match(self.md))
            m = Matcher.MatchReftime("<2007-01-02 03:04:05")
            self.assert_(not m.match(self.md))
            m = Matcher.MatchReftime("==2007-01-02 03:04:04")
            self.assert_(not m.match(self.md))

    #   def testReferenceTimeInfo(self):
    #       time = datetime(2002, 9, 8, 7, 6, 5)
    #       self.md.set_reference_time_info(time)
    #       time1 = self.md.get_reference_time_info()
    #       self.assertEqual([time], time1)

    #       timea = datetime(2002, 6, 5, 4, 3, 2)
    #       timeb = datetime(2002, 7, 6, 5, 4, 3)
    #       self.md.set_reference_time_info((timea, timeb))
    #       if verbose: self.md.write(sys.stderr)
    #       times = self.md.get_reference_time_info()
    #       self.assertEqual(len(times), 1)
    #       timea1, timeb1 = times[0]

    #       self.assertEqual(timea, timea1)
    #       self.assertEqual(timeb, timeb1)

    #   def testNotes(self):
    #       self.md.add_note("this is a test note")
    #       self.md.add_note("this is another test note")
    #       notes = self.md.get_notes()
    #       self.assertEqual(len(notes), 2)
    #       self.assert_( (datetime.now() - notes[0][0]) < timedelta(hours = 1))
    #       self.assertEqual(notes[0][1], "this is a test note")
    #       self.assert_( (datetime.now() - notes[1][0]) < timedelta(hours = 1))
    #       self.assertEqual(notes[1][1], "this is another test note")
    #       if verbose: self.md.write(sys.stderr)

    unittest.main()

