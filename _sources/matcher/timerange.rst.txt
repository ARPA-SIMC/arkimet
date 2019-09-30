Matching time ranges
====================

Some time range matchers use time unit suffixes. Here is a table with their
meaning:

* ``s``:  seconds
* ``m``:  minutes
* ``h``:  hours
* ``d``:  days
* ``mo``: months
* ``y``:  years

When unit suffixes are used, all time values are normalised to either
seconds or months, according to what is appropriate. This means it is
possible to use seconds to match a time range expressed in hours, or to use
years to match a time range expressed in months, but it is not possible to
use months to match a time range expressed in days, because months have a
variable number of days.


GRIB1
-----

Syntax: ``timerange:GRIB1,type,p1,p2``

Any of ``type``, ``p1``, or ``p2`` can be omitted; if omitted, any value will match.

``p1`` and ``p2`` must have a time unit suffix (any of s,m,h,d,mo,y) unless they
are 0.

Examples
^^^^^^^^

Given timerange ``GRIB1(0, 6h)`` (6-hours forecast)

* ``timerange:GRIB1,0`` matches
* ``timerange:GRIB1,0,6h`` matches
* ``timerange:GRIB1,0,360m`` matches
* ``timerange:GRIB1,0,21600s`` matches
* ``timerange:GRIB1,1`` does not match
* ``timerange:GRIB1,0,6m`` does not match

Given timerange ``GRIB1(2, 6h, 12h)`` (valid between reftime+6h and reftime+12h):

* ``timerange:GRIB1,2`` matches
* ``timerange:GRIB1,2,6h,12h`` matches
* ``timerange:GRIB1,2,6h`` matches (partial match also works)
* ``timerange:GRIB1,1`` does not match
* ``timerange:GRIB1,2,6m,12m`` does not match


GRIB2
-----

Syntax: ``timerange:GRIB2,type,unit,p1,p2``

Any of type, unit, p1 or p2 can be omitted; if omitted, any value will match.

Unit is given explicitly and p1,p2 do not use suffixes.

Examples
^^^^^^^^

Given timerange ``GRIB2(0, 1, 6h, 0h)`` (6-hours forecast):

* ``timerange:GRIB2,0`` matches
* ``timerange:GRIB2,0,1,6`` matches
* ``timerange:GRIB2,0,1,6,0`` matches
* ``timerange:GRIB2,1`` does not match
* ``timerange:GRIB2,0,1,5`` does not match
* ``timerange:GRIB2,0,2,6`` does not match
* ``timerange:GRIB2,0,0,360`` does not match


Timedef
-------

Syntax: ``timerange:Timedef,fcstep,proctype,proclen``

* ``fcstep`` is the forecast step
* ``proctype`` is the type of statistical processing (use ``-`` for data with no
  statistical processing)
* ``proclen`` is the time duration the interval for statistical processing

Any of ``fcstep``, ``proctype``, ``proclen`` can be omitted; if omitted, any value
will match.

Examples
^^^^^^^^

Given timerange ``Timedef(6h)`` (6-hours forecast):

* ``timerange:Timedef,6h`` matches

Given timerange ``Timedef(6h, 0, 3h)`` (6-hours forecast, average over 3 hours):

* ``timerange:Timedef,6h,0,3h`` matches: 6hours forecast, average over 3 hours
* ``timerange:Timedef,,0,3h`` matches: all averages over 3 hours
* ``timerange:Timedef,3h`` does not match
* ``timerange:Timedef,,1,3h`` does not match


BUFR
----

Syntax: ``timerange:BUFR,val``

``val`` can be omitted; if omitted, any value will match.

``val`` must have a time unit suffix (any of s,m,h,d,mo,y) unless it is 0.

Examples
^^^^^^^^

Given timerange ``BUFR(6h)`` (6-hours forecast):

* ``timerange:BUFR`` matches
* ``timerange:BUFR,6h`` matches
* ``timerange:BUFR,360m`` matches
* ``timerange:BUFR,21600s`` matches
* ``timerange:BUFR,0`` does not match
* ``timerange:BUFR,6m`` does not match

.. toctree::
   :maxdepth: 2
   :caption: Contents:

.. doctest matched: 17
.. doctest not_matched: 12
