Matching origins
================

GRIB1
-----

Syntax: ``origin:GRIB1,centre,subcentre,process``

Any of ``centre``, ``subcentre``, ``process`` can be omitted; if omitted, any
value will match.

Examples
^^^^^^^^

Given origin ``GRIB1(98, 0, 12)``:

* ``origin:GRIB1`` matches: you can omit trailing commas
* ``origin:GRIB1,,,`` matches
* ``origin:GRIB1,98`` matches
* ``origin:GRIB1,98,,12`` matches
* ``origin:GRIB2`` does not match
* ``origin:GRIB1,200`` does not match
* ``origin:GRIB1,98,0,11`` does not match

GRIB2
-----

Syntax: ``origin:GRIB2,centre,subcentre,processtype,bgprocessid,processid``

Any of ``centre``, ``subcentre``, ``processtype``, ``bgprocessid``,
``processid`` can be omitted; if omitted, any value will match.

Examples
^^^^^^^^

Given origin ``GRIB2(98, 0, 1, 2, 3)``:

* ``origin:GRIB2`` matches
* ``origin:GRIB2,98`` matches
* ``origin:GRIB2,98,,,2`` matches
* ``origin:GRIB2,98,0,1,2,3`` matches
* ``origin:GRIB1`` does not match
* ``origin:GRIB2,200`` does not match
* ``origin:GRIB2,98,1,,2`` does not match

BUFR
----

Syntax: ``origin:BUFR,centre,subcentre``

Any of ``centre`` or ``subcentre`` can be omitted; if omitted, any value will
match.

``centre`` is the originating centre (see Common Code table C-11).

``subcentre`` is the originating subcentre (see Common Code table C-12).

Examples
^^^^^^^^

Given origin ``BUFR(98, 0)``:

* ``origin:BUFR`` matches
* ``origin:BUFR,,`` matches
* ``origin:BUFR,98`` matches
* ``origin:BUFR,98,0`` matches
* ``origin:BUFR,,0`` matches
* ``origin:GRIB1`` does not match
* ``origin:BUFR,200`` does not match
* ``origin:BUFR,98,1`` does not match

ODIMH5
------

Syntax: ``origin:ODIMH5,wmo,rad,nod_or_plc``

Origin of data identified using "WMO" (wmo), "RAD" (rad) and "NOD" or "PLC".

For the ``nod_or_plc`` attribute values, ``NOD`` takes precedence over ``PLC``.

``wmo``, ``rad`` and ``nod_or_plc`` values can be omitted.

Examples
^^^^^^^^

Given origin ``ODIMH5(02954, FI44, Anjalankoski)``:

* ``origin:ODIMH5`` matches
* ``origin:ODIMH5,02954`` matches
* ``origin:ODIMH5,,FI44`` matches
* ``origin:ODIMH5,,,Anjalankoski`` matches
* ``origin:GRIB1`` does not match
* ``origin:BUFR,200`` does not match
* ``origin:BUFR,98,1`` does not match

.. toctree::
   :maxdepth: 2
   :caption: Contents:

.. doctest matched: 17
.. doctest not_matched: 12
