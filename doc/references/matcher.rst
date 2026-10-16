.. _match_expressions:

Match expressions
=================

An arkimet matcher is composed of various match expressions, grouped by
metadata type. Each group is separated by ``;`` or by a newline. All groups
must be satisfied for the matcher to match.

Each group contains one or more type-specific match expression, separated by
``or``. All expressions inside a group are ORed together, which means that only
one of them must be true for the group to match.

It is possible to do, for example::

    origin:arpa or GRIB1,98,0; product:t or z

This matches data which are from Arpa or ECMWF, and whose product is
temperature or geopotential.

It is not possible to say "this origin OR this product": only expressions
inside the same group can be ORed together.

Examples
--------

Given origin ``GRIB1(200, 0, 0)``, product ``GRIB1(98, 128, 167)``, level ``GRIB1(1)``:

* ``origin:arpa`` matches
* ``origin:arpa or GRIB1,98,0`` matches
* ``origin:arpa; product:t`` matches
* ``origin:GRIB1,200; product:t or z`` matches
* ``origin:arpa; product:t; level:g00 or g02`` matches

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   match-alias
   matcher/reftime
   matcher/origin
   matcher/area
   matcher/product
   matcher/level
   matcher/timerange
   matcher/proddef
   matcher/quantity
   matcher/task
   matcher/run

.. doctest matched: 5
.. doctest not_matched: 0
