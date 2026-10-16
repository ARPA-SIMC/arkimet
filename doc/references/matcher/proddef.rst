Matching proddef information
============================

Matching GRIB proddefs
----------------------

Syntax: ``proddef:GRIB:key1=val1,key2=val2,...``

Matches any GRIB proddef that has at least all the given keys, with the given
values.

There is no exaustive list of possible keys, as they can be anything that
gets set by the GRIB scanning configuration.

Examples
^^^^^^^^

Given proddef ``GRIB(md=1, nn=2)``:

* ``proddef:GRIB:md=1`` matches
* ``proddef:GRIB:md=1,nn=2`` matches
* ``proddef:GRIB:md=2,nn=2`` does not match
* ``proddef:GRIB:answer=42`` does not match

.. toctree::
   :maxdepth: 2
   :caption: Contents:

.. doctest matched: 2
.. doctest not_matched: 2
