Matching run information
========================

MINUTE
------

Syntax: ``run:MINUTE,hour:min``

``min`` can be ommitted: if omitted, ``00`` is assumed.

``hour:min`` can be completely omitted: in that case any ``MINUTE`` run will match.

Examples
^^^^^^^^

Given run ``MINUTE(12:00)``:

* ``run:MINUTE`` matches
* ``run:MINUTE,12`` matches
* ``run:MINUTE,12:00`` matches
* ``run:MINUTE,13`` does not match
* ``run:MINUTE,12:30`` does not match

Given run ``MINUTE(12:30)``:

* ``run:MINUTE,12:30`` matches
* ``run:MINUTE,12`` does not match

.. toctree::
   :maxdepth: 2
   :caption: Contents:

.. doctest matched: 4
.. doctest not_matched: 3
