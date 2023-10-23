Arkimet match-alias.conf database
=================================

``/etc/arkimet/match-alias.conf`` is a configuration file that contains a
database of shortcuts for match expressions (see :ref:`match_expressions`).

When launching arkimet commands, the file given in the environment variable
``ARKI_ALIASES`` is tried. Else, ``/etc/arkimet/match-alias.conf`` is tried.
Else, nothing is loaded.

The file is in ``.ini``-like format, with ``[sections]`` representing metadata
types and their ``key = value`` entries representing shortcuts: when a ``key``
is found in a match expression, it is recursively replaced with its value.

For example::

  [product]
  # Temperature
  t = GRIB1,200,2,11 or GRIB1,98,128,130 or GRIB1,98,128,167 or GRIB1,200,200,11 or GRIB2,200,0,200,11

This allows to construct a match expression such as ``product: t or GRIB1,200,2,12``.

Expansion happens recursively, so it is possible for an alias to reference another.

The aliases configured in the client and in the arki-server should always match.
If they don't, an error about conflicting aliases is raised, regardless of an alias
being used or not.
