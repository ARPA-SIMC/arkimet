Using aliases
=============

For every metadata type it is possible to define some aliases for the most
commonly used search expressions.

Aliases are defined in ``/etc/arkimet/match-alias.conf``, which has one section per
metadata type. For example::

  [origin]
  arpa = GRIB1,200

  [product]
  # Pressure
  pr      = GRIB1,200,2,1
  # MSL pressure
  mslp    = GRIB1,200,2,2  or GRIB1,98,128,151
  # Geopotential
  z       = GRIB1,200,2,6  or GRIB1,98,128,129
  # Temperature
  t       = GRIB1,200,2,11 or GRIB1,98,128,130 or GRIB1,98,128,167 or GRIB1,200,200,11
  
  [level]
  # Surface
  g00    = GRIB1,1                                              
  
  # Height from surface
  g02    = GRIB1,105,2                                            
  g10    = GRIB1,105,10


.. toctree::
   :maxdepth: 2
   :caption: Contents:

.. doctest matched: 0
.. doctest not_matched: 0
