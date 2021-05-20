.. _datasets-discard:

``discard`` dataset
===================

This dataset type just discards all data during dispatch.

It is a special purpose dataset type that can be used for
:ref:`datasets-duplicates` or, specifying a ``filter``, to throw away
well-known data that one does not want to keep and to clutter the error
dataset.


Example configuration
---------------------

::

  [name]
  type = discard
  filter = origin:GRIB,200
