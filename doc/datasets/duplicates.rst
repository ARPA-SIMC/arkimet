.. _datasets-duplicates:

``duplicates`` dataset
======================

This dataset holds data that was rejected as a duplicate during dispatch.

You can use ``duplicates`` as dataset type, in which case it defaults to
``simple`` (see :ref:`datasets-simple`). Otherwise any other dataset format
will do: other useful options can be :ref:`datasets-outbound` and
:ref:`datasets-discard`.


Example configuration
---------------------

This could be an example of a standard setup::

  [duplicates]
  type = duplicates
  step = daily
  delete age = 3

This will just store the data, but won't allow to query it or automatically
delete it, and will require inspecting it manually::

  [duplicates]
  type = outbound
  step = daily

This will throw away all data that has been interpreted as duplicates. You may
want to use a ``simple`` type first, to inspect duplicates for some time to
make sure your ``filter`` and ``unique`` rules in other datasets are correct,
and only then switch to ``discard``::

  [duplicates]
  type = discard

