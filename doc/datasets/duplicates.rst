.. _datasets-duplicates:

``duplicates`` dataset
======================

This dataset holds data that was rejected as a duplicate during dispatch.

You can use ``duplicates`` as dataset type, in which case it defaults to
``simple`` (see :ref:`datasets-simple`). Otherwise any other dataset format
will do: other useful options can be :ref:`datasets-outbound` and
:ref:`datasets-discard`.

You can give the duplicates dataset a name other than "error" by using
``use=duplicates`` in its configuration.

If the duplicates dataset is not defined, the error dataset is used instead
(see :ref:`datasets-error`).

Example configuration
---------------------

This could be an example of a standard setup::

  name = duplicates
  type = duplicates
  step = daily
  delete age = 3

This will just store the data, but won't allow to query it or automatically
delete it, and will require inspecting it manually::

  name = duplicates
  type = outbound
  step = daily

This will throw away all data that has been interpreted as duplicates. You may
want to use a ``simple`` type first, to inspect duplicates for some time to
make sure your ``filter`` and ``unique`` rules in other datasets are correct,
and only then switch to ``discard``::

  name = duplicates
  type = discard

This is an alternate way of specifying the same thing::

  name = devnull
  type = discard
  use = duplicates
