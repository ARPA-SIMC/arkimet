.. _datasets-error:

``error`` dataset
=================

This dataset holds data that could not be dispatched successfully.

You can use ``error`` as dataset type, in which case it defaults to ``simple``
(see :ref:`datasets-simple`). Otherwise any other dataset format will do: other
useful options can be :ref:`datasets-outbound` and :ref:`datasets-discard`.

A common reason data may end up in the error dataset is if it not matched by
any dataset ``filter`` rule. Storing it in the error dataset means the data is
not lost, and can be inspected and reimported after adjusting the filter rules.


Example configuration
---------------------

This could be an example of a standard setup::

  [error]
  type = error
  step = daily
  delete age = 15

This will just store the data, but won't allow to query it or automatically
delete it, and will require inspecting it manually::

  [error]
  type = outbound
  step = daily

This will throw away all data which had issues during dispatch. It may be
useful in some cases, but it will make dispatch errors unrecoverable unless you
keep the original data::

  [error]
  type = discard
