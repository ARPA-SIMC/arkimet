.. _datasets-remote:

``remote`` dataset
==================

This configures a dataset which is stored remotely and accessed by querying a
remote `arki-server`.

This kind of dataset supports queries, but not dispatching or arki-check, which
can only be performed locally.

The ``path`` keyword will be a URL instead of a filesystem path


Example configuration
---------------------

::

  [example]
  type = remote
  path = http://arkimet.local/dataset/example
