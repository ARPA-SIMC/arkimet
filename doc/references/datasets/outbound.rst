.. _datasets-outbound:

``outbound`` dataset
====================

This dataset appends data to segments, and does nothing else. Notably, it has
no index and does not support queries or other kinds of maintenance like
archival or deletion of old data.

It was originally designed to support populating an export area made available
via an arragement like a directory exported via HTTP or public FTP.


Example configuration
---------------------

::

  [name]
  type = outbound
  step = daily
  filter = product:…
