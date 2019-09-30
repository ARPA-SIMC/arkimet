.. _segments:

Data segments
=============

Most arkimet datasets (see :ref:`datasets`) are organised as a collection of
segments, where each segment contains data spanning a given time period.

Arkimet supports various types of segments, to store data which can or cannot
be concatenated, and possibly use compression.

Data in each segment, regardless of its type, can be trivially extracted using
standard system tools, to make it possible to recover stored data without
needing the availability of arkimet code.

A segment type is selected automatically according to the requirements of the
data format stored in the dataset, and it is possible to force a dataset to use
a ``dir`` segment by configuring it as ``segments = dir``.

Segments can be converted to ``tar``, ``zip``, and ``gz`` segments via
``arki-check`` ``--tar``, ``--zip``, and ``--compress`` options.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   segments/concat
   segments/dir
   segments/gz
   segments/tar
   segments/zip
