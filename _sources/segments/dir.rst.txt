.. _segments-dir:

dir segments
============

This kind of segment stores all data inside a directory, with one file per
datum.

It is suitable for formats that do not support contatenation, like ODIMH5, or
for storing data that could be concatenated but is very large, like
high-resolution GRIB files.

Files are numbered sequentially, starting from 0. The name of each file is the
sequence number, and its extension is the format of the data it contains.

The directory also contains a ``.sequence`` file that is used for generating
sequence numbers when appending to the segment.

The directory name represents the period of time stored in the segment. The
directory extension represents the format of the data it contains.


.. toctree::
   :maxdepth: 2
   :caption: Contents:
