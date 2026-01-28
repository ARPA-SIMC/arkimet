====================================
Adding support for a new file format
====================================

This document describes how to add support for a new data format in arkimet,
provided that:

* Data is contained inside a single file
* Concatenation of multiple data in a single file (GRIB or BUFR style) is not
  required
* New metadata types are not required
* There are Python libraries able to understand the data format, enough to be
  able to extract the relevant information for arkimet metadata

This should be a common case, and it is how ODIMH5, JPEG and NetCDF data is
currently handled.

Support is added in two steps:

1. Make the C++ arkimet file aware of the new file format, giving it an integer
   code, a string identifier, and a way to recognize the format from a file
   name.
2. Create a Python scanner to extract metadata from files.


Adding support in the C++ library
=================================

#. Decide a short string identifier for the new format. You can look at the
   ``format_names`` array in ``arki/defs.cc`` to see what are the existing
   ones.
#. In ``arki/defs.h``, add an entry for the new format in the ``DataFormat``
   enum.
#. In ``arki/defs.cc``, add the relevant name to the ``format_names`` array.
#. In ``arki/defs.cc``, add support for the new name in the
   ``format_from_string`` function.
#. In ``arki/data.cc``, add change ``detect_format`` to support the extension
   of the new file format.
#. Recompile arkimet


Adding a Python scanner
=======================

#. Copy ``python/arkimet/scan/netcdf.py`` to
   ``python/arckimet/scan/{formatname}.py``, where ``formatname`` is the string
   identifier previously added to ``arki/defs.cc``.
#. Using the NetCDF scanner as a template, code support for your new scanner.
#. Copy ``python/tests/test_scan_netcdf.py`` to
   ``python/tests/test_scan_{formatname}.py`` and use it as a template for your
   unit tests to test your new scanner's behaviour.
#. Update ``python/meson.build`` to include your new scanner in the arkimet
   binary distribution.
