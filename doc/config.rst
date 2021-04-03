Arkimet runtime configuration
=============================

Environment variables
---------------------

There are a number of environment variables that can be used to control arkimet.

You can use ``arki-dump --info`` to list the current configuration in
machine-parsable JSON format.


``ARKI_FORMATTER``
~~~~~~~~~~~~~~~~~~

Default: ``/etc/arkimet/format``

Directory which contains Python scripts that can be used to customize how values
are formatted when using ``--format`` kind of options.

The value provided will be searched *before*, not *instead*, the default value.


``ARKI_BBOX``
~~~~~~~~~~~~~

Default: ``/etc/arkimet/bbox``

Directory which contains Python scripts that can be used to customize how
bounding boxes are computed from ``Area`` metadata.

The value provided will be searched *before*, not *instead*, the default value.


``ARKI_POSTPROC``
~~~~~~~~~~~~~~~~~

Default: ``/usr/lib/arkimet``

Directory which contains executable programs that can be used as
postprocessors.

The value provided will be searched *before*, not *instead*, the default value.


``ARKI_QMACRO``
~~~~~~~~~~~~~~~

Default: ``/etc/arkimet/qmacro``

Directory which contains Python scripts that implement the available querymacro
options.

The value provided will be searched *before*, not *instead*, the default value.


``ARKI_SCAN``
~~~~~~~~~~~~~

Default: ``/etc/arkimet/scan``

Directory which contains Python scripts that compute metadata from raw input
data.

The value provided will be searched *before*, not *instead*, the default value.

``ARKI_SCAN_GRIB1``, ``ARKI_SCAN_GRIB2``, ``ARKI_SCAN_BUFR``,
``ARKI_SCAN_ODIMH5`` are also supported for compatibility with the past, but
should be considered deprecated.


``ARKI_IOTRACE``
~~~~~~~~~~~~~~~~

Default: unset.

When set, it is a pathname to a file where arkimet will write a log of I/O
operations, used for performance debugging.


``ARKI_IO_TIMEOUT``
~~~~~~~~~~~~~~~~~~~

Default: 0 (no timeout)

Timeout (in seconds) used for write operations while programs like
``arki-query`` or ``arki-server`` are sending out data.


.. toctree::
   :maxdepth: 2
   :caption: Contents:
