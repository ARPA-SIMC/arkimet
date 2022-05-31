arki-server HTTP API
====================

Sorting
-------

The argument to the ``&sort=…`` query argument specifies how to sort data in the
output.

Its syntax is::

	[interval:]type[,type...]

``type`` can be one of ``origin``, ``product``, ``level``, ``timerange``,
``reftime``, ``area``, ``proddef``, ``run``, ``task``, ``quantity``, ``value``.
It specifies what metadata types are used for sorting.

Data will be sorted with the first type listed, and when it is the same by the
second type listed, and so on.

Prefix the metadata type with a ``-`` to specify reverse sorting. For example:
``-reftime,product`` gives newer data first, and for data with the same reference
time, sorting by product.

``interval`` can be one of ``minute``, ``hour``, ``day``, ``month``, ``year``.
If specified, data are grouped together by the given interval, and only data
within the same interval are sorted. Groups will be in increasing reftime
order. This allows to avoid storing all data in memory for sorting, requiring
only storage space for one group at a time. It also means that data can be
streamed as soon as the first group has been collected and sorted, instead of
waiting for all the query results to have been produced and sorted before
beginning downloading the results.


``arki-server`` urls
--------------------

``GET /``
^^^^^^^^^

Shows an index with a link to all datasets

``GET /config``
^^^^^^^^^^^^^^^

Returns an arkimet configuration file with information on all datasets known
by this instance of ``arki-server``.

``POST /qexpand``
^^^^^^^^^^^^^^^^^

POST arguments:

* ``query=<arkimet query>``

Returns ``query`` with all aliases expanded with the alias database used by
this instance of ``arki-server``.

This is used by ``arki-query`` to verify that a query containing aliases is
interpreted consistently across different systems.

``GET /aliases``
^^^^^^^^^^^^^^^^

Returns the alias database used by this instance of ``arki-server``.

``POST /query``
^^^^^^^^^^^^^^^

POST arguments:

* ``query=<arkimet query>``
* ``qmacro=<same as arki-query --qmacro>``
* ``style=<see below>``
* ``sort=<same as arki-query --sort>``
* ``command=<postprocessing command>``

Query data in all datasets, like an API version of arki-query.

Style control how the query results are returned, and can be one of:

 * ``metadata``: return the metadata, in binary format
 * ``inline``: return the metadata and the data, in binary format
 * ``data``: return the data only
 * ``postprocess``: postprocess using the given ``command``. Any files uploaded
   will be made useable by the postprocessor
 * ``rep_metadata``: return the output of the metadata report named by ``command``
 * ``rep_summary``: return the output of the summary report named by ``command``


``/summary``
^^^^^^^^^^^^

POST arguments:

* ``query=<arkimet query>``
* ``style=<see below>``

Query the merged summary of all datasets.

Style can be one of:

 * ``binary``: return the summary in arkimet's compact binary format
 * ``yaml``: return the summary in yaml format
 * ``json``: return the summary in json format

``GET /dataset/<name>``
^^^^^^^^^^^^^^^^^^^^^^^

Shows a page describing the dataset

``POST /dataset/<name>/query``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

POST arguments:

* ``query=<arkimet query>``
* ``style=<see below>``
* ``sort=<same as arki-query --sort>``
* ``command=<postprocessing command>``

Query data in the given dataset, like an API version of arki-query.

Style control how the query results are returned, and can be one of:

 * ``metadata``: return the metadata, in binary format
 * ``inline``: return the metadata and the data, in binary format
 * ``data``: return the data only
 * ``postprocess``: postprocess using the given ``command``. Any files uploaded
   will be made useable by the postprocessor
 * ``rep_metadata``: return the output of the metadata report named by ``command``
 * ``rep_summary``: return the output of the summary report named by ``command``

**Deprecated**: this endpoint is also available as ``GET`` only if
``style=postprocess`` for compatibility with an old client. Please only use
``POST`` instead of ``GET``.

``POST /dataset/<name>/summary``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

POST arguments:

* ``query=<arkimet query>``
* ``style=<see below>``

Query the summary of the given dataset.

Style can be one of:

 * ``binary``: return the summary in arkimet's compact binary format
 * ``yaml``: return the summary in yaml format
 * ``json``: return the summary in json format

``POST /dataset/<name>/summaryshort``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

POST arguments:

* ``query=<arkimet query>``
* ``style=<see below>``

Query the summary of the given dataset, outputting only the list of all
metadata values that exist in the query results.

Style can be one of:

 * ``binary``: return the summary in arkimet's compact binary format
 * ``yaml``: return the summary in yaml format
 * ``json``: return the summary in json format

``GET /dataset/<name>/config``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Returns an arkimet configuration file with the configuration for this dataset
in this instance of ``arki-server``.

.. toctree::
   :maxdepth: 2
   :caption: Contents:
