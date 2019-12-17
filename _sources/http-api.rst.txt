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

``/``
^^^^^

Shows an index with a link to all datasets

``/config``
^^^^^^^^^^^

Returns an arkimet configuration file with information on all datasets known
by this instance of ``arki-server``.

``/qexpand?query=<query>``
^^^^^^^^^^^^^^^^^^^^^^^^^^

Returns ``query`` with all aliases expanded with the alias database used by
this instance of ``arki-server``.

This is used by ``arki-query`` to verify that a query containing aliases is
interpreted consistently across different systems.

``/aliases``
^^^^^^^^^^^^

Returns the alias database used by this instance of ``arki-server``.

``/query?query=<query>&qmacro=<qmacro>&style=<style>[&sort=<sort>][&command=<command>]``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Query data in all datasets, aggregating it using the query macro named by
``<qmacro>``.

Style control how the query results are returned, and can be one of:

 * ``metadata``: return the metadata, in binary format
 * ``inline``: return the metadata and the data, in binary format
 * ``data``: return the data only
 * ``postprocess``: postprocess using the given ``command``. Any files uploaded
   will be made useable by the postprocessor
 * ``rep_metadata``: return the output of the metadata report named by ``command``
 * ``rep_summary``: return the output of the summary report named by ``command``


``/summary?query=<query>&style=<style>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Query the merged summary of all datasets.

Style can be one of:

 * ``binary``: return the summary in arkimet's compact binary format
 * ``yaml``: return the summary in yaml format
 * ``json``: return the summary in json format

``/dataset/<name>``
^^^^^^^^^^^^^^^^^^^

Shows a page describing the dataset

``/dataset/<name>/query?query=<query>&style=<style>[&sort=<sort>][&command=<command>]``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Query data in the given dataset.

Style control how the query results are returned, and can be one of:

 * ``metadata``: return the metadata, in binary format
 * ``inline``: return the metadata and the data, in binary format
 * ``data``: return the data only
 * ``postprocess``: postprocess using the given ``command``. Any files uploaded
   will be made useable by the postprocessor
 * ``rep_metadata``: return the output of the metadata report named by ``command``
 * ``rep_summary``: return the output of the summary report named by ``command``

``/dataset/<name>/summary?query=<query>&style=<style>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Query the summary of the given dataset.

Style can be one of:

 * ``binary``: return the summary in arkimet's compact binary format
 * ``yaml``: return the summary in yaml format
 * ``json``: return the summary in json format

``/dataset/<name>/summaryshort?query=<query>&style=<style>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Query the summary of the given dataset, outputting only the list of all
metadata values that exist in the query results.

Style can be one of:

 * ``binary``: return the summary in arkimet's compact binary format
 * ``yaml``: return the summary in yaml format
 * ``json``: return the summary in json format

`/dataset/<name>/config`
^^^^^^^^^^^^^^^^^^^^^^^^

Returns an arkimet configuration file with the configuration for this dataset
in this instance of ``arki-server``.

.. toctree::
   :maxdepth: 2
   :caption: Contents:
