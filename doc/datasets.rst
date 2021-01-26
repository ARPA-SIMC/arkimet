.. _datasets:

Datasets
========

Arkimet supports different dataset formats, which offer different performance
and indexing features to match various ways in which data is stored and
queried.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   datasets/iseg
   datasets/simple
   datasets/ondisk2
   datasets/archive


Dataset configuration
---------------------

Datasets are configured with simple ``key = value`` configuration options.

These are all the supported options:

* ``archive age``: data older than this number of days will be moved to the
  dataset archive during maintenance.
* ``delete age``: data older than this number of days will be deleted during
  maintenance.
* ``eatmydata``: disable `fsync`/`fdatasync` operations while writing data to
  dataset, and disable sqlite' journaling and other data integrity features.
  This makes acquiring data very fast, but an interrupted import or a
  concurrent import may cause data corruption.
* ``format``: format of data in the dataset (one of ``grib``, ``bufr``, ``odimh5``, ``vm2``)
* ``index``: comma-separated list of names of metadata to index for faster queries
* ``path``: path to the dataset, or URL for ``remote`` datasets.
* ``replace``: when ``yes``, importing duplicate data will replace the existing
  version. When ``no``, importing duplicate data will be rejected. When
  ``usn``, importing duplicate BUFR data will replace the existing version only
  if the BUFR Update Sequence Number is greater than the one currently in the
  dataset.
* ``restrict``: comma-separated list of names that have access to the dataset.
  This allows filtering with the ``--restrict`` option on command line.
* ``smallfiles``: ``yes`` or ``no``. When ``yes``, the file contents are also
  saved in the index, to speed up extraction of data with tiny payloads like
  ``vm2``.
* ``step``: segmentation step for the dataset (one of ``daily``, ``weekly``,
  ``biweekly``, ``monthly``, and ``yearly``).
* ``type``: dataset type (one of ``iseg``, ``ondisk2``, ``simple``, ``error``,
  ``duplicates``, ``remote``, ``outbound``, ``discard``, ``file``).
* ``unique``: comma-separated list of names of metadata that, taken together,
  make it unique in the dataset
