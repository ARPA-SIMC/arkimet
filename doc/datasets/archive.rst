dataset archives
================

Datasets can distinguish between online and archived data stored in two
different formats.

Online data can be stored in a format with detailed indexing like ``iseg`` (see
:ref:`datasets-iseg`), while archived data can be stored in a format with less
features and overhead like ``simple`` (see :ref:`datasets-simple`).

Archives are stored in the ``.archive/`` subdirectory of a dataset. The
``archive age`` configuration value can be used to automate moving to
``.archive/``, during repack, those segments that only contain data older than
the given age.

Data is moved to ``.archive/`` as entire segments after checking and repacking of
the online part. This should ensure that archived segments contain no
duplicates and need no further repacking.

The ``.archive/`` directory contain several subdirectory, each with a dataset in
:ref:`simple <datasets-simple>` format containing data. Queries on the archives query
each dataset in sequence, ordered by name, with ``last`` always kept at the end.

Segments that get archived are moved from the online dataset to the
``.archive/last`` dataset, and can be manually moved from ``last`` to any
other subdirectory of ``.archive/``. arkimet will only ever move segments to
``.archive/last`` and the rest can be maintained with procedures external to
arkimet with no interference.


Online archives
---------------

Online archives are the default type of archive that is created automatically
as ``.archive/last`` during maintenance. They are instances of :ref:`simple
<datasets-simple>` datasets, and all the simple dataset documentation applies
to them.


Offline archives
----------------

Offline archives are archives whose data has been moved to external media. The
only thing that is left is a ``.summary`` file that describes the data that
would be there.

It is possible to bring an offline archive online by copying/linking/mounting
it next to its ``.summmary`` file. If both the ``.summary`` file and the archive
directory are present, arkimet will ignore the ``.summary`` file when reading,
and will ignore the archive directory when checking. This has the effect of
making an archive read-only when the ``$archivename.summary`` file is present.

To run a check/fix/repack operation on an offline archive, bring it online,
remove the ``$archivename.summary`` file, run the check/fix/repack operation, and
copy ``$archivename/summary`` to ``$archivename.summary`` to mark it read-only
again.

.. toctree::
   :maxdepth: 2
   :caption: Contents:
