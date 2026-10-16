=====================
arkimet documentation
=====================

Welcome to wreport's documentation. The documentation follows the `Diataxis
<https://diataxis.fr/>`_ structure of tutorials, how-to guides, explanations
and reference.

Introduction
============

Arkimet is a set of tools to organize, archive and distribute data files. It
currently supports data in GRIB, BUFR, HDF5 and VM2 formats.

Arkimet manages a set of datasets (see :ref:`datasets`), each of which contains
omogeneous data stored in segments (see :ref:`segments`). It exploits the
commonalities between the data in a dataset to implement a fast, and
space-efficient indexing system.
        
When data is ingested into arkimet, it is scanned and annotated with metadata,
such as reference time and product information, then it is dispatched to one of
the datasets configured in the system. From this point onwards, data are
treated like opaque blobs, and only metadata are used for indexing or
searching, reducing the need for format-specific operations after ingesting.
        
Datasets can be queried both locally and over the network using an HTTP-based
protocol.

Old data can be archived using an automatic maintenance procedure, and archives
can be taken offline and back online at will.

A summary of offline data is kept online, so that arkimet is able to report
that more data for a query would be available but is currently offline.
        
Arkimet is Free Software, licensed under the GNU General Public License version
2 or later.
 

.. toctree::
   :maxdepth: 2
   :caption: Tutorials


.. toctree::
   :maxdepth: 2
   :caption: Howtos

   howtos/new-data-format
   howtos/python-api/format_metadata.rst
   howtos/python-api/read_inline_file.rst

.. toctree::
   :maxdepth: 2
   :caption: Explanations


.. toctree::
   :maxdepth: 2
   :caption: References

   references/metadata
   references/matcher
   references/datasets
   references/segments
   references/http-api
   references/config
   references/python-api/arkimet

.. toctree::
   :maxdepth: 2
   :caption: Index

   genindex
   search

