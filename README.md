# Arkimet

## Build status

[![Build Status](https://badges.herokuapp.com/travis/ARPA-SIMC/arkimet?branch=master&env=DOCKER_IMAGE=centos:7&label=centos7)](https://travis-ci.org/ARPA-SIMC/arkimet)
[![Build Status](https://badges.herokuapp.com/travis/ARPA-SIMC/arkimet?branch=master&env=DOCKER_IMAGE=fedora:29&label=fedora29)](https://travis-ci.org/ARPA-SIMC/arkimet)
[![Build Status](https://badges.herokuapp.com/travis/ARPA-SIMC/arkimet?branch=master&env=DOCKER_IMAGE=fedora:30&label=fedora30)](https://travis-ci.org/ARPA-SIMC/arkimet)
[![Build Status](https://badges.herokuapp.com/travis/ARPA-SIMC/arkimet?branch=master&env=DOCKER_IMAGE=fedora:rawhide&label=fedorarawhide)](https://travis-ci.org/ARPA-SIMC/arkimet)

[![Build Status](https://copr.fedorainfracloud.org/coprs/simc/stable/package/arkimet/status_image/last_build.png)](https://copr.fedorainfracloud.org/coprs/simc/stable/package/arkimet/)

## Introduction

Arkimet is a set of tools to organize, archive and distribute data files. It
currently supports data in GRIB, BUFR, HDF5 and VM2 formats.

Arkimet manages a set of [datasets](doc/datasets.md), each of which contains
omogeneous data stored in [segments](doc/segments.md). It exploits the
commonalities between the data in a dataset to implement a fast, powerful and
space-efficient indexing system.

When data is ingested into arkimet, it is scanned and annotated with metadata,
such as reference time and product information, then it is dispatched to one of
the datasets configured in the system.

Datasets can be queried with a comprehensive query language, both locally and
over the network using an HTTP-based protocol.

Old data can be archived using an automatic maintenance procedure, and archives
can be taken offline and back online at will.

A summary of offline data is kept online, so that arkimet is able to report
that more data for a query would be available but is currently offline.

Arkimet is Free Software, licensed under the GNU General Public License version
2 or later.

## Features

### General

All arkimet functionality besides metadata extraction and dataset recovery is
file format agnostic.

Data is treated like an opaque, read only binary string, that is never modified
to guarantee integrity.

Data files in the archive are only accessed using append operations, to avoid
the risk of accidentally corrupting existing data.

### Metadata

The extraction of metadata is very flexible, and it can be customized with
Python scripts.

Metadata contains timestamped annotations to track data workflow.

Metadata can be summarised, to represent what data can be found in a big
dataset without needing to access its contents.

Summaries can be shared to build data catalogs.

### Remote access

Remote data access is provided through arki-server, an HTTP server application.

arki-server can serve data from local datasets, as well as from remote datasets
served by other. arki-server instances (this allows, for example, to provide a
single arki-server external front-end to various internal arki-servers in an
organisation).

arki-server can be run behind apache mod-proxy to provide encrypted (SSL) or
authenticated access.

Client data access is done using the featureful libCURL, and can access the
server over SSL or through HTTP proxies.

When performing a query, it is possible to extract only the summary of its
results, as a quick preview before actually transfering the result data.

Postprocessing chains can be provided by the server to transfer only the
postprocessed data (e.g. transferring an average value instead of a large grid
of data).

### Archive

File layout can be customised depending on data volumes (one file per day, one
file per month, etc.)

Each dataset can be configured to index a different set of metadata items, to
provide the best tradeoff between indexing speed, disk space used by the index
and query speed.

Arkimet can detect if a datum already exists in a dataset, and either replace
the old version or refuse to import the new one. It is possible to customize
what metadata fields make data unique in each dataset.

Datasets are self-contained, so it is possible to store them in offline media,
and query them right away as soon as the offline media comes online.

### User interfaces

A powerful and flexible suite of commandline tools allows to easily integrate
arkimet into automated data processing chains in production systems.

arki-server not only allows remote access to the datasets, but it also provides
a low-level, web-based query interface.

ArkiWEB is a web-based front-end to arkimet that provides simple and powerful
browsing and data retrieval for end users.

### Factsheet

The metadata currently supported are:
 * Reference time
 * Origin
 * Product
 * Level
 * Time Range
 * Area
 * Ensemble run information

The formats used to handle metadata are:
 * Arkimet-specific compact binary representation
 * Human-readable, easy to parse YAML representation
 * JSON

The file formats that can be scanned are:
 * WMO GRIB (edition 1 and edition 2), any template can be
   supported via eccodes and Python scripts
 * WMO BUFR (edition 3 and edition 4), via DB-All.e
 * HDF5, currently only used for ODIM radar data
 * VM2 ascii line-based format used by ARPAE-SIMC

The software architecture is mostly format-agnostic, and is built to support
implementing more data formats easily.

The query language supports:

 * Exact or partial match on any types of metadata
 * Advanced matching of reference times:
```
# Open or close intervals
reftime:>=2005
reftime:>=2005,<2007
# Exact years, month, days, hours, etc.
reftime:=2007
reftime:=2007-05
# Time extremes can vary in precision
reftime:>=2005-06-01 12:00
# Also interval of time of day can be specified
reftime:>=12:00,<18:00
# And repetitions during the day
reftime:>=2007-06/6h
reftime:=2007,>=12:00/30m,<18:00
```
 * Geospatial queries
```
area: bbox covers POINT(11 44)
area: bbox intersects LINESTRING(10 43, 10 45, 12 45, 12 43, 10 43)
# It is possible to configure aliases for query fragments, to use in common-queries:
area: italy
```

## Licensing and acquisition

Arkimet is Free Software, licensed under the GNU General Public License version 2 or later.

## Software dependencies

Arkimet should build and run on any reasonably recent Unix system, and has been
tested on Debian, RedHat, Suse Linux and AIX systems.

Python version 3.6 or later is required for the import scripts http://www.lua.org/

SQLite is required for indexed datasets http://www.sqlite.org/ and an embedded
version is included with arkimet and used if the system does not provide one.

The fast LZO compression library is required for saving space-efficient
metadata, and an embedded version is included with arkimet and used if the
system does not provide one.

CURL is optionally required to access remote datasets http://curl.haxx.se/

ECMWF grib api is optionally required for GRIB (edition 1 and 2) support
http://www.ecmwf.int/products/data/software/gribapi.html

DB-All.e version 6.0 or later is optionally required to enable BUFR support.


## Examples

To extract metadata:

	$ arki-scan --yaml --annotate file.grib

To create a summary of the content of many GRIB files:

	$ arki-scan --summary *.grib > summary

To view the contents of the generated summary file:

	$ arki-dump --yaml summary

To select GRIB messages from a file:

	$ arki-scan file.grib | arki-grep --data 'origin:GRIB1,98;reftime:>=2008' > file1.grib

To configure a dataset interactively:

	$ arki-config datasets/name

To dispatch grib messages into datasets:

	$ arki-mergeconf datasets/* > allowed-datasets
	$ arki-scan --dispatch=allowed-datasets file.grib > dispatched.md

To query:

	$ arki-query 'origin:GRIB1,98;reftime:>=2008' datasets/* --data > output.grib

To get a preview of the results without performing the query:

	$ arki-query 'origin:GRIB1,98;reftime:>=2008' datasets/* --summary --yaml --annotate

To export some datasets for remote access:

	$ arki-mergeconf datasets/* > datasets.conf
	$ arki-server --port=8080 datasets.conf

To query a remote dataset:

	$ arki-mergeconf http://host.name > datasets.conf
	$ arki-query 'origin:GRIB1,98;reftime:>=2008' datasets.conf --data
