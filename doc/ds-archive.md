# dataset archives

Datasets can distinguish between online and archived data stored in two
different formats.

Online data can be stored in a format with detailed indexing like
[iseg](ds-iseg.md) or [ondisk2](ds-ondisk2.md), while archived data can be
stored in a format with less features and overhead like [simple](ds-simple.md).

Archives are stored in the `.archives` subdirectory of a dataset. The `archive
age` configuration value can be used to automate moving to `.archives/`, during
repack, those segments that only contain data older than the given age.

Data is moved to `.archives` as entire segments after checking and repacking of
the online part. This should ensure that archived segments contain no
duplicates and need no further repacking.

The `.archives` directory contain several subdirectory, each with a dataset in
[simple](ds-simple.md) format containing data. Queries on the archives query
each dataset in sequence, ordered by name, with `last` always kept at the end.

Segments that get archived are moved from the online dataset to the
`.archives/last` dataset, and can be manually moved from `last` to any other
subdirectory of `.archives`. arkimet will only ever move segments to
`.archives/last` and the rest can be maintained with procedures external to
arkimet with no interference.

## Online archives

Online archives are the default type of archive that is created automatically
as `.archives/last` during maintenance. They are instances of
[simple](ds-simple.md) datasets, and all the simple dataset documentation
applies to them.

## Offline archives

Offline archives are archives whose data has been moved to external media. The
only thing that is left is a .summary file that describes the data that would
be there.
