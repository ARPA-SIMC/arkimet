# simple datasets

This dataset contains [segments](segments.md) with minimal indexing.

It is useful for long-term storage of data, due to the small metadata overhead
and simple structure.

The only query optimization it supports is when querying by a restricted
date-time range.

Because of the lack of a detailed index, `simple` dataset cannot efficiently
detect duplicate data on import, therefore that feature is not implemented.

## Dataset layout

At the root of the dataset there is a `MANIFEST` file that lists the segments
known to the dataset, together with their reference time spans. The MANIFEST
file can be encoded in plain text or in a `.sqlite` database.

For each segment there is an associated `.metadata` file that contains metadata
for all data in the segment. This makes it possible to select data according to
a query, without needing to rescan it every time.

For each segment there is also an associated `.summary` file, that contains a
summary of the data within the segment. This is intended to quickly filter out
segments during a query without needing to scan through all the `.metadata`
file contents, and to support summary queries by merging existing summaries
instead of recomputing them for all data queried.

## General check and repack notes

Since the dataset is intended for long-term archival, repack will never delete
a data segment. All data segments found without a `.metadata` file or next to
an empty `.metadata` file will always be rescanned.


## Check and repack on concat segments

### During check

- the segment must be a file
- the segment must exist [missing]
- all data known by the index for this segment must be present on disk [unaligned]
- no pair of (offset, size) data spans from the index can overlap [unaligned]
- data must start at the beginning of the segment [dirty]
- there must be no gaps between data in the segment [dirty]
- data must end at the end of the segment [dirty]
- find segments that can only contain data older than `archive age` days [archive_age]
- find segments that can only contain data older than `delete age` days [delete_age]
- the span of reference times in each segment must fit inside the interval
  implied by the segment file name (FIXME: should this be disabled for
  archives, to deal with datasets that had a change of step in their lifetime?) [corrupted]
- the segment name must represent an interval matching the dataset step
  (FIXME: should this be disabled for archives, to deal with datasets that had
  a change of step in their lifetime?) [corrupted]
- data on disk must match the order of data used by queries [dirty]
- find data files not known by the index [unaligned]
- the segment must not be newer than the index [unaligned]
- `.metadata` file must not be empty [unaligned]
- `.metadata` file must not be older than the data [unaligned]
- `.summary` file must not be older than the `.metadata` file [unaligned]
- `MANIFEST` file must not be older than the `.metadata` file [unaligned]
- if the index has been deleted, accessing the dataset recreates it
  empty, and a check will rebuild it. Until it gets rebuilt, segments
  not present in the index would not be considered when querying the
  dataset
    
- metadata in the `.metadata` file must contain reference time elements [corrupted]

### During --accurate check

- format-specific consistency checks on the content of each file must pass [unaligned]

### During fix

- [dirty] segments are not touched
- [unaligned] segments are imported in-place
- [missing] segments are removed from the index
- [corrupted] segments can only be fixed by manual intervention. They
  are reported and left untouched
- [archive age] segments are not touched
- [delete age] segments are not touched

### During repack

- [dirty] segments are rewritten to be without holes and have data in the right order.
  In concat segments, this is done to guarantee linear disk access when
  data are queried in the default sorting order. In dir segments, this
  is done to avoid sequence numbers growing indefinitely for datasets
  with frequent appends and removes.
- [missing] segments are removed from the index
- [corrupted] segments are not touched
- [archive age] segments are repacked if needed, then moved to .archive/last
- [delete age] segments are deleted
- [delete age] [dirty] a segment that needs to be both repacked and
  deleted, gets deleted without repacking
- [archive age] [dirty] a segment that needs to be both repacked and
  archived, gets repacked before archiving
- [unaligned] segments are not touched


## Check and repack on dir segments

### During check

- the segment must be a directory [unaligned]
- the size of each data file must match the data size exactly [corrupted]
- the modification time of a directory segment can vary unpredictably,
  so it is ignored. The modification time of the sequence file is used
  instead.
- the segment must exist [missing]
- all data known by the index for this segment must be present on disk [unaligned]
- no pair of (offset, size) data spans from the index can overlap [unaligned]
- data must start at the beginning of the segment [dirty]
- there must be no gaps between data in the segment [dirty]
- data must end at the end of the segment [dirty]
- find segments that can only contain data older than `archive age` days [archive_age]
- find segments that can only contain data older than `delete age` days [delete_age]
- the span of reference times in each segment must fit inside the interval
  implied by the segment file name (FIXME: should this be disabled for
  archives, to deal with datasets that had a change of step in their lifetime?) [corrupted]
- the segment name must represent an interval matching the dataset step
  (FIXME: should this be disabled for archives, to deal with datasets that had
  a change of step in their lifetime?) [corrupted]
- data on disk must match the order of data used by queries [dirty]
- find data files not known by the index [unaligned]
- the segment must not be newer than the index [unaligned]
- `.metadata` file must not be empty [unaligned]
- `.metadata` file must not be older than the data [unaligned]
- `.summary` file must not be older than the `.metadata` file [unaligned]
- `MANIFEST` file must not be older than the `.metadata` file [unaligned]
- if the index has been deleted, accessing the dataset recreates it
  empty, and a check will rebuild it. Until it gets rebuilt, segments
  not present in the index would not be considered when querying the
  dataset
    
- metadata in the `.metadata` file must contain reference time elements [corrupted]

### During --accurate check

- format-specific consistency checks on the content of each file must pass [unaligned]

### During fix

- [dirty] segments are not touched
- [unaligned] segments are imported in-place
- [missing] segments are removed from the index
- [corrupted] segments can only be fixed by manual intervention. They
  are reported and left untouched
- [archive age] segments are not touched
- [delete age] segments are not touched

### During repack

- [dirty] segments are rewritten to be without holes and have data in the right order.
  In concat segments, this is done to guarantee linear disk access when
  data are queried in the default sorting order. In dir segments, this
  is done to avoid sequence numbers growing indefinitely for datasets
  with frequent appends and removes.
- [missing] segments are removed from the index
- [corrupted] segments are not touched
- [archive age] segments are repacked if needed, then moved to .archive/last
- [delete age] segments are deleted
- [delete age] [dirty] a segment that needs to be both repacked and
  deleted, gets deleted without repacking
- [archive age] [dirty] a segment that needs to be both repacked and
  archived, gets repacked before archiving
- [unaligned] segments are not touched