# ondisk2 datasets

This dataset contains [segments](segments.md) with a global index of each datum
in the dataset.

It can enforce detection of duplicates, enforcing uniqueness on the set of
metadata selected with the `unique` configuration keyword.

It can optimize queries that use metadata selected with the `index`
configuration keyword.

## Dataset layout

At the root of the dataset there is an `index.sqlite` file that maps each datum
to its segment, offset and metadata.

The `.sqlite` file contains additional indices for the metadata listed in the
`unique` configuration value, to do quick duplicate detection, and extra
indices for the metadata listed in the `index` configuration value.

## General check and repack notes

The index is not able to distinguish between segments that have been fully
deleted, and segments that have never been indexed.

To prevent a scenario where deletion of the index followed by a repack would
consider all segments in the dataset as containing deleted data, and therefore
delete them all from disk, when arkimet notices that `index.sqlite` is missing,
it creates a `needs-check-do-not-pack` file at the root of the dataset.

When `needs-check-do-not-pack` is present, data files not known by the index
are marked for rescanning ([unaligned]) instead of for deletion ([deleted]).

The `needs-check-do-not-pack` file is removed by a successful `check`
operation. `repack` refuses to run if `needs-check-do-not-pack` is present.
This is intended to ensure that no repack is run until the index is rebuilt
with a full check and rescan of all the data in the dataset.

## Check and repack on concat segments

### During check

- the segment must be a file
- the segment must exist [deleted]
- segments that only contain data that has been removed are
  identified as not present in the index [new]
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
- data files not known by the index are considered data files whose
  entire content has been removed [deleted]
- if the index has been deleted, accessing the dataset recreates it
  empty, and creates a `needs-check-do-not-pack` file in the root of
  the dataset.
- if a `needs-check-do-not-pack` file is present, segments not known by
  the index are marked for reindexing instead of deletion [unaligned]

### During --accurate check

- format-specific consistency checks on the content of each file must pass [unaligned]

### During fix

- [unaligned] segments are imported in-place
- [dirty] segments are not touched
- [deleted] segments are removed from the index
- [archive age] segments are not touched
- [delete age] segments are not touched

### During repack

- [dirty] segments are rewritten to be without holes and have data in the right order.
  In concat segments, this is done to guarantee linear disk access when
  data are queried in the default sorting order. In dir segments, this
  is done to avoid sequence numbers growing indefinitely for datasets
  with frequent appends and removes.
- [deleted] segments are removed from the index
- [archive age] segments are repacked if needed, then moved to .archive/last
- [delete age] segments are deleted
- [unaligned] when `needs-check-do-not-pack` is present in the dataset
  root directory, running a repack fails asking to run a check first,
  to prevent deleting data that should be reindexed instead


## Check and repack on dir segments

### During check

- the segment must be a directory [unaligned]
- the size of each data file must match the data size exactly [corrupted]
- the segment must exist [deleted]
- segments that only contain data that has been removed are
  identified as not present in the index [new]
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
- data files not known by the index are considered data files whose
  entire content has been removed [deleted]
- if the index has been deleted, accessing the dataset recreates it
  empty, and creates a `needs-check-do-not-pack` file in the root of
  the dataset.
- if a `needs-check-do-not-pack` file is present, segments not known by
  the index are marked for reindexing instead of deletion [unaligned]

### During --accurate check

- format-specific consistency checks on the content of each file must pass [unaligned]

### During fix

- [unaligned] segments are imported in-place
- [dirty] segments are not touched
- [deleted] segments are removed from the index
- [archive age] segments are not touched
- [delete age] segments are not touched

### During repack

- [dirty] segments are rewritten to be without holes and have data in the right order.
  In concat segments, this is done to guarantee linear disk access when
  data are queried in the default sorting order. In dir segments, this
  is done to avoid sequence numbers growing indefinitely for datasets
  with frequent appends and removes.
- [deleted] segments are removed from the index
- [archive age] segments are repacked if needed, then moved to .archive/last
- [delete age] segments are deleted
- [unaligned] when `needs-check-do-not-pack` is present in the dataset
  root directory, running a repack fails asking to run a check first,
  to prevent deleting data that should be reindexed instead
