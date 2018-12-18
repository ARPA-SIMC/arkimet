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
- the segment must exist [missing]
- an empty segment not known by the index must be considered deleted [deleted]
- data files not known by a valid index are considered data files whose
  entire content has been removed [deleted]
- segments that contain some data that has been removed are
  identified as to be repacked [dirty]
- segments that only contain data that has been removed are
  identified as fully deleted [deleted]
- all data known by the index for this segment must be present on disk [corrupted]
- no pair of (offset, size) data spans from the index can overlap [corrupted]
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
- segments not known by the index, but when the index is either
  missing, older than the file, or marked as needing checking, are marked
  for reindexing instead of deletion [unaligned]
- if the index has been deleted, accessing the dataset recreates it
  empty, and creates a `needs-check-do-not-pack` file in the root of
  the dataset.
    
- while `needs-check-do-not-pack` is present, files with gaps are
  marked for rescanning instead of repacking. This prevents a scenario
  in which, after the index has been deleted, and some data has been
  imported that got appended to an existing segment, that segment would
  be considered as needing repack instead of rescan. [unaligned]

### During --accurate check

- format-specific consistency checks on the content of each file must pass [corrupted]

### During fix

- [deleted] segments are left untouched
- [dirty] segments are not touched
- [unaligned] segments are imported in-place
- [missing] segments are removed from the index
- [corrupted] segments can only be fixed by manual intervention. They
  are reported and left untouched
- [archive age] segments are not touched
- [delete age] segments are not touched

### During repack

- [deleted] segments are removed from disk
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
- [unaligned] when `needs-check-do-not-pack` is present in the dataset
  root directory, running a repack fails asking to run a check first,
  to prevent deleting data that should be reindexed instead


## Check and repack on dir segments

### During check

- the segment must be a directory [unaligned]
- the size of each data file must match the data size exactly [corrupted]
- the modification time of a directory segment can vary unpredictably,
  so it is ignored. The modification time of the sequence file is used
  instead.
- if arkimet is interrupted during rollback of an append operation on a
  dir dataset, there are files whose name has a higher sequence number
  than the sequence file. This will break further appends, and needs to
  be detected and fixed. [unaligned]
- the segment must exist [missing]
- an empty segment not known by the index must be considered deleted [deleted]
- data files not known by a valid index are considered data files whose
  entire content has been removed [deleted]
- segments that contain some data that has been removed are
  identified as to be repacked [dirty]
- segments that only contain data that has been removed are
  identified as fully deleted [deleted]
- all data known by the index for this segment must be present on disk [corrupted]
- no pair of (offset, size) data spans from the index can overlap [corrupted]
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
- segments not known by the index, but when the index is either
  missing, older than the file, or marked as needing checking, are marked
  for reindexing instead of deletion [unaligned]
- if the index has been deleted, accessing the dataset recreates it
  empty, and creates a `needs-check-do-not-pack` file in the root of
  the dataset.
    
- while `needs-check-do-not-pack` is present, files with gaps are
  marked for rescanning instead of repacking. This prevents a scenario
  in which, after the index has been deleted, and some data has been
  imported that got appended to an existing segment, that segment would
  be considered as needing repack instead of rescan. [unaligned]

### During --accurate check

- format-specific consistency checks on the content of each file must pass [corrupted]

### During fix

- [unaligned] fix low sequence file value by setting it to the highest
  sequence number found.
- [unaligned] fix low sequence file value by setting it to the highest
  sequence number found, with one file truncated / partly written.
- [deleted] segments are left untouched
- [dirty] segments are not touched
- [unaligned] segments are imported in-place
- [missing] segments are removed from the index
- [corrupted] segments can only be fixed by manual intervention. They
  are reported and left untouched
- [archive age] segments are not touched
- [delete age] segments are not touched

### During repack

- [deleted] segments are removed from disk
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
- [unaligned] when `needs-check-do-not-pack` is present in the dataset
  root directory, running a repack fails asking to run a check first,
  to prevent deleting data that should be reindexed instead
