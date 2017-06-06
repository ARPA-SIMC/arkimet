# iseg datasets

This dataset contains [segments](segments.md) with an index next to each
segment, with a `.index` extension, containing a SQLite database.

It can enforce detection of duplicates, enforcing uniqueness on the set of
metadata selected with the `unique` configuration keyword.

Duplicate detection relies on the invariant that segment naming produces
segments that do not overlap, so a datum can only be found in a well defined
segment, and duplicate detection can be performed on a per-segment basis.

It can optimize queries that use metadata selected with the `index`
configuration keyword.

## Dataset layout

The `.index` files contain the metadata for all data in the segment.

The `.index` file contains additional indices for the metadata listed in the
`unique` configuration value, to do quick duplicate detection, and extra
indices for the metadata listed in the `index` configuration value.


## Check and repack on concat segments

### During check

- the segment must be a file
- the segment must exist [missing]
- segments that only contain data that has been removed are
  identified as fully deleted [deleted]
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
- data on disk must match the order of data used by queries [dirty]
- find data files not known by the index [unaligned]
- segments found in the dataset without a `.index` file are marked for rescanning [unaligned]

### During --accurate check

- format-specific consistency checks on the content of each file must pass [unaligned]

### During fix

- [dirty] segments are not touched
- [unaligned] segments are imported in-place
- [missing] segments are removed from the index
- [deleted] segments are removed from the index
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
- [deleted] segments are removed from the index
- [corrupted] segments are not untouched
- [archive age] segments are repacked if needed, then moved to .archive/last
- [delete age] segments are deleted
- [unaligned] segments are not touched


## Check and repack on dir segments

### During check

- the segment must be a directory [unaligned]
- the size of each data file must match the data size exactly [corrupted]
- the segment must exist [missing]
- segments that only contain data that has been removed are
  identified as fully deleted [deleted]
- all data known by the index for this segment must be present on disk [unaligned]
- data must start at the beginning of the segment [dirty]
- there must be no gaps between data in the segment [dirty]
- data must end at the end of the segment [dirty]
- find segments that can only contain data older than `archive age` days [archive_age]
- find segments that can only contain data older than `delete age` days [delete_age]
- the span of reference times in each segment must fit inside the interval
  implied by the segment file name (FIXME: should this be disabled for
  archives, to deal with datasets that had a change of step in their lifetime?) [corrupted]
- data on disk must match the order of data used by queries [dirty]
- find data files not known by the index [unaligned]
- segments found in the dataset without a `.index` file are marked for rescanning [unaligned]

### During --accurate check

- format-specific consistency checks on the content of each file must pass [unaligned]

### During fix

- [dirty] segments are not touched
- [unaligned] segments are imported in-place
- [missing] segments are removed from the index
- [deleted] segments are removed from the index
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
- [deleted] segments are removed from the index
- [corrupted] segments are not untouched
- [archive age] segments are repacked if needed, then moved to .archive/last
- [delete age] segments are deleted
- [unaligned] segments are not touched