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


## Check and repack on concat segments

### During check

- the segment must exist [deleted]
- all data known by the index for this segment must be present on disk [unaligned]
- no pair of (offset, size) data spans from the index can overlap [unaligned]
- data must start at the beginning of the segment [dirty]
- there must be no gaps between data in the segment [dirty]
- data must end at the end of the segment [dirty]
- find data files now known by the index [new]
- find segments that can only contain data older than `archive age` days [archive_age]
- find segments that can only contain data older than `delete age` days [delete_age]
- the segment name must represent an interval matching the dataset step
  (FIXME: should this be disabled for archives, to deal with datasets that had
  a change of step in their lifetime?) [corrupted]
- the segment must be a file
- data on disk must match the order of data used by queries [dirty]

### During --accurate check

- format-specific consistency checks on the content of each file must pass [unaligned]

### During fix

- [new] segments are imported in-place
- [unaligned] segments are reimported in-place
- [dirty] segments are not touched
- [deleted] segments are removed from the index
- [archive age] segments are not touched
- [delete age] segments are not touched

### During repack

- [new] segments are deleted
- [unaligned] segments are not touched
- [dirty] segments are rewritten to be without holes and have data in the right order.
  In concat segments, this is done to guarantee linear disk access when
  data are queried in the default sorting order. In dir segments, this
  is done to avoid sequence numbers growing indefinitely for datasets
  with frequent appends and removes.
- [deleted] segments are removed from the index
- [archive age] segments are repacked if needed, then moved to .archive/last
- [delete age] segments are deleted


## Check and repack on dir segments

### During check

- the segment must exist [deleted]
- all data known by the index for this segment must be present on disk [unaligned]
- no pair of (offset, size) data spans from the index can overlap [unaligned]
- data must start at the beginning of the segment [dirty]
- there must be no gaps between data in the segment [dirty]
- data must end at the end of the segment [dirty]
- find data files now known by the index [new]
- find segments that can only contain data older than `archive age` days [archive_age]
- find segments that can only contain data older than `delete age` days [delete_age]
- the segment name must represent an interval matching the dataset step
  (FIXME: should this be disabled for archives, to deal with datasets that had
  a change of step in their lifetime?) [corrupted]
- the segment must be a directory [unaligned]
- the size of each data file must match the data size exactly [corrupted]
- data on disk must match the order of data used by queries [dirty]

### During --accurate check

- format-specific consistency checks on the content of each file must pass [unaligned]

### During fix

- [new] segments are imported in-place
- [unaligned] segments are reimported in-place
- [dirty] segments are not touched
- [deleted] segments are removed from the index
- [archive age] segments are not touched
- [delete age] segments are not touched

### During repack

- [new] segments are deleted
- [unaligned] segments are not touched
- [dirty] segments are rewritten to be without holes and have data in the right order.
  In concat segments, this is done to guarantee linear disk access when
  data are queried in the default sorting order. In dir segments, this
  is done to avoid sequence numbers growing indefinitely for datasets
  with frequent appends and removes.
- [deleted] segments are removed from the index
- [archive age] segments are repacked if needed, then moved to .archive/last
- [delete age] segments are deleted
