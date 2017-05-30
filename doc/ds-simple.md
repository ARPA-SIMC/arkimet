# simple datasets

This dataset contains [segments](segments.md) with minimal indexing.

It is useful for long-term storage of data, due to the small metadata overhead
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

## Check and repack on concat segments

### During check

- the segment must exist
- all data known by the index for this segment must be present on disk
- the segment must be a file
- `.metadata` file must not be empty
- `.metadata` file must not be older than the data
- `.summary` file must not be older than the `.metadata` file
- metadata in the `.metadata` file must contain reference time elements
- the span of reference times in each segment must fit inside the interval
  implied by the segment file name (FIXME: should this be disabled for
  archives, to deal with datasets that had a change of step in their lifetime?)
- the segment name must represent an interval matching the dataset step
  (FIXME: should this be disabled for archives, to deal with datasets that had
  a change of step in their lifetime?)

### During fix
- if the `.metadata` file does not exist or is older than the segment, the
  segment data are rescanned to regenerate the `.metadata` file.
- if the `.summary` file does not exist or is older than the `.metadata` file,
  it is regenerated with the contents of the `.metadata` file.
- if the `.metadata` file is newer than the `MANIFEST` file, its information
  is updated inside the `MANIFEST` file.


### During repack

- if the segment contents are changed during segment repack, the
  `.metadata` file is rewritten to match the new contents. The `.summary` and
  `MANIFEST` files are updated accordingly.


## Check and repack on dir segments

### During check

- the segment must exist
- all data known by the index for this segment must be present on disk
- the segment must be a directory
- `.metadata` file must not be empty
- `.metadata` file must not be older than the data
- `.summary` file must not be older than the `.metadata` file
- metadata in the `.metadata` file must contain reference time elements
- the span of reference times in each segment must fit inside the interval
  implied by the segment file name (FIXME: should this be disabled for
  archives, to deal with datasets that had a change of step in their lifetime?)
- the segment name must represent an interval matching the dataset step
  (FIXME: should this be disabled for archives, to deal with datasets that had
  a change of step in their lifetime?)

### During fix
- if the `.metadata` file does not exist or is older than the segment, the
  segment data are rescanned to regenerate the `.metadata` file.
- if the `.summary` file does not exist or is older than the `.metadata` file,
  it is regenerated with the contents of the `.metadata` file.
- if the `.metadata` file is newer than the `MANIFEST` file, its information
  is updated inside the `MANIFEST` file.


### During repack

- if the segment contents are changed during segment repack, the
  `.metadata` file is rewritten to match the new contents. The `.summary` and
  `MANIFEST` files are updated accordingly.