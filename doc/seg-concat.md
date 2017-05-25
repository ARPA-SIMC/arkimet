# concat segments

This kind of segment stores all data in a single file, simply concatenated one
after the other.

It is suitable for formats whose workflow naturally concatenates data, such as
GRIB or BUFR.

The file name represents the period of time stored in the segment. The file
extension represents the format of the data it contains.

## Optional padding

For VM2 data, which is a line-based format, the segment supports padding data
with a newline. The newline is not considered part of the data, and it still
gets written, skipped on read, and not considered as a gap during checks.

## Check and repack

### During check

 - no pair of `(offset, size)` data spans from the index can overlap
 - file size must be exactly the same as `offset+size` for the data in the
   index with the highest offset for this segment
 - all the data in the index must cover the whole file, without holes
 - all data known by the index for this segment must be present on disk

### Optional thorough check

 - run format-specific consistency checks on the content of each file
### During fix

 - data files present on disk but not in the index are rescanned and added to
   the index
 - data files present in the index but not on disk are removed from the index
### During repack

 - data files present on disk but not in the index are deleted from the disk
 - files older than `delete age` are removed
 - files older than `archive age` are moved to `last/`
 - data files present in the index but not on disk are removed from the index
 - if the order in the data file does not match the order required from the
   index, data files are rewritten rearranging the data, and the offsets in the
   index are updated accordingly. This is done to guarantee linear disk access
   when data are queried in the default sorting order.
