# dir segments

This kind of segment stores all data inside a directory, with one file per
datum.

It is suitable for formats that do not support contatenation, like ODIMH5, or
for storing data that could be concatenated but is very large, like
high-resolution GRIB files.

Files are numbered sequentially, starting from 0. The name of each file is the
sequence number, and its extension is the format of the data it contains.

The directory also contains a `.sequence` file that is used for generating
sequence numbers when appending to the segment.

The directory name represents the period of time stored in the segment. The
directory extension represents the format of the data it contains.

## Check and repack

### During check

 - the directory must exist
 - the file sequence numbers must start from 0 and there must be no gaps
 - sequence files on disk must match the order of data in the index
 - all data known by the index for this segment must be present on disk

### Optional thorough check

 - run format-specific consistency checks on the content of each file
 - ensure each file contains one and only one datum, in case dir segments are
   used to store data that can be concatenated

### During fix

 - data files present on disk but not in the index are rescanned and added to
   the index
 - data files present in the index but not on disk are removed from the index
### During repack

 - data files present on disk but not in the index are deleted from the disk
 - files older than `delete age` are removed
 - files older than `archive age` are moved to `last/`
 - data files present in the index but not on disk are removed from the index
 - data files are renumbered starting from 0, sequentially without gaps, in the
   order given by the index, or by reference time if there is no index.
