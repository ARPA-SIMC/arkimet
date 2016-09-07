# New in version 1.0

 *  Removed dependency on libwibble

 *  Minimal python3 bindings, and arki-server rewritten in python

 *  Implemented sharded datasets: in a dataset configuration, use
    `shard=yearly`, `shard=monthly`, or `shard=weekly`, to have the dataset
    create subdatasets where data will be saved. Locking will only happen at
    the subdataset level.

    There is no automatic conversion from a non-sharded to a sharded dataset.

 *  Implemented `arki-check --unarchive=segment`, to move a segment from
    `.archives/last` back to the main dataset.

 *  Implemented `arki-check --state` to show the computed state of each segment
    in a dataset.

 *  Removed arki-check --scantest

 *  Simple and ondisk2 datasets now can only contain segments matching the
    dataset step. arki-check will give errors if it finds data files with a name
    that does not fit in the dataset.
    
    Archived data is not affected by this change.
 
 *  Data scanning now only uses "grib" as a format for all grib editions,
    dropping the previous "grib1"/"grib2" distinction.
     
    This has the effect that newly created segments in datasets will have a
    .grib extension where previously they would have had a .grib1 or .grib2
    extension. Existing segments are still supported.

 *  AssignedDataset will not appear among new metadata anymore.
