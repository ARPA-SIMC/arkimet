# Data segments

Most arkimet [datasets](datasets.md) are organised as a collection of segments,
where each segment contains data spanning a given time period.

Arkimet currently supports two types of segments:

 * [`concat` segments](seg-concat.md)
 * [`dir` segments](seg-dir.md)

A segment type is selected automatically according to the requirements of the
data format stored in the dataset, and it is possible to force a dataset to use
a `dir` segment by configuring it as `segments = dir`.
