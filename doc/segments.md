# Data segments

Most arkimet [datasets](datasets.md) are organised as a collection of segments,
where each segment contains data spanning a given time period.

Arkimet supports various types of segments:

 * [`concat` segments](seg-concat.md)
 * [`dir` segments](seg-dir.md)
 * [`tar` segments](seg-tar.md)
 * [`zip` segments](seg-zip.md)
 * [`gz` and `gzidx` segments](seg-gz.md)

A segment type is selected automatically according to the requirements of the
data format stored in the dataset, and it is possible to force a dataset to use
a `dir` segment by configuring it as `segments = dir`.

Segments can be converted to `tar`, `zip`, and `gz`/`gzidx` segments via
`arki-check` `--tar`, `--zip`, and `--compress` options.
