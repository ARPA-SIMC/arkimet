## arki-testtar

`arki-testtar` is a script with no dependencies besides python3, which creates
a `.tar.gz` file of a dataset, without storing the actual data.

It preserves data file names and sizes, but when untarred, their contents are
replaced with a filesystem hole.

The command can be used to replicate a production dataset into a development
machine, to benchmark query code or reproduce production issues.

Usage:

```
$ arki-testtar --help
usage: arki-testtar [-h] [-x] [-c] [-C ROOT] [--debug] [-v] [--skip-archives]
                    [--years YEARS]
                    src

Create a .tar archive with a mock version of a dataset

positional arguments:
  src              dataset to archive or archive to extract

optional arguments:
  -h, --help       show this help message and exit
  -x, --extract    extract an archive
  -c, --create     create archive (default)
  -C ROOT          root directory to use (default: current directory)
  --debug          debugging output
  -v, --verbose    verbose output
  --skip-archives  skip .archive contents
  --years YEARS    limit data outside .archive to the given years (can be
                   yyyy, yyyy:, :yyyy, or yyyy:yyyy, extremes included)
```

Example:

 * On a production machine, run: `arki-testtar -C /srv/datasets -cv datasetname > datasetname.tar.gz`
 * Copy `datasetname.tar.gz` to a development machine
 * Extract it with `arki-testtar -xv datasetname.tar.gz`

Now datasetname can be queried normally. `arki-check --accurate` and
`arki-check` commands which require a data rescan will of course fail, since
the data will appear as zeroes.

On a filesystem which supports holes, disk usage will be that of arkimet
metadata and indices *only*.


## Fetching significant production datasets (ARPA-specific)

- For GRIB: `ssh arkiope arki-testtar -C /arkivio/arkimet/dataset/ -cv cosmo_5M_ita | arki-testtar -xv /dev/stdin`
- For BUFR: `ssh arkiope arki-testtar --skip-archives -C /arkivio/arkimet/dataset/ -cv gts_synop | arki-testtar -xv /dev/stdin`
- For ODIM: `ssh arkiope arki-testtar -C /arkivio/arkimet/dataset/ -cv odimGAT | arki-testtar -xv /dev/stdin`
- For VM2: `ssh arkioss arki-testtar --year-limit=2021: -C /arkivio/arkimet/dataset/ -cv locali | arki-testtar -xv /dev/stdin`


## Building with profiling

Using [meson](https://mesonbuild.com/) there is a `profiling` option which can
be set to `none`, `gprof`, and `gperftools`.

Setting it to a value other than `none` generates executables under
`build-profile/src` with a name starting with `bench-*`, that can be used as
test programs without involving Python:

* `bench-arki-query [matcher] [dataset_path]`: runs the given query on the given
  dataset, counting results. This uses indices and builds output metadata, but
  does not access the data
* `bench-arki-query-data [matcher] [dataset_path]`: runs the given query on the
  given dataset, piping the resulting data to standard output. This accesses
  the data.
* `bench-arki-query-postproc [matcher] [dataset_path]`: runs the given query on
  the given dataset, postprocessed using `cat` as a postprocessor. This
  accesses the data and the postprocessor management code.


## Profiling with gprof

Building:

```
meson setup -Dprofiling=gperf build-profile
ninja -C build-profile
```

Running:

```
$ build-profile/src/bench-arki-query '' datasets/cosmo_5M_ita
448663 elements matched.
# gprof build-profile/src/bench-arki-query gmon.out 
```


## Profiling with gperftools

Building:

```
meson setup -Dprofiling=gperftools build-profile
ninja -C build-profile
```

Running:

```
$ build-profile/src/bench-arki-query '' bench/cosmo_5M_ita
PROFILE: interrupts/evictions/bytes = 820/138/86792
448663 elements matched.
$ google-pprof --callgrind build-profile/src/bench-arki-query bench-arki-query.log > bench-arki-query.callgrind
$ kcachegrind bench-arki-query.callgrind
```


## Profiling with valgrind

Building:

```
meson setup -Dprofiling=valgrind build-profile
ninja -C build-profile
```

This builds everything normally, and also builds the `bench-*` commands.


Running (you want to restrict the query a bit, as things run significantly slower):

```
$ valgrind --tool=callgrind build-profile/src/bench-arki-query '' bench/cosmo_5M_ita
448663 elements matched.
$ kcachegrind callgrind.out.*
```
