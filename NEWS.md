# New in version 1.27

* reftime match expressions using repeating intervals (like `reftime:=yesterday
  %3h`) now always base the interval or a round time, instead of the time
  specified in the previous part of the query. Where previously something like
  `reftime:>=today 12:30 %6h` would match today at 12:30 and 18:30, then
  tomorrow at 00:30, 06:30, and so on, now it will match today at 18:00,
  tomorrow at 00:00, tomorrow at 06:00, and so on.
  (side effect of fixing #206)
* Added Python bindings for arki.dataset.Session
  (side effect of fixing #206)
* Added Python bindings for arkimet.dataset.Dataset
  (side effect of fixing #206)
* Python code should now instantiate datasets and matchers using Session; the
  old way still works, and raises `DeprecationWarning`s

# New in version 1.26

* Fixed use of geos compile-time version detection (#225)
* Removed make install depending on make check (#218)
* Fix ODIM scanner for volumes with missing 'radhoriz' parameter (#227)

# New in version 1.25

* Fixes for geos ≥ 3.8.0 (#224)

# New in version 1.24

* Fixed return value of arki-query (#221)

# New in version 1.23

 * Added `arki-query --progress`
 * Restored `processing..` message on `arki-query --verbose` (#216)
 * Added a periodic callback during queries, used to check for SIGINT and raising KeyboardError (#210)
 * Removed spurious returns in `Summary.write` (#214)
 * Fixed database queries with empty reftime values (#215)

# New in version 1.22

 * Keep a Sesssion structure which tracks and reuses Segment objects, and as a consequence, file descriptors (#213)
 * Fixed typo in GRIB2 scanner and related GRIB2 test

# New in version 1.21

 * Fixed sorted data queries in python bindings (#212)

# New in version 1.20

 * Fixed hanging of arki-query when using postprocessor (#209)
 * Fixed arki-dump from stdin

# New in version 1.19

 * Fixed level and timerange formatter for GRIB1 and GRIB2 (#207)

# New in version 1.18

 * Ported documentation to sphinx
 * Fixed arki-server
 * arki-web-proxy command

# New in version 1.17

 * Greatly enlarged Python API
 * New HTTP API
 * Redesigned and unified all the documentation (see https://arpa-simc.github.io/arkimet/)
 * Ported command line scripts to python
 * Removed unused gridspace querymacro
 * Removed unused --targetfile option
 * Removed unused --report option
 * Ported all Lua infrastructure and scripts to Python
 * Ported to new dballe 8 API
 * Cleaned up tests and autoconf checks
 * Use POSIX return values from command line tools (see https://www.freebsd.org/cgi/man.cgi?query=sysexits)
 * Work when sendfile is not available
 * ondisk2 datasets are deprecated from now on in favour of iseg, but still fully supported
 * sphinx-based documentation for python bindings
 * Do not stop at reporting the first 'dirty' cause found in a segment (#187)
 * Fixed order of iteration / overriding options of scanners (#177)
 * Fixed --annotate when some timedef keys are missing (#202)
 * Fixed errors in zip utils (#173)
 * Fixed bug on dataset compression (#170)
 * Fail a match on a metadata which does not have the data to match (#166)

# New in version 1.12

 * Check and repack are able to fix an interrupted append to a dir dataset (#156)
 * Fixed writing VM2 as inline metadata (#160)
 * Fixed copyok/copyko empty file creation (#155)
 * Filter input from stdin (#164, #159)
 * Fixed tar option (#162)
 * Restored odim sequence file behaviour (#163)

# New in version 1.9

 * implemented tar/zip segments (`arki-check` options `--tar` and `--zip`) (#131)
 * implemented soft bound on memory usage (default 128Mi, can be modified with `--flush-threshold` option in `arki-scan`, #152)
 * refactoring of stdin input: implemented `--stdin` (not available for `--dispatch`, #126)
 * grib definitions path now reflects environment variable if present
 * `arki-check --remove` now needs a `--fix` to actually delete data
 * Timerange handling for BUFR "generic" #(125)
 * fixed and improved tests (#128) (#139) (#135) (#140) (#151)
 * `arki-xargs` now removes temporary files if present (#124) (#134)
 * `arki-check --state` now look into archives (#95)
 * clarified move options scope in manpage (#146)
 * removed old files from source code (#133)

# New in version 1.6

 * Locking is now disabled by default on file datasets and it's possible to disable it on dataset too using: `locking=no` option (#76)
 * Improved performance of `arki-scan --dispatch` for `iseg` and `ondisk2` datasets (#110)
 * Arkimet can now be built on EcCodes (#121)
 * It's now possible to scan grib2 with different time units (#120)

## Fixes
 * fixed yaml metadata parsing issue (#107)
 * fixed SQLite error with concurrent `arki-check` and `arki-scan` on `iseg` dataset (#115)
 * fixed `ondisk2` and `iseg` datasets ignoring reftime matcher without date (#116)
 * fixed `arki-query` from metadata file (#117)
 * fixed race condition on simple dataset writes (#122)
 * fixed build without geos (#123)

# New in version 1.5

 * Implemented single segment check on `iseg`datasets (#109)
 * Extended `--filter` option for all dataset types for `--state`, `--repack` and `--remove-all` operations (#111)
 * Fixed issue when importing a file with records extending across a number of steps greater than `ulimit -n`, `arki-scan` didn't import the data correctly, because the process has too many open files (#103)
 * Fixed concurrency issue on `iseg` datasets (#108)

# New in version 1.4

 * Better HTTP code handling
 * Added documentation for datasets, segments, checks, fixes and repacks
 * Fixed some inconsistencies in `arki-check` behaviour
 * Test code refactoring
 * Added local arki-server for test suite
 * Other minor bug fixes

# New in version 1.1

 * New dataset format with no dataset-wide index, and one .sqlite index per segment.
   The configuration required is:
   `type = iseg`
   `format = grib|bufr|vm2|odimh5`
   (in order to be able to enumerate the segments of the right kind, they need to 
   be told explicitly what data format they are going to store)

# New in version 1.0

 *  Removed dependency on libwibble

 *  Minimal python3 bindings, and arki-server rewritten in python

 *  Added `arki-server --journald` to format output messages so that they can be
    interpreted by journald

 *  Added `arki-server --perflog` to log json info records for each query

 *  Implemented sharded datasets: in a dataset configuration, use
    `shard=yearly`, `shard=monthly`, or `shard=weekly`, to have the dataset
    create subdatasets where data will be saved. Locking will only happen at
    the subdataset level.

    There is no automatic conversion from a non-sharded to a sharded dataset.

 *  Implemented `arki-check --unarchive=segment`, to move a segment from
    `.archives/last` back to the main dataset.

 *  Implemented `arki-check --state` to show the computed state of each segment
    in a dataset.

 *  Implemented `--copyok` and `--copyko` options for `arki-scan` to copy
    somewhere every successfully imported (or failed) single message (i.e. a
    single grib in a multiple grib input file)

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
