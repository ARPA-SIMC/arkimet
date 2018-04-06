## Process this file with automake to produce Makefile.in

SUBDIRS = .

AM_CPPFLAGS = -I$(top_srcdir) -I$(top_builddir) $(SQLITE3_CFLAGS) -DCONF_DIR=\"$(confdir)\" -DPOSTPROC_DIR=\"$(postprocdir)\" $(ZLIB_CFLAGS) $(LIBARCHIVE_CFLAGS)
if FILE_OFFSET_BITS_64
AM_CPPFLAGS += -D_FILE_OFFSET_BITS=64
endif
AM_CXXFLAGS = $(EXTRA_CXXFLAGS) $(DEVEL_CXXFLAGS)
AM_LDFLAGS = $(EXTRA_LDFLAGS)


arkiincludedir = $(includedir)/arki

nobase_dist_arkiinclude_HEADERS = \
		wibble/sys/process.h \
		wibble/sys/childprocess.h \
		wibble/sys/exec.h \
		wibble/sys/signal.h \
		core/fwd.h \
		core/time.h \
		core/fuzzytime.h \
		core/file.h \
		core/tests.h \
		binary.h \
		defs.h \
		exceptions.h \
		bbox.h \
		configfile.h \
		libconfig.h \
		dataset/fwd.h \
		dataset/lock.h \
		dataset/reporter.h \
		dataset/time.h \
		dataset/archive.h \
		dataset/segment.h \
		dataset/segment/managers.h \
		dataset/empty.h \
		dataset/testlarge.h \
		dataset/file.h \
		dataset/gridquery.h \
		dataset.h \
		dataset/index.h \
		dataset/index/attr.h \
		dataset/index/base.h \
		dataset/index/aggregate.h \
		dataset/index/summarycache.h \
		dataset/index/manifest.h \
		dataset/local.h \
		dataset/segmented.h \
		dataset/indexed.h \
		dataset/maintenance.h \
		dataset/memory.h \
		dataset/merged.h \
		dataset/ondisk2.h \
		dataset/ondisk2/index.h \
		dataset/ondisk2/reader.h \
		dataset/ondisk2/test-utils.h \
		dataset/ondisk2/writer.h \
		dataset/ondisk2/checker.h \
		dataset/outbound.h \
		dataset/offline.h \
		dataset/simple.h \
		dataset/simple/reader.h \
		dataset/simple/writer.h \
		dataset/simple/checker.h \
		dataset/iseg.h \
		dataset/iseg/index.h \
		dataset/iseg/reader.h \
		dataset/iseg/writer.h \
		dataset/iseg/checker.h \
		dataset/step.h \
		dataset/tests.h \
		dataset/maintenance-test.h \
		datasets.h \
		dispatcher.h \
		emitter.h \
		emitter/json.h \
		emitter/memory.h \
		formatter.h \
		validator.h \
		iotrace.h \
		itemset.h \
		matcher/area.h \
		matcher.h \
		matcher/level.h \
		matcher/origin.h \
		matcher/proddef.h \
		matcher/product.h \
		matcher/quantity.h \
		matcher/reftime.h \
		matcher/reftime/parser.h \
		matcher/run.h \
		matcher/source.h \
		matcher/task.h \
		matcher/tests.h \
		matcher/timerange.h \
		matcher/utils.h \
		metadata.h \
		metadata/collection.h \
		metadata/consumer.h \
		metadata/clusterer.h \
		metadata/xargs.h \
		metadata/libarchive.h \
		metadata/fwd.h \
		metadata/stream.h \
		metadata/tests.h \
		metadata/test-generator.h \
		metadatagrid.h \
		reader.h \
		nag.h \
		postprocess.h \
		querymacro.h \
		runtime/config.h \
		runtime/inputs.h \
		runtime.h \
		runtime/io.h \
		runtime/processor.h \
		runtime/tests.h \
		runtime/arki-query.h \
		runtime/arki-check.h \
		runtime/arki-scan.h \
		scan/base.h \
		scan/any.h \
		segment.h \
		segment/fwd.h \
		segment/fd.h \
		segment/concat.h \
		segment/lines.h \
		segment/dir.h \
		segment/tar.h \
		segment/gz.h \
		segment/gzidx.h \
		segment/seqfile.h \
		segment/tests.h \
		sort.h \
		summary.h \
		summary/intern.h \
		summary/table.h \
		summary/codec.h \
		summary/stats.h \
		summary/short.h \
		targetfile.h \
		tests/tests.h \
		transaction.h \
		types/fwd.h \
		types/area.h \
		types/assigneddataset.h \
		types/bbox.h \
		types.h \
		types-init.h \
		types/level.h \
		types/note.h \
		types/origin.h \
		types/proddef.h \
		types/product.h \
		types/quantity.h \
		types/reftime.h \
		types/run.h \
		types/source.h \
		types/source/blob.h \
		types/source/inline.h \
		types/source/url.h \
		types/task.h \
		types.tcc \
		types/tests.h \
		types/timerange.h \
		types/utils.h \
		types/value.h \
		types/typevector.h \
		types/typeset.h \
		utils/accounting.h \
		utils/gzip.h \
		utils/compress.h \
		utils/files.h \
		utils/geos.h \
		utils/geosfwd.h \
		utils/h5.h \
		utils.h \
		utils/process.h \
		utils/regexp.h \
		utils/sqlite.h \
		utils/string.h \
		utils/sys.h \
		utils/yaml.h \
		utils/tar.h \
		utils/commandline/core.h \
		utils/commandline/doc.h \
		utils/commandline/engine.h \
		utils/commandline/options.h \
		utils/commandline/parser.h \
		utils/commandline/wordwrap.h \
		utils/tests.h \
		values.h

reftime_parser_built_sources = \
		matcher/reftime/reftime-lex.h \
		matcher/reftime/reftime-lex.cc \
		matcher/reftime/reftime-parse.hh \
		matcher/reftime/reftime-parse.cc

BUILT_SOURCES = $(reftime_parser_built_sources)

%-lex.h %-lex.cc: %-lex.ll
	$(srcdir)/buildflex $<

%-parse.hh %-parse.cc: %-parse.yy
	$(srcdir)/buildbison $<

lib_LTLIBRARIES = libarkimet.la

noinst_LTLIBRARIES = libarkimet-test.la

# Build these incrementally
libarkimet_la_CPPFLAGS = $(AM_CPPFLAGS) -Werror
libarkimet_la_SOURCES =
libarkimet_la_LDFLAGS = -version-info @LIBARKI_VERSION_INFO@ $(SQLITE3_LDFLAGS) $(ZLIB_LIBS) $(LIBARCHIVE_LIBS) $(DEVEL_LIBS) -lpthread
libarkimet_test_la_SOURCES = tests/main.cpp
tests_arki_test_SOURCES =
tests_arki_test_LDADD =


#
# Core functions
#
if DEVEL_DO_TYPES
libarkimet_la_SOURCES += \
		wibble/sys/process.cpp \
		wibble/sys/childprocess.cpp \
		wibble/sys/exec.cpp \
		wibble/sys/signal.cpp \
		core/time.cc \
		core/fuzzytime.cc \
		core/file.cc \
		exceptions.cc \
		nag.cc \
		utils.cc \
		utils/accounting.cc \
		utils/geos.cc \
		utils/gzip.cc \
		utils/files.cc \
		utils/process.cc \
		utils/regexp.cc \
		utils/string.cc \
		utils/sys.cc \
		utils/yaml.cc \
		utils/tar.cc \
		utils/commandline/core.cpp \
		utils/commandline/options.cpp \
		utils/commandline/doc.cpp \
		utils/commandline/parser.cpp \
		utils/commandline/engine.cpp \
		utils/commandline/wordwrap.cpp \
		binary.cc \
		configfile.cc \
		transaction.cc \
		emitter.cc \
		emitter/memory.cc \
		emitter/json.cc \
		values.cc \
		bbox.cc \
		types.cc \
		types/utils.cc \
		types/source.cc \
		types/source/blob.cc \
		types/source/inline.cc \
		types/source/url.cc \
		types/reftime.cc \
		types/origin.cc \
		types/product.cc \
		types/level.cc \
		types/timerange.cc \
		types/area.cc \
		types/proddef.cc \
		types/note.cc \
		types/assigneddataset.cc \
		types/bbox.cc \
		types/run.cc \
		types/task.cc \
		types/quantity.cc \
		types/value.cc \
		types/typevector.cc \
		types/typeset.cc \
		itemset.cc \
		matcher.cc \
		matcher/utils.cc \
		matcher/origin.cc \
		matcher/product.cc \
		matcher/level.cc \
		matcher/timerange.cc \
		matcher/reftime.cc \
		matcher/reftime/parser.cc \
		matcher/reftime/reftime-lex.cc \
		matcher/reftime/reftime-parse.cc \
		matcher/area.cc \
		matcher/proddef.cc \
		matcher/run.cc \
		matcher/source.cc \
		matcher/task.cc \
		matcher/quantity.cc \
		runtime/config.cc \
		runtime/inputs.cc \
		runtime/io.cc \
		runtime/tests.cc \
		types-init.cc

libarkimet_test_la_SOURCES += \
		tests/tests.cc \
		core/tests.cc \
		types/tests.cc \
		utils/tests.cc

tests_arki_test_SOURCES += \
		tests/tests-test.cc \
		core/time-test.cc \
		core/fuzzytime-test.cc \
		core/file-test.cc \
		utils-test.cc \
		utils/accounting-test.cc \
		utils/gzip-test.cc \
		utils/yaml-test.cc \
		utils/tar-test.cc \
		utils/files-test.cc \
		utils/process-test.cc \
		utils/regexp-test.cc \
		utils/geos-test.cc \
		binary-test.cc \
		configfile-test.cc \
		transaction-test.cc \
		emitter/memory-test.cc \
		emitter/json-test.cc \
		values-test.cc \
		types-test.cc \
		types/utils-test.cc \
		types/source-test.cc \
		types/reftime-test.cc \
		types/origin-test.cc \
		types/product-test.cc \
		types/level-test.cc \
		types/timerange-test.cc \
		types/area-test.cc \
		types/proddef-test.cc \
		types/note-test.cc \
		types/assigneddataset-test.cc \
		types/bbox-test.cc \
		types/run-test.cc \
		types/task-test.cc \
		types/quantity-test.cc \
		types/value-test.cc \
		types/typevector-test.cc \
		types/typeset-test.cc \
		itemset-test.cc

if IOTRACE
libarkimet_la_SOURCES += iotrace.cc
tests_arki_test_SOURCES += iotrace-test.cc
endif

if LZO
AM_CPPFLAGS += $(LZO_CFLAGS)
libarkimet_la_LDFLAGS += $(LZO_LIBS)
endif

if LUA
nobase_dist_arkiinclude_HEADERS += report.h formatter/lua.h utils/lua.h tests/lua.h
libarkimet_la_SOURCES += formatter/lua.cc utils/lua.cc
tests_arki_test_SOURCES += formatter/lua-test.cc utils/lua-test.cc tests/lua.cc
AM_CPPFLAGS += $(LUA_CFLAGS)
libarkimet_la_LDFLAGS += $(LUA_LIBS)
tests_arki_test_LDADD += $(LUA_LIBS)
endif

if GEOS
AM_CPPFLAGS += $(GEOS_CFLAGS)
libarkimet_la_LDFLAGS += $(GEOS_LIBS)
endif

if VM2
AM_CPPFLAGS += $(VM2_CFLAGS)
nobase_dist_arkiinclude_HEADERS += scan/vm2.h utils/vm2.h
libarkimet_la_SOURCES += utils/vm2.cc
libarkimet_la_LDFLAGS += $(VM2_LIBS)
tests_arki_test_SOURCES += utils/vm2-test.cc
endif

endif

#
# Metadata types
#
if DEVEL_DO_METADATA
libarkimet_la_SOURCES += \
		utils/h5.cc \
		utils/compress.cc \
		utils/sqlite.cc \
		metadata.cc \
		metadata/collection.cc \
		metadata/stream.cc \
		metadata/clusterer.cc \
		metadata/consumer.cc \
		metadata/libarchive.cc \
		metadata/test-generator.cc \
		reader.cc \
		summary/stats.cc \
		summary/intern.cc \
		summary/table.cc \
		summary/codec.cc \
		summary/short.cc \
		summary.cc \
		segment.cc \
		segment/fd.cc \
		segment/concat.cc \
		segment/lines.cc \
		segment/dir.cc \
		segment/tar.cc \
		segment/gz.cc \
		segment/gzidx.cc \
		segment/seqfile.cc \
		sort.cc \
		formatter.cc \
		validator.cc \
		postprocess.cc \
		scan/base.cc \
		scan/any.cc

tests_arki_test_SOURCES += \
		metadata/tests.cc \
		matcher/tests.cc \
		utils/compress-test.cc \
		utils/sqlite-test.cc \
		sort-test.cc \
		formatter-test.cc \
		validator-test.cc \
		bbox-test.cc \
		metadata-test.cc \
		metadata/collection-test.cc \
		metadata/clusterer-test.cc \
		metadata/stream-test.cc \
		metadata/libarchive-test.cc \
		metadata/test-generator-test.cc \
		reader-test.cc \
		summary-test.cc \
		summary/intern-test.cc \
		summary/table-test.cc \
		summary/stats-test.cc \
		summary/short-test.cc \
		matcher-test.cc \
		matcher/utils-test.cc \
		matcher/origin-test.cc \
		matcher/product-test.cc \
		matcher/level-test.cc \
		matcher/timerange-test.cc \
		matcher/reftime-test.cc \
		matcher/reftime/parser-test.cc \
		matcher/area-test.cc \
		matcher/proddef-test.cc \
		matcher/run-test.cc \
		matcher/source-test.cc \
		matcher/task-test.cc \
		matcher/quantity-test.cc \
		scan/any-test.cc \
		segment-test.cc \
		segment/fd-test.cc \
		segment/concat-test.cc \
		segment/lines-test.cc \
		segment/seqfile-test.cc \
		segment/dir-test.cc \
		segment/tar-test.cc \
		segment/gz-test.cc \
		segment/gzidx-test.cc \
		segment/tests.cc \
		runtime/io-test.cc \
		runtime/inputs-test.cc \
		runtime/config-test.cc

if GRIBAPI
nobase_dist_arkiinclude_HEADERS += scan/grib.h
libarkimet_la_SOURCES += scan/grib.cc
tests_arki_test_SOURCES += scan/grib-test.cc
AM_CPPFLAGS += $(GRIBAPI_CFLAGS) $(LUA_CFLAGS)
libarkimet_la_LDFLAGS += $(GRIBAPI_LIBS) $(LUA_LIBS)
endif

if DBALLE
nobase_dist_arkiinclude_HEADERS += scan/bufr.h
libarkimet_la_SOURCES += scan/bufr.cc
libarkimet_la_LDFLAGS += $(DBALLE_LIBS)
AM_CPPFLAGS += $(DBALLE_CFLAGS)
tests_arki_test_SOURCES += scan/bufr-test.cc
if LUA
nobase_dist_arkiinclude_HEADERS += scan/bufrlua.h
libarkimet_la_SOURCES += scan/bufrlua.cc
endif
endif

if HDF5
AM_CPPFLAGS += $(HDF5_CFLAGS)
nobase_dist_arkiinclude_HEADERS += scan/odimh5.h
libarkimet_la_SOURCES += scan/odimh5.cc
libarkimet_la_LDFLAGS += $(HDF5_LIBS)
tests_arki_test_SOURCES += utils/h5-test.cc scan/odimh5-test.cc
tests_arki_test_LDADD += $(HDF5_LIBS)
endif

if VM2
libarkimet_la_SOURCES += scan/vm2.cc
tests_arki_test_SOURCES += scan/vm2-test.cc
endif

endif

#
# Datasets
#
if DEVEL_DO_DATASETS
libarkimet_la_SOURCES += \
		metadata/xargs.cc \
		targetfile.cc \
		dataset.cc \
		dataset/lock.cc \
		dataset/reporter.cc \
		dataset/time.cc \
		dataset/segment.cc \
		dataset/segment/managers.cc \
		dataset/maintenance.cc \
		dataset/memory.cc \
		dataset/index.cc \
		dataset/index/base.cc \
		dataset/index/attr.cc \
		dataset/index/manifest.cc \
		dataset/index/aggregate.cc \
		dataset/index/summarycache.cc \
		dataset/step.cc \
		dataset/local.cc \
		dataset/segmented.cc \
		dataset/indexed.cc \
		dataset/file.cc \
		dataset/archive.cc \
		dataset/simple.cc \
		dataset/simple/reader.cc \
		dataset/simple/writer.cc \
		dataset/simple/checker.cc \
		dataset/iseg.cc \
		dataset/iseg/index.cc \
		dataset/iseg/reader.cc \
		dataset/iseg/writer.cc \
		dataset/iseg/checker.cc \
		dataset/ondisk2.cc \
		dataset/ondisk2/index.cc \
		dataset/ondisk2/reader.cc \
		dataset/ondisk2/writer.cc \
		dataset/ondisk2/checker.cc \
		dataset/outbound.cc \
		dataset/offline.cc \
		dataset/empty.cc \
		dataset/testlarge.cc \
		dataset/merged.cc \
		dataset/gridquery.cc \
		datasets.cc \
		metadatagrid.cc \
		dispatcher.cc \
		querymacro.cc \
		runtime/arki-query.cc \
		runtime/arki-check.cc \
		runtime/arki-scan.cc

tests_arki_test_SOURCES += \
		targetfile-test.cc \
		metadata/xargs-test.cc \
		dataset/tests.cc \
		dataset/lock-test.cc \
		dataset/reporter-test.cc \
		dataset/time-test.cc \
		dataset/segment-test.cc \
		dataset/segment/managers-test.cc \
		dataset/maintenance-test.cc \
		dataset/memory-test.cc \
		dataset/index-test.cc \
		dataset/index/base-test.cc \
		dataset/index/attr-test.cc \
		dataset/index/manifest-test.cc \
		dataset/index/aggregate-test.cc \
		dataset/index/summarycache-test.cc \
		dataset/step-test.cc \
		dataset/local-test.cc \
		dataset/segmented-test.cc \
		dataset/indexed-test.cc \
		dataset/file-test.cc \
		dataset/archive-test.cc \
		dataset/simple-test.cc \
		dataset/simple/reader-test.cc \
		dataset/simple/writer-test.cc \
		dataset/simple/checker-test.cc \
		dataset/simple/maintenance-test.cc \
		dataset/iseg-test.cc \
		dataset/iseg/index-test.cc \
		dataset/iseg/reader-test.cc \
		dataset/iseg/writer-test.cc \
		dataset/iseg/checker-test.cc \
		dataset/iseg/maintenance-test.cc \
		dataset/ondisk2-test.cc \
		dataset/ondisk2/index-test.cc \
		dataset/ondisk2/reader-test.cc \
		dataset/ondisk2/writer-test.cc \
		dataset/ondisk2/checker-test.cc \
		dataset/ondisk2/test-utils.cc \
		dataset/ondisk2/maintenance-test.cc \
		dataset/outbound-test.cc \
		dataset/offline-test.cc \
		dataset/empty-test.cc \
		dataset/testlarge-test.cc \
		dataset/merged-test.cc \
		dataset/gridquery-test.cc \
		dataset-test.cc \
		dataset-reader-test.cc \
		dataset-writer-test.cc \
		dataset-checker-test.cc \
		dataset-concurrency-test.cc \
		metadatagrid-test.cc \
		postprocess-test.cc \
		querymacro-test.cc \
		datasets-test.cc \
		dispatcher-test.cc \
		runtime/processor-test.cc \
		runtime/arki-query-test.cc \
		runtime/arki-check-test.cc \
		runtime/arki-scan-test.cc \
		runtime-test.cc

if CURL
AM_CPPFLAGS += @LIBCURL_CPPFLAGS@
nobase_dist_arkiinclude_HEADERS += dataset/http.h
libarkimet_la_SOURCES += dataset/http.cc
libarkimet_la_LDFLAGS += @LIBCURL@
tests_arki_test_SOURCES += dataset/http-test.cc
endif

if LUA
libarkimet_la_SOURCES += report.cc
tests_arki_test_SOURCES += report-test.cc
endif

endif

#
# Runtime
#
if DEVEL_DO_RUNTIME
libarkimet_la_SOURCES += \
		runtime/processor.cc \
		runtime.cc
endif

if GCOV
export ARKI_HAS_GCOV=yes
endif

# Tests
TESTS_ENVIRONMENT = $(top_srcdir)/arki/runtest
check_PROGRAMS = tests/arki-test
check_DATA = tests/testdata.json

check-local:
	for test in $(check_PROGRAMS); do \
		$(TESTS_ENVIRONMENT) $$test ; \
	done

tests_arki_test_LDADD += libarkimet.la libarkimet-test.la $(SQLITE3_LDFLAGS) $(LIBARCHIVE_LIBS) $(DEVEL_LIBS) -lpthread

tests/testdata.json: tests/arki-test
	$(top_srcdir)/doc/update --testsuite=$< --cache-testsuite=$@

EXTRA_DIST = runtest buildbison buildflex \
	     matcher/reftime/reftime-lex.ll \
	     matcher/reftime/reftime-parse.yy \
	     $(reftime_parser_built_sources) \
	     tests/testdata.json
