## Process this file with automake to produce Makefile.in

SUBDIRS = .

AM_CPPFLAGS = -I$(top_srcdir) -I$(top_builddir) $(SQLITE3_CFLAGS) -DCONF_DIR=\"$(confdir)\" -DPOSTPROC_DIR=\"$(postprocdir)\" $(ZLIB_CFLAGS) $(LIBARCHIVE_CFLAGS) $(LIBZIP_CFLAGS) $(LIBSSL_CFLAGS)
if FILE_OFFSET_BITS_64
AM_CPPFLAGS += -D_FILE_OFFSET_BITS=64
endif
AM_CXXFLAGS = $(EXTRA_CXXFLAGS) $(DEVEL_CXXFLAGS)
AM_LDFLAGS = $(EXTRA_LDFLAGS)


arkiincludedir = $(includedir)/arki

nobase_dist_arkiinclude_HEADERS = \
		core/fwd.h \
		core/time.h \
		core/fuzzytime.h \
		core/file.h \
		core/cfg.h \
		core/tests.h \
		core/binary.h \
		core/transaction.h \
		core/curl.h \
		stream.h \
		stream/base.h \
		stream/filter.h \
		stream/concrete.h \
		stream/concrete.tcc \
		stream/concrete-parts.tcc \
		stream/discard.h \
		stream/buffer.h \
		stream/tests.h \
		defs.h \
		exceptions.h \
		bbox.h \
		libconfig.h \
		dataset/fwd.h \
		dataset/lock.h \
		dataset/impl.h \
		dataset/query.h \
		dataset/reporter.h \
		dataset/progress.h \
		dataset/time.h \
		dataset/session.h \
		dataset/archive.h \
		dataset/empty.h \
		dataset/fromfunction.h \
		dataset/testlarge.h \
		dataset/file.h \
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
		dataset/querymacro.h \
		dataset/pool.h \
		dispatcher.h \
		structured/emitter.h \
		structured/keys.h \
		structured/reader.h \
		structured/json.h \
		structured/memory.h \
		formatter.h \
		iotrace.h \
		matcher.h \
		matcher/fwd.h \
		matcher/aliases.h \
		matcher/parser.h \
		matcher/area.h \
		matcher/level.h \
		matcher/origin.h \
		matcher/proddef.h \
		matcher/product.h \
		matcher/quantity.h \
		matcher/reftime.h \
		matcher/reftime/lexer.h \
		matcher/reftime/parser.h \
		matcher/run.h \
		matcher/task.h \
		matcher/tests.h \
		matcher/timerange.h \
		matcher/utils.h \
		metadata.h \
		metadata/data.h \
		metadata/collection.h \
		metadata/clusterer.h \
		metadata/xargs.h \
		metadata/archive.h \
		metadata/fwd.h \
		metadata/postprocess.h \
		metadata/sort.h \
		metadata/stream.h \
		metadata/tests.h \
		metadata/test-generator.h \
		metadata/validator.h \
		nag.h \
		runtime.h \
		scan.h \
		scan/fwd.h \
		scan/validator.h \
		scan/mock.h \
		scan/odimh5.h \
		scan/netcdf.h \
		segment.h \
		segment/fwd.h \
		segment/base.h \
		segment/base.tcc \
		segment/common.h \
		segment/missing.h \
		segment/fd.h \
		segment/dir.h \
		segment/tar.h \
		segment/zip.h \
		segment/gz.h \
		segment/seqfile.h \
		segment/tests.h \
		segment/tests.tcc \
		summary.h \
		summary/intern.h \
		summary/table.h \
		summary/codec.h \
		summary/stats.h \
		summary/short.h \
		tests/daemon.h \
		tests/tests.h \
		types/fwd.h \
		types/bundle.h \
		types/area.h \
		types/assigneddataset.h \
		types/bbox.h \
		types.h \
		types-init.h \
		types/encoded.h \
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
		types/tests.h \
		types/timerange.h \
		types/utils.h \
		types/value.h \
		types/typevector.h \
		types/typeset.h \
		types/values.h \
		utils/accounting.h \
		utils/iostream.h \
		utils/gzip.h \
		utils/compress.h \
		utils/files.h \
		utils/geos.h \
		utils/geosfwd.h \
		utils/process.h \
		utils/regexp.h \
		utils/sqlite.h \
		utils/string.h \
		utils/sys.h \
		utils/subprocess.h \
		utils/yaml.h \
		utils/tar.h \
		utils/zip.h \
		utils/term.h \
		utils/tests.h \
		utils/testrunner.h

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
libarkimet_la_LDFLAGS = -version-info @LIBARKI_VERSION_INFO@ $(SQLITE3_LDFLAGS) $(ZLIB_LIBS) $(LIBARCHIVE_LIBS) $(LIBZIP_LIBS) $(LIBSSL_LIBS) $(DEVEL_LIBS) -lpthread
libarkimet_test_la_SOURCES = tests/main.cpp
tests_arki_test_SOURCES =
tests_arki_test_LDADD =


#
# Core functions
#
libarkimet_la_SOURCES += \
		core/time.cc \
		core/fuzzytime.cc \
		core/file.cc \
		core/cfg.cc \
		core/binary.cc \
		core/transaction.cc \
		core/curl.cc \
		stream.cc \
		stream/base.cc \
		stream/filter.cc \
		stream/concrete.cc \
		stream/discard.cc \
		stream/buffer.cc \
		exceptions.cc \
		nag.cc \
		utils/accounting.cc \
		utils/geos.cc \
		utils/gzip.cc \
		utils/files.cc \
		utils/process.cc \
		utils/regexp.cc \
		utils/string.cc \
		utils/sys.cc \
		utils/subprocess.cc \
		utils/yaml.cc \
		utils/tar.cc \
		utils/zip.cc \
		utils/term.cc \
		structured/emitter.cc \
		structured/keys.cc \
		structured/reader.cc \
		structured/memory.cc \
		structured/json.cc \
		bbox.cc \
		types.cc \
		types/bundle.cc \
		types/utils.cc \
		types/encoded.cc \
		types/values.cc \
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
		types/itemset.cc \
		matcher.cc \
		matcher/aliases.cc \
		matcher/parser.cc \
		matcher/utils.cc \
		matcher/origin.cc \
		matcher/product.cc \
		matcher/level.cc \
		matcher/timerange.cc \
		matcher/reftime.cc \
		matcher/reftime/lexer.cc \
		matcher/reftime/parser.cc \
		matcher/reftime/reftime-lex.cc \
		matcher/reftime/reftime-parse.cc \
		matcher/area.cc \
		matcher/proddef.cc \
		matcher/run.cc \
		matcher/task.cc \
		matcher/quantity.cc \
		types-init.cc \
		utils/compress.cc \
		utils/sqlite.cc \
		metadata.cc \
		metadata/sort.cc \
		metadata/data.cc \
		metadata/collection.cc \
		metadata/stream.cc \
		metadata/clusterer.cc \
		metadata/archive.cc \
		metadata/validator.cc \
		metadata/postprocess.cc \
		metadata/test-generator.cc \
		summary/stats.cc \
		summary/intern.cc \
		summary/table.cc \
		summary/codec.cc \
		summary/short.cc \
		summary.cc \
		segment.cc \
		segment/common.cc \
		segment/missing.cc \
		segment/fd.cc \
		segment/dir.cc \
		segment/tar.cc \
		segment/zip.cc \
		segment/gz.cc \
		segment/seqfile.cc \
		formatter.cc \
		scan.cc \
		scan/validator.cc \
		scan/mock.cc \
		scan/odimh5.cc \
		scan/netcdf.cc

libarkimet_test_la_SOURCES += \
		tests/daemon.cc \
		tests/tests.cc \
		core/tests.cc \
		types/tests.cc \
		utils/tests.cc \
		utils/testrunner.cc

tests_arki_test_SOURCES += \
		tests/tests-test.cc \
		core/time-test.cc \
		core/fuzzytime-test.cc \
		core/file-test.cc \
		core/cfg-test.cc \
		core/binary-test.cc \
		core/transaction-test.cc \
		core/curl-test.cc \
		stream-test.cc \
		stream/base-test.cc \
		stream/filter-test.cc \
		stream/concrete-test.cc \
		stream/concrete-timeout-test.cc \
		stream/discard-test.cc \
		stream/buffer-test.cc \
		stream/tests.cc \
		utils/accounting-test.cc \
		utils/gzip-test.cc \
		utils/yaml-test.cc \
		utils/tar-test.cc \
		utils/zip-test.cc \
		utils/files-test.cc \
		utils/process-test.cc \
		utils/regexp-test.cc \
		utils/geos-test.cc \
		structured/memory-test.cc \
		structured/json-test.cc \
		types-test.cc \
		types/bundle-test.cc \
		types/utils-test.cc \
		types/encoded-test.cc \
		types/values-test.cc \
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
		types/itemset-test.cc \
		metadata/tests.cc \
		matcher/tests.cc \
		utils/compress-test.cc \
		utils/sqlite-test.cc \
		formatter-test.cc \
		bbox-test.cc \
		metadata-test.cc \
		metadata/sort-test.cc \
		metadata/data-test.cc \
		metadata/collection-test.cc \
		metadata/clusterer-test.cc \
		metadata/stream-test.cc \
		metadata/archive-test.cc \
		metadata/validator-test.cc \
		metadata/test-generator-test.cc \
		summary-test.cc \
		summary/intern-test.cc \
		summary/table-test.cc \
		summary/stats-test.cc \
		summary/short-test.cc \
		matcher-test.cc \
		matcher/aliases-test.cc \
		matcher/parser-test.cc \
		matcher/utils-test.cc \
		matcher/origin-test.cc \
		matcher/product-test.cc \
		matcher/level-test.cc \
		matcher/timerange-test.cc \
		matcher/reftime-test.cc \
		matcher/reftime/lexer-test.cc \
		matcher/reftime/parser-test.cc \
		matcher/area-test.cc \
		matcher/proddef-test.cc \
		matcher/run-test.cc \
		matcher/task-test.cc \
		matcher/quantity-test.cc \
		scan-test.cc \
		scan/validator-test.cc \
		scan/mock-test.cc \
		scan/odimh5-test.cc \
		scan/netcdf-test.cc \
		segment-test.cc \
		segment/common-test.cc \
		segment/missing-test.cc \
		segment/fd-test.cc \
		segment/seqfile-test.cc \
		segment/dir-test.cc \
		segment/tar-test.cc \
		segment/zip-test.cc \
		segment/gz-test.cc \
		segment/tests.cc

if IOTRACE
libarkimet_la_SOURCES += iotrace.cc
tests_arki_test_SOURCES += iotrace-test.cc
endif

if LZO
AM_CPPFLAGS += $(LZO_CFLAGS)
libarkimet_la_LDFLAGS += $(LZO_LIBS)
endif

if GEOS
AM_CPPFLAGS += $(GEOS_CFLAGS)
libarkimet_la_LDFLAGS += $(GEOS_LIBS)
endif

if GRIBAPI
nobase_dist_arkiinclude_HEADERS += scan/grib.h
libarkimet_la_SOURCES += scan/grib.cc
tests_arki_test_SOURCES += scan/grib-test.cc
AM_CPPFLAGS += $(GRIBAPI_CFLAGS)
libarkimet_la_LDFLAGS += $(GRIBAPI_LIBS)
endif

if DBALLE
nobase_dist_arkiinclude_HEADERS += scan/bufr.h
libarkimet_la_SOURCES += scan/bufr.cc
libarkimet_la_LDFLAGS += $(DBALLE_LIBS)
AM_CPPFLAGS += $(DBALLE_CFLAGS)
tests_arki_test_SOURCES += scan/bufr-test.cc
endif

if VM2
AM_CPPFLAGS += $(VM2_CFLAGS)
nobase_dist_arkiinclude_HEADERS += scan/vm2.h utils/vm2.h
libarkimet_la_SOURCES += utils/vm2.cc scan/vm2.cc
libarkimet_la_LDFLAGS += $(VM2_LIBS)
tests_arki_test_SOURCES += utils/vm2-test.cc scan/vm2-test.cc

if LUA
AM_CPPFLAGS += $(LUA_CFLAGS)
libarkimet_la_LDFLAGS += $(LUA_LIBS)
endif
endif


#
# Datasets
#
if DEVEL_DO_DATASETS
libarkimet_la_SOURCES += \
		metadata/xargs.cc \
		dataset.cc \
		dataset/lock.cc \
		dataset/query.cc \
		dataset/reporter.cc \
		dataset/progress.cc \
		dataset/time.cc \
		dataset/session.cc \
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
		dataset/fromfunction.cc \
		dataset/testlarge.cc \
		dataset/merged.cc \
		dataset/querymacro.cc \
		dataset/pool.cc \
		dispatcher.cc \
		runtime.cc

tests_arki_test_SOURCES += \
		metadata/xargs-test.cc \
		metadata/postprocess-test.cc \
		dataset/tests.cc \
		dataset/lock-test.cc \
		dataset/query-test.cc \
		dataset/reporter-test.cc \
		dataset/progress-test.cc \
		dataset/time-test.cc \
		dataset/session-test.cc \
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
		dataset/fromfunction-test.cc \
		dataset/testlarge-test.cc \
		dataset/merged-test.cc \
		dataset/querymacro-test.cc \
		dataset/pool-test.cc \
		dataset-test.cc \
		dataset-reader-test.cc \
		dataset-writer-test.cc \
		dataset-checker-test.cc \
		dataset-concurrency-test.cc \
		dispatcher-test.cc \
		runtime-test.cc

if CURL
AM_CPPFLAGS += @LIBCURL_CPPFLAGS@
nobase_dist_arkiinclude_HEADERS += dataset/http.h
libarkimet_la_SOURCES += dataset/http.cc
libarkimet_la_LDFLAGS += @LIBCURL@
tests_arki_test_SOURCES += dataset/http-test.cc
endif

endif

if GCOV
export ARKI_HAS_GCOV=yes
endif

# Tests
TESTS_ENVIRONMENT = $(top_srcdir)/arki/runtest
check_PROGRAMS = tests/arki-test

check-local:
	for test in $(check_PROGRAMS); do \
		$(TESTS_ENVIRONMENT) $$test ; \
	done

tests_arki_test_LDADD += libarkimet.la libarkimet-test.la 
if GEOS
tests_arki_test_LDADD += $(GEOS_LIBS)
endif
if VM2
tests_arki_test_LDADD += $(VM2_LIBS)
if LUA
tests_arki_test_LDADD += $(LUA_LIBS)
endif
endif
if LZO
tests_arki_test_LDADD += $(LZO_LIBS)
endif
tests_arki_test_LDADD += $(SQLITE3_LDFLAGS) $(ZLIB_LIBS) $(LIBARCHIVE_LIBS) $(LIBZIP_LIBS) $(LIBSSL_LIBS) $(DEVEL_LIBS) -lpthread

EXTRA_DIST = runtest buildbison buildflex \
	     matcher/reftime/reftime-lex.ll \
	     matcher/reftime/reftime-parse.yy \
	     $(reftime_parser_built_sources) \
	     dataset/http-test-daemon
