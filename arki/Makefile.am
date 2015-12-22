## Process this file with automake to produce Makefile.in

SUBDIRS = .

AM_CPPFLAGS = -I$(top_srcdir) -I$(top_builddir) $(SQLITE3_CFLAGS) -DCONF_DIR=\"$(confdir)\" -DPOSTPROC_DIR=\"$(postprocdir)\" $(ZLIB_CFLAGS)
AM_CXXFLAGS = $(EXTRA_CXXFLAGS) $(DEVEL_CXXFLAGS)
AM_LDFLAGS = $(EXTRA_LDFLAGS)


arkiincludedir = $(includedir)/arki

nobase_dist_arkiinclude_HEADERS = \
		wibble/exception.h \
		wibble/regexp.h \
		wibble/sys/process.h \
		wibble/sys/childprocess.h \
		wibble/sys/exec.h \
		wibble/sys/fs.h \
		wibble/sys/signal.h \
		wibble/sys/filelock.h \
		wibble/tests/tut.h \
		wibble/grcal/grcal.h \
		binary.h \
		defs.h \
		bbox.h \
		configfile.h \
		libconfig.h \
		dataset/archive.h \
		dataset/data.h \
		dataset/data/fd.h \
		dataset/data/concat.h \
		dataset/data/lines.h \
		dataset/data/dir.h \
		dataset/data/tests.h \
		dataset/discard.h \
		dataset/empty.h \
		dataset/file.h \
		dataset/gridquery.h \
		dataset.h \
		dataset/http/inbound.h \
		dataset/http/server.h \
		dataset/http/test-utils.h \
		dataset/index.h \
		dataset/index/attr.h \
		dataset/index/base.h \
		dataset/index/aggregate.h \
		dataset/index/summarycache.h \
		dataset/index/manifest.h \
		dataset/index/contents.h \
		dataset/local.h \
		dataset/maintenance.h \
		dataset/memory.h \
		dataset/merged.h \
		dataset/ondisk2.h \
		dataset/ondisk2/reader.h \
		dataset/ondisk2/test-utils.h \
		dataset/ondisk2/writer.h \
		dataset/outbound.h \
		datasetpool.h \
		dataset/simple/datafile.h \
		dataset/simple/reader.h \
		dataset/simple/writer.h \
		dataset/targetfile.h \
		dataset/test-scenario.h \
		dataset/tests.h \
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
		metadata/collection.h \
		metadata/consumer.h \
		metadata/clusterer.h \
		metadata/xargs.h \
		metadatagrid.h \
		metadata.h \
		metadata/stream.h \
		metadata/tests.h \
		metadata/test-generator.h \
		nag.h \
		postprocess.h \
		querymacro.h \
		runtime/config.h \
		runtime.h \
		runtime/io.h \
		runtime/printer.h \
		runtime/processor.h \
		scan/any.h \
		scan/dir.h \
		sort.h \
		summary.h \
		summary/intern.h \
		summary/table.h \
		summary/codec.h \
		summary/stats.h \
		targetfile.h \
		tests/tests.h \
		transaction.h \
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
		types/time.h \
		types/timerange.h \
		types/utils.h \
		types/value.h \
		types/typevector.h \
		types/typeset.h \
		utils/accounting.h \
		utils/gzip.h \
		utils/compress.h \
		utils/datareader.h \
		utils/fd.h \
		utils/files.h \
		utils/geosdef.h \
		utils/geosfwd.h \
		utils/h5.h \
		utils.h \
		utils/intern.h \
		utils/pcounter.h \
		utils/process.h \
		utils/sqlite.h \
		utils/raii.h \
		utils/string.h \
		utils/sys.h \
		utils/yaml.h \
		utils/commandline/core.h \
		utils/commandline/doc.h \
		utils/commandline/engine.h \
		utils/commandline/options.h \
		utils/commandline/parser.h \
		utils/commandline/wordwrap.h \
		utils/net/mime.h \
		utils/net/server.h \
		utils/net/http.h \
		values.h

BUILT_SOURCES = $(top_builddir)/arki/matcher/reftime/reftime-lex.h \
		$(top_builddir)/arki/matcher/reftime/reftime-lex.cc \
		$(top_builddir)/arki/matcher/reftime/reftime-parse.hh \
		$(top_builddir)/arki/matcher/reftime/reftime-parse.cc

all_built_sources: $(BUILT_SOURCES)

$(top_builddir)/arki/matcher/reftime/reftime-lex.h $(top_builddir)/arki/matcher/reftime/reftime-lex.cc: $(srcdir)/matcher/reftime/reftime-lex.ll
	$(srcdir)/buildflex $<

$(top_builddir)/arki/matcher/reftime/reftime-parse.hh $(top_builddir)/arki/matcher/reftime/reftime-parse.cc: $(srcdir)/matcher/reftime/reftime-parse.yy
	$(srcdir)/buildbison $<

lib_LTLIBRARIES = libarkimet.la

# Build these incrementally
libarkimet_la_SOURCES =
libarkimet_la_LDFLAGS = -version-info @LIBARKI_VERSION_INFO@ $(SQLITE3_LDFLAGS) $(ZLIB_LIBS) $(DEVEL_LIBS) -lpthread
tests_arki_test_SOURCES = tests/tut-main.cpp
tests_arki_test_LDADD =


#
# Core functions
#
if DEVEL_DO_TYPES
libarkimet_la_SOURCES += \
		wibble/exception.cpp \
		wibble/regexp.cpp \
		wibble/sys/fs.cpp \
		wibble/sys/filelock.cpp \
		wibble/sys/process.cpp \
		wibble/sys/childprocess.cpp \
		wibble/sys/exec.cpp \
		wibble/sys/signal.cpp \
		wibble/log/file.cpp \
		wibble/log/stream.cpp \
		wibble/log/ostream.cpp \
		wibble/log/syslog.cpp \
		wibble/log/filters.cpp \
		wibble/grcal/grcal.cpp \
		nag.cc \
		utils.cc \
		utils/accounting.cc \
		utils/gzip.cc \
		utils/fd.cc \
		utils/files.cc \
		utils/intern.cc \
		utils/pcounter.cc \
		utils/process.cc \
		utils/raii.cc \
		utils/string.cc \
		utils/sys.cc \
		utils/yaml.cc \
		utils/commandline/core.cpp \
		utils/commandline/options.cpp \
		utils/commandline/doc.cpp \
		utils/commandline/parser.cpp \
		utils/commandline/engine.cpp \
		utils/commandline/wordwrap.cpp \
		utils/net/mime.cpp \
		utils/net/server.cpp \
		utils/net/http.cpp \
		binary.cc \
		configfile.cc \
		transaction.cc \
		emitter.cc \
		emitter/memory.cc \
		emitter/json.cc \
		tests/tests.cc \
		values.cc \
		bbox.cc \
		types.cc \
		types/utils.cc \
		types/source.cc \
		types/source/blob.cc \
		types/source/inline.cc \
		types/source/url.cc \
		types/time.cc \
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
		runtime/io.cc \
		runtime/printer.cc \
		types-init.cc

tests_arki_test_SOURCES += \
		tests/tests-tut.cc \
		types/tests.cc \
		utils/tests.cc \
		utils-test.cc \
		utils/accounting-tut.cc \
		utils/gzip-test.cc \
		utils/yaml-test.cc \
		utils/fd-tut.cc \
		utils/files-tut.cc \
		utils/intern-tut.cc \
		utils/pcounter-tut.cc \
		utils/process-tut.cc \
		utils/raii-tut.cc \
		binary-test.cc \
		configfile-test.cc \
		transaction-test.cc \
		emitter/memory-tut.cc \
		emitter/json-tut.cc \
		values-test.cc \
		types-test.cc \
		types/utils-test.cc \
		types/source-test.cc \
		types/time-test.cc \
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
		itemset-tut.cc

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
tests_arki_test_SOURCES += formatter/lua-tut.cc utils/lua-tut.cc tests/lua.cc
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
tests_arki_test_SOURCES += utils/vm2-tut.cc
endif

endif

#
# Metadata types
#
if DEVEL_DO_METADATA
libarkimet_la_SOURCES += \
		utils/h5.cc \
		utils/datareader.cc \
		utils/compress.cc \
		utils/sqlite.cc \
		metadata.cc \
		metadata/consumer.cc \
		metadata/collection.cc \
		metadata/stream.cc \
		metadata/clusterer.cc \
		metadata/test-generator.cc \
		summary/stats.cc \
		summary/intern.cc \
		summary/table.cc \
		summary/codec.cc \
		summary.cc \
		sort.cc \
		formatter.cc \
		validator.cc \
		postprocess.cc \
		scan/any.cc \
		scan/dir.cc

tests_arki_test_SOURCES += \
		metadata/tests.cc \
		matcher/tests.cc \
		utils/datareader-tut.cc \
		utils/compress-tut.cc \
		utils/sqlite-tut.cc \
		sort-test.cc \
		formatter-tut.cc \
		validator-test.cc \
		bbox-test.cc \
		metadata-test.cc \
		metadata/consumer-tut.cc \
		metadata/collection-tut.cc \
		metadata/clusterer-tut.cc \
		metadata/stream-tut.cc \
		metadata/test-generator-tut.cc \
		summary-test.cc \
		summary/intern-tut.cc \
		summary/table-tut.cc \
		summary/stats-tut.cc \
		matcher-tut.cc \
		matcher/utils-tut.cc \
		matcher/origin-tut.cc \
		matcher/product-tut.cc \
		matcher/level-tut.cc \
		matcher/timerange-tut.cc \
		matcher/reftime-tut.cc \
		matcher/reftime/parser-tut.cc \
		matcher/area-tut.cc \
		matcher/proddef-tut.cc \
		matcher/run-tut.cc \
		matcher/source-tut.cc \
		matcher/task-tut.cc \
		matcher/quantity-tut.cc \
		scan/any-tut.cc \
		scan/dir-tut.cc \
		runtime/io-tut.cc \
		runtime/config-tut.cc

if GRIBAPI
nobase_dist_arkiinclude_HEADERS += scan/grib.h
libarkimet_la_SOURCES += scan/grib.cc
tests_arki_test_SOURCES += scan/grib-tut.cc
AM_CPPFLAGS += $(GRIBAPI_CFLAGS) $(LUA_CFLAGS)
libarkimet_la_LDFLAGS += $(GRIBAPI_LIBS) $(LUA_LIBS)
endif

if DBALLE
nobase_dist_arkiinclude_HEADERS += scan/bufr.h
libarkimet_la_SOURCES += scan/bufr.cc
libarkimet_la_LDFLAGS += $(DBALLE_LIBS)
AM_CPPFLAGS += $(DBALLE_CFLAGS)
tests_arki_test_SOURCES += scan/bufr-tut.cc
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
tests_arki_test_SOURCES += utils/h5-tut.cc scan/odimh5-tut.cc
endif

if NETCDF
AM_CPPFLAGS += $(NETCDF_CFLAGS)
nobase_dist_arkiinclude_HEADERS += scan/nc.h
libarkimet_la_SOURCES += scan/nc.cc
libarkimet_la_LDFLAGS += $(NETCDF_LIBS)
tests_arki_test_SOURCES += scan/nc-tut.cc
endif

if VM2
libarkimet_la_SOURCES += scan/vm2.cc
tests_arki_test_SOURCES += scan/vm2-tut.cc
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
		dataset/data.cc \
		dataset/data/fd.cc \
		dataset/data/concat.cc \
		dataset/data/lines.cc \
		dataset/data/dir.cc \
		dataset/maintenance.cc \
		dataset/memory.cc \
		dataset/index.cc \
		dataset/index/base.cc \
		dataset/index/attr.cc \
		dataset/index/manifest.cc \
		dataset/index/aggregate.cc \
		dataset/index/summarycache.cc \
		dataset/index/contents.cc \
		dataset/targetfile.cc \
		dataset/local.cc \
		dataset/file.cc \
		dataset/archive.cc \
		dataset/simple/datafile.cc \
		dataset/simple/reader.cc \
		dataset/simple/writer.cc \
		dataset/ondisk2.cc \
		dataset/ondisk2/reader.cc \
		dataset/ondisk2/writer.cc \
		dataset/http/server.cc \
		dataset/http/inbound.cc \
		dataset/outbound.cc \
		dataset/discard.cc \
		dataset/empty.cc \
		dataset/merged.cc \
		dataset/gridquery.cc \
		dataset/test-scenario.cc \
		datasetpool.cc \
		metadatagrid.cc \
		dispatcher.cc \
		querymacro.cc

tests_arki_test_SOURCES += \
		targetfile-tut.cc \
		metadata/xargs-tut.cc \
		dataset/tests.cc \
		dataset/data-tut.cc \
		dataset/data/tests.cc \
		dataset/data/fd-tut.cc \
		dataset/data/concat-tut.cc \
		dataset/data/lines-tut.cc \
		dataset/data/dir-tut.cc \
		dataset/maintenance-test.cc \
		dataset/memory-tut.cc \
		dataset/index-test.cc \
		dataset/index/base-tut.cc \
		dataset/index/attr-tut.cc \
		dataset/index/manifest-tut.cc \
		dataset/index/aggregate-tut.cc \
		dataset/index/summarycache-tut.cc \
		dataset/index/contents-tut.cc \
		dataset/targetfile-tut.cc \
		dataset/local-test.cc \
		dataset/file-tut.cc \
		dataset/archive-tut.cc \
		dataset/simple/datafile-tut.cc \
		dataset/simple/reader-tut.cc \
		dataset/simple/writer-tut.cc \
		dataset/ondisk2-tut.cc \
		dataset/ondisk2/reader-tut.cc \
		dataset/ondisk2/writer-tut.cc \
		dataset/ondisk2/test-utils.cc \
		dataset/http/server-tut.cc \
		dataset/http/inbound-tut.cc \
		dataset/http/test-utils.cc \
		dataset/outbound-tut.cc \
		dataset/discard-tut.cc \
		dataset/empty-tut.cc \
		dataset/merged-tut.cc \
		dataset/gridquery-tut.cc \
		dataset/test-scenario-tut.cc \
		dataset-tut.cc \
		metadatagrid-test.cc \
		postprocess-tut.cc \
		querymacro-tut.cc \
		datasetpool-tut.cc \
		dispatcher-tut.cc \
		runtime/processor-tut.cc \
		runtime-test.cc

if CURL
AM_CPPFLAGS += @LIBCURL_CPPFLAGS@
nobase_dist_arkiinclude_HEADERS += dataset/http.h
libarkimet_la_SOURCES += dataset/http.cc
libarkimet_la_LDFLAGS += @LIBCURL@
tests_arki_test_SOURCES += dataset/http-tut.cc
endif

if LUA
libarkimet_la_SOURCES += report.cc
tests_arki_test_SOURCES += report-tut.cc
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

check-local:
	for test in $(check_PROGRAMS); do \
		$(TESTS_ENVIRONMENT) $$test ; \
	done

tests_arki_test_LDADD += libarkimet.la $(SQLITE3_LDFLAGS) $(DEVEL_LIBS) -lpthread

EXTRA_DIST = runtest buildbison buildflex \
	     matcher/reftime/reftime-lex.ll \
	     matcher/reftime/reftime-parse.yy \
	     $(top_builddir)/arki/matcher/reftime/reftime-lex.h \
	     $(top_builddir)/arki/matcher/reftime/reftime-parse.hh
