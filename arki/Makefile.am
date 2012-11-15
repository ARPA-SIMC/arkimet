## Process this file with automake to produce Makefile.in

SUBDIRS = .

arkiincludedir = $(includedir)/arki

nobase_dist_arkiinclude_HEADERS = \
		bbox.h \
		configfile.h \
		libconfig.h \
		data.h \
		data/impl.h \
		data/concat.h \
		dataset/archive.h \
		dataset/discard.h \
		dataset/empty.h \
		dataset/file.h \
		dataset/gridquery.h \
		dataset.h \
		dataset/http/inbound.h \
		dataset/http/server.h \
		dataset/http/test-utils.h \
		dataset/index/attr.h \
		dataset/index/base.h \
		dataset/local.h \
		dataset/maintenance.h \
		dataset/merged.h \
		dataset/ondisk2/aggregate.h \
		dataset/ondisk2.h \
		dataset/ondisk2/index.h \
		dataset/ondisk2/reader.h \
		dataset/ondisk2/test-utils.h \
		dataset/ondisk2/writer.h \
		dataset/outbound.h \
		datasetpool.h \
		dataset/simple/datafile.h \
		dataset/simple/index.h \
		dataset/simple/reader.h \
		dataset/simple/writer.h \
		dataset/targetfile.h \
		dataset/test-scenario.h \
		dataset/test-utils.h \
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
		matcher/task.h \
		matcher/test-utils.h \
		matcher/timerange.h \
		matcher/utils.h \
		metadata/collection.h \
		metadata/consumer.h \
		metadatagrid.h \
		metadata.h \
		metadata/stream.h \
		metadata/tests.h \
		metadata/test-generator.h \
		nag.h \
		postprocess.h \
		querymacro.h \
		refcounted.h \
		runtime/config.h \
		runtime.h \
		runtime/io.h \
		runtime/processor.h \
		scan/any.h \
		scan/dir.h \
		sort.h \
		summary.h \
		summary/node.h \
		summary/stats.h \
		targetfile.h \
		tests/test-utils.h \
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
		types/task.h \
		types.tcc \
		types/test-utils.h \
		types/time.h \
		types/timerange.h \
		types/utils.h \
		types/value.h \
		utils/accounting.h \
		utils/codec.h \
		utils/compress.h \
		utils/datareader.h \
		utils/dataset.h \
		utils/fd.h \
		utils/files.h \
		utils/geosdef.h \
		utils/geosfwd.h \
		utils.h \
		utils/intern.h \
		utils/pcounter.h \
		utils/process.h \
		utils/sqlite.h \
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

noinst_LTLIBRARIES = libarkimet-core.la libarkimet-types.la libarkimet-dataset.la

libarkimet_core_la_SOURCES = \
		nag.cc \
		utils.cc \
		utils/accounting.cc \
		utils/codec.cc \
		utils/datareader.cc \
		utils/fd.cc \
		utils/files.cc \
		utils/intern.cc \
		utils/pcounter.cc \
		utils/process.cc \
		configfile.cc \
		transaction.cc \
		emitter.cc \
		emitter/memory.cc \
		emitter/json.cc \
		data.cc \
		data/impl.cc \
		data/concat.cc \
		tests/test-utils.cc
libarkimet_core_la_LDFLAGS =

libarkimet_types_la_SOURCES = \
		utils/compress.cc \
		utils/sqlite.cc \
		values.cc \
		types.cc \
		types/utils.cc \
		types/source.cc \
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
		itemset.cc \
		metadata.cc \
		metadata/consumer.cc \
		metadata/collection.cc \
		metadata/stream.cc \
		metadata/test-generator.cc \
		summary.cc \
		summary/node.cc \
		summary/stats.cc \
		matcher.cc \
		matcher/utils.cc \
		matcher/origin.cc \
		matcher/product.cc \
		matcher/level.cc \
		matcher/timerange.cc \
		matcher/reftime.cc \
		matcher/reftime/reftime-lex.cc \
		matcher/reftime/reftime-parse.cc \
		matcher/area.cc \
		matcher/proddef.cc \
		matcher/run.cc \
		matcher/task.cc \
		matcher/quantity.cc \
		sort.cc \
		formatter.cc \
		validator.cc \
		bbox.cc \
		postprocess.cc \
		scan/any.cc \
		scan/dir.cc \
		types-init.cc
libarkimet_types_la_LIBADD = libarkimet-core.la
libarkimet_types_la_LDFLAGS =

libarkimet_dataset_la_SOURCES = \
		utils/dataset.cc \
		targetfile.cc \
		dataset.cc \
		dataset/maintenance.cc \
		dataset/index/base.cc \
		dataset/index/attr.cc \
		dataset/targetfile.cc \
		dataset/local.cc \
		dataset/file.cc \
		dataset/archive.cc \
		dataset/simple/index.cc \
		dataset/simple/datafile.cc \
		dataset/simple/reader.cc \
		dataset/simple/writer.cc \
		dataset/ondisk2.cc \
		dataset/ondisk2/aggregate.cc \
		dataset/ondisk2/index.cc \
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
libarkimet_dataset_la_LIBADD = libarkimet-types.la
libarkimet_dataset_la_LDFLAGS =

lib_LTLIBRARIES = libarkimet.la
libarkimet_la_SOURCES = \
		runtime/io.cc \
		runtime/config.cc \
		runtime/processor.cc \
		runtime.cc
libarkimet_la_LIBADD = libarkimet-dataset.la
libarkimet_la_LDFLAGS = -version-info @LIBARKI_VERSION_INFO@ ../wibble/libwibble.la $(SQLITE3_LDFLAGS) $(ZLIB_LIBS) $(DEVEL_LIBS) -lpthread

if GCOV
export ARKI_HAS_GCOV=yes
endif

# Tests
TESTS = tests/arki-test
TESTS_ENVIRONMENT = $(top_srcdir)/arki/runtest
check_PROGRAMS = tests/arki-test
#tests_arki_test_CXXFLAGS = --std=c++11
tests_arki_test_SOURCES = \
		utils-tut.cc \
		utils/accounting-tut.cc \
		utils/codec-tut.cc \
		utils/compress-tut.cc \
		utils/datareader-tut.cc \
		utils/sqlite-tut.cc \
		utils/fd-tut.cc \
		utils/files-tut.cc \
		utils/intern-tut.cc \
		utils/dataset-tut.cc \
		utils/pcounter-tut.cc \
		utils/process-tut.cc \
		refcounted-tut.cc \
		configfile-tut.cc \
		transaction-tut.cc \
		emitter/memory-tut.cc \
		emitter/json-tut.cc \
		data-tut.cc \
		data/impl-tut.cc \
		data/concat-tut.cc \
		values-tut.cc \
		types-tut.cc \
		types/utils-tut.cc \
		types/source-tut.cc \
		types/time-tut.cc \
		types/reftime-tut.cc \
		types/origin-tut.cc \
		types/product-tut.cc \
		types/level-tut.cc \
		types/timerange-tut.cc \
		types/area-tut.cc \
		types/proddef-tut.cc \
		types/note-tut.cc \
		types/assigneddataset-tut.cc \
		types/bbox-tut.cc \
		types/run-tut.cc \
		types/task-tut.cc \
		types/quantity-tut.cc \
		types/value-tut.cc \
		itemset-tut.cc \
		sort-tut.cc \
		formatter-tut.cc \
		validator-tut.cc \
		bbox-tut.cc \
		targetfile-tut.cc \
		metadata-tut.cc \
		metadata/consumer-tut.cc \
		metadata/collection-tut.cc \
		metadata/stream-tut.cc \
		metadata/tests.cc \
		metadata/test-generator-tut.cc \
		summary-tut.cc \
		summary/node-tut.cc \
		summary/stats-tut.cc \
		matcher-tut.cc \
		matcher/test-utils.cc \
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
		matcher/task-tut.cc \
		matcher/quantity-tut.cc \
		scan/any-tut.cc \
		scan/dir-tut.cc \
		dataset/maintenance-tut.cc \
		dataset/index/base-tut.cc \
		dataset/index/attr-tut.cc \
		dataset/targetfile-tut.cc \
		dataset/local-tut.cc \
		dataset/file-tut.cc \
		dataset/archive-tut.cc \
		dataset/simple/index-tut.cc \
		dataset/simple/datafile-tut.cc \
		dataset/simple/reader-tut.cc \
		dataset/simple/writer-tut.cc \
		dataset/ondisk2-tut.cc \
		dataset/ondisk2/aggregate-tut.cc \
		dataset/ondisk2/index-tut.cc \
		dataset/ondisk2/reader-tut.cc \
		dataset/ondisk2/writer-tut.cc \
		dataset/ondisk2/test-utils.cc \
		dataset/http/server-tut.cc \
		dataset/http/inbound-tut.cc \
		dataset/http/test-utils.cc \
		dataset/test-utils.cc \
		dataset/outbound-tut.cc \
		dataset/discard-tut.cc \
		dataset/empty-tut.cc \
		dataset/merged-tut.cc \
		dataset/gridquery-tut.cc \
		dataset/test-scenario-tut.cc \
		dataset-tut.cc \
		metadatagrid-tut.cc \
		postprocess-tut.cc \
		querymacro-tut.cc \
		datasetpool-tut.cc \
		dispatcher-tut.cc \
		runtime/io-tut.cc \
		runtime/config-tut.cc \
		runtime/processor-tut.cc \
		runtime-tut.cc \
		tests/tut-main.cpp
tests_arki_test_LDADD = libarkimet.la ../wibble/libwibble.la $(SQLITE3_LDFLAGS) $(DEVEL_LIBS) -lpthread

INCLUDES = -I$(top_srcdir) -I$(top_builddir) $(SQLITE3_CFLAGS) -DCONF_DIR=\"$(confdir)\" -DPOSTPROC_DIR=\"$(postprocdir)\" $(ZLIB_CFLAGS) $(DEVEL_CFLAGS)

if IOTRACE
libarkimet_core_la_SOURCES += iotrace.cc
tests_arki_test_SOURCES += iotrace-tut.cc
endif

if GRIBAPI
nobase_dist_arkiinclude_HEADERS += scan/grib.h
libarkimet_types_la_SOURCES += scan/grib.cc
tests_arki_test_SOURCES += scan/grib-tut.cc
INCLUDES += $(GRIBAPI_CFLAGS) $(LUA_CFLAGS)
libarkimet_types_la_LDFLAGS += $(GRIBAPI_LIBS) $(LUA_LIBS)
tests_arki_test_LDADD += $(GRIBAPI_LIBS) $(LUA_LIBS)
endif

if LUA
nobase_dist_arkiinclude_HEADERS += report.h formatter/lua.h utils/lua.h tests/lua.h
libarkimet_types_la_SOURCES += formatter/lua.cc utils/lua.cc report.cc
tests_arki_test_SOURCES += formatter/lua-tut.cc utils/lua-tut.cc report-tut.cc tests/lua.cc
INCLUDES += $(LUA_CFLAGS)
libarkimet_types_la_LDFLAGS += $(LUA_LIBS)
tests_arki_test_LDADD += $(LUA_LIBS)
endif

if DBALLE
nobase_dist_arkiinclude_HEADERS += scan/bufr.h
libarkimet_types_la_SOURCES += scan/bufr.cc
tests_arki_test_SOURCES += scan/bufr-tut.cc
INCLUDES += $(DBALLE_CFLAGS)
libarkimet_types_la_LDFLAGS += $(DBALLE_LIBS)
tests_arki_test_LDADD += $(DBALLE_LIBS)
if LUA
nobase_dist_arkiinclude_HEADERS += scan/bufrlua.h
libarkimet_types_la_SOURCES += scan/bufrlua.cc
endif
endif

if CURL
nobase_dist_arkiinclude_HEADERS += dataset/http.h
libarkimet_dataset_la_SOURCES += dataset/http.cc
tests_arki_test_SOURCES += dataset/http-tut.cc
INCLUDES += @LIBCURL_CPPFLAGS@
libarkimet_dataset_la_LDFLAGS += @LIBCURL@
tests_arki_test_LDADD += @LIBCURL@
endif

if ODIMH5
nobase_dist_arkiinclude_HEADERS += scan/odimh5.h
libarkimet_types_la_SOURCES += scan/odimh5.cc
tests_arki_test_SOURCES += scan/odimh5-tut.cc
INCLUDES += $(ODIMH5_CFLAGS)
libarkimet_types_la_LDFLAGS += $(ODIMH5_LIBS)
tests_arki_test_LDADD += $(ODIMH5_LIBS)
endif

if GEOS
INCLUDES += $(GEOS_CFLAGS)
libarkimet_la_LDFLAGS += $(GEOS_LIBS)
tests_arki_test_LDADD += $(GEOS_LIBS)
endif

if LZO
INCLUDES += $(LZO_CFLAGS)
libarkimet_core_la_LDFLAGS += $(LZO_LIBS)
tests_arki_test_LDADD += $(LZO_LIBS)
endif

if VM2
nobase_dist_arkiinclude_HEADERS += scan/vm2.h utils/vm2.h
libarkimet_types_la_SOURCES += scan/vm2.cc utils/vm2.cc
tests_arki_test_SOURCES += scan/vm2-tut.cc utils/vm2-tut.cc
libarkimet_types_la_LDFLAGS += $(VM2_LIBS)
INCLUDES += $(VM2_CFLAGS)
endif

EXTRA_DIST = runtest buildbison buildflex \
	     matcher/reftime/reftime-lex.ll \
	     matcher/reftime/reftime-parse.yy \
	     $(top_builddir)/arki/matcher/reftime/reftime-lex.h \
	     $(top_builddir)/arki/matcher/reftime/reftime-parse.hh
