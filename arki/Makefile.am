## Process this file with automake to produce Makefile.in

SUBDIRS = .

arkiincludedir = $(includedir)/arki

pkginclude_HEADERS = \
		config.h \
		configfile.h \
		transaction.h \
		dataset.h \
		datasetpool.h \
		dispatcher.h \
		emitter.h \
		matcher.h \
		itemset.h \
		metadata.h \
		summary.h \
		refcounted.h \
		runtime.h \
		types.h \
		types.tcc \
		metadatagrid.h \
		sort.h \
		formatter.h \
		bbox.h \
		targetfile.h \
		postprocess.h \
		querymacro.h \
		values.h \
		utils.h \
		nag.h \
		types-init.h

dsincludedir = $(pkginclude)/dataset
dsinclude_HEADERS = \
		dataset/targetfile.h \
		dataset/local.h \
		dataset/file.h \
		dataset/maintenance.h \
		dataset/archive.h \
		dataset/ondisk2.h \
		dataset/outbound.h \
		dataset/discard.h \
		dataset/empty.h \
		dataset/merged.h \
		dataset/gridquery.h \
		dataset/test-utils.h \
		dataset/test-scenario.h

dsod2includedir = $(pkginclude)/dataset/ondisk2
dsod2include_HEADERS = \
		dataset/ondisk2/aggregate.h \
		dataset/ondisk2/index.h \
		dataset/ondisk2/reader.h \
		dataset/ondisk2/writer.h \
		dataset/ondisk2/test-utils.h

dsod2wrincludedir = $(pkginclude)/dataset/ondisk2/writer
dsod2wrinclude_HEADERS = \
		dataset/ondisk2/writer/datafile.h

dssincludedir = $(pkginclude)/dataset/simple
dssinclude_HEADERS = \
		dataset/simple/index.h \
		dataset/simple/datafile.h \
		dataset/simple/reader.h \
		dataset/simple/writer.h

dshincludedir = $(pkginclude)/dataset/http
dshinclude_HEADERS = \
		dataset/http/server.h \
		dataset/http/inbound.h \
		dataset/http/test-utils.h

dsiincludedir = $(pkginclude)/dataset/index
dsiinclude_HEADERS = \
		dataset/index/base.h \
		dataset/index/attr.h

emitterincludedir = $(pkginclude)/emitter
emitterinclude_HEADERS = \
		emitter/memory.h \
		emitter/json.h

formatterincludedir = $(pkginclude)/formatter
formatterinclude_HEADERS =

matcherincludedir = $(pkginclude)/matcher
matcherinclude_HEADERS = \
		matcher/test-utils.h \
		matcher/utils.h \
		matcher/origin.h \
		matcher/product.h \
		matcher/level.h \
		matcher/timerange.h \
		matcher/reftime.h \
		matcher/area.h \
		matcher/proddef.h \
		matcher/run.h \
		matcher/task.h \
		matcher/quantity.h

matcherrtincludedir = $(pkginclude)/matcher/reftime
matcherrtinclude_HEADERS = \
		matcher/reftime/parser.h

metadataincludedir = $(pkginclude)/metadata
metadatainclude_HEADERS = \
		metadata/consumer.h \
		metadata/collection.h \
		metadata/stream.h \
		metadata/test-generator.h

scanincludedir = $(pkginclude)/scan
scaninclude_HEADERS = \
		scan/any.h \
		scan/dir.h

summaryincludedir = $(pkginclude)/summary
summaryinclude_HEADERS = \
		summary/node.h \
		summary/stats.h

runtimeincludedir = $(pkginclude)/runtime
runtimeinclude_HEADERS = \
		runtime/io.h \
		runtime/processor.h \
		runtime/config.h

typesincludedir = $(pkginclude)/types
typesinclude_HEADERS = \
		types/utils.h \
		types/test-utils.h \
		types/time.h \
		types/reftime.h \
		types/source.h \
		types/utils.h \
		types/origin.h \
		types/product.h \
		types/level.h \
		types/timerange.h \
		types/area.h \
		types/proddef.h \
		types/note.h \
		types/assigneddataset.h \
		types/bbox.h \
		types/run.h \
		types/task.h \
		types/quantity.h

utilsincludedir = $(pkginclude)/utils
utilsinclude_HEADERS = \
		utils/geosfwd.h \
		utils/geosdef.h \
		utils/accounting.h \
		utils/codec.h \
		utils/compress.h \
		utils/datareader.h \
		utils/sqlite.h \
		utils/fd.h \
		utils/files.h \
		utils/dataset.h \
		utils/pcounter.h \
		utils/process.h

testincludedir = $(pkginclude)/tests
testinclude_HEADERS = \
		tests/test-utils.h

BUILT_SOURCES = $(top_builddir)/arki/matcher/reftime/reftime-lex.h \
		$(top_builddir)/arki/matcher/reftime/reftime-lex.cc \
		$(top_builddir)/arki/matcher/reftime/reftime-parse.hh \
		$(top_builddir)/arki/matcher/reftime/reftime-parse.cc

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
		utils/pcounter.cc \
		utils/process.cc \
		configfile.cc \
		transaction.cc \
		emitter/memory.cc \
		emitter/json.cc \
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
		dataset/ondisk2/writer/datafile.cc \
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
tests_arki_test_SOURCES = \
		utils-tut.cc \
		utils/accounting-tut.cc \
		utils/codec-tut.cc \
		utils/compress-tut.cc \
		utils/datareader-tut.cc \
		utils/sqlite-tut.cc \
		utils/fd-tut.cc \
		utils/files-tut.cc \
		utils/dataset-tut.cc \
		utils/pcounter-tut.cc \
		utils/process-tut.cc \
		refcounted-tut.cc \
		configfile-tut.cc \
		transaction-tut.cc \
		emitter/memory-tut.cc \
		emitter/json-tut.cc \
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
		itemset-tut.cc \
		sort-tut.cc \
		formatter-tut.cc \
		bbox-tut.cc \
		targetfile-tut.cc \
		metadata-tut.cc \
		metadata/consumer-tut.cc \
		metadata/collection-tut.cc \
		metadata/stream-tut.cc \
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
		dataset/ondisk2/writer/datafile-tut.cc \
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

if GRIBAPI
scaninclude_HEADERS += scan/grib.h
libarkimet_types_la_SOURCES += scan/grib.cc
tests_arki_test_SOURCES += scan/grib-tut.cc
INCLUDES += $(GRIBAPI_CFLAGS) $(LUA_CFLAGS)
libarkimet_types_la_LDFLAGS += $(GRIBAPI_LIBS) $(LUA_LIBS)
tests_arki_test_LDADD += $(GRIBAPI_LIBS) $(LUA_LIBS)
endif

if LUA
pkginclude_HEADERS += report.h
formatterinclude_HEADERS += formatter/lua.h
utilsinclude_HEADERS += utils/lua.h
testinclude_HEADERS += tests/lua.h
libarkimet_types_la_SOURCES += formatter/lua.cc utils/lua.cc report.cc
tests_arki_test_SOURCES += formatter/lua-tut.cc utils/lua-tut.cc report-tut.cc tests/lua.cc
INCLUDES += $(LUA_CFLAGS)
libarkimet_types_la_LDFLAGS += $(LUA_LIBS)
tests_arki_test_LDADD += $(LUA_LIBS)
endif

if DBALLE
scaninclude_HEADERS += scan/bufr.h
libarkimet_types_la_SOURCES += scan/bufr.cc
tests_arki_test_SOURCES += scan/bufr-tut.cc
INCLUDES += $(DBALLE_CFLAGS)
libarkimet_types_la_LDFLAGS += $(DBALLE_LIBS)
tests_arki_test_LDADD += $(DBALLE_LIBS)
if LUA
scaninclude_HEADERS += scan/bufrlua.h
libarkimet_types_la_SOURCES += scan/bufrlua.cc
endif
endif

if CURL
dsinclude_HEADERS += dataset/http.h
libarkimet_dataset_la_SOURCES += dataset/http.cc
tests_arki_test_SOURCES += dataset/http-tut.cc
INCLUDES += @LIBCURL_CPPFLAGS@
libarkimet_dataset_la_LDFLAGS += @LIBCURL@
tests_arki_test_LDADD += @LIBCURL@
endif

if ODIMH5
scaninclude_HEADERS += scan/odimh5.h
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

EXTRA_DIST = runtest buildbison buildflex \
	     matcher/reftime/reftime-lex.ll \
	     matcher/reftime/reftime-parse.yy \
	     $(top_builddir)/arki/matcher/reftime/reftime-lex.h \
	     $(top_builddir)/arki/matcher/reftime/reftime-parse.hh
