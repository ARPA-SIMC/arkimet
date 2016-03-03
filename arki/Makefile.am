## Process this file with automake to produce Makefile.in

arkiincludedir = $(includedir)/arki

nobase_dist_arkiinclude_HEADERS = \
		arki/wibble/sys/process.h \
		arki/wibble/sys/childprocess.h \
		arki/wibble/sys/exec.h \
		arki/wibble/sys/fs.h \
		arki/wibble/sys/signal.h \
		arki/wibble/sys/filelock.h \
		arki/wibble/tests/tut.h \
		arki/wibble/grcal/grcal.h \
		arki/binary.h \
		arki/defs.h \
		arki/exceptions.h \
		arki/file.h \
		arki/bbox.h \
		arki/configfile.h \
		arki/libconfig.h \
		arki/dataset/reporter.h \
		arki/dataset/archive.h \
		arki/dataset/segment.h \
		arki/dataset/segment/fd.h \
		arki/dataset/segment/concat.h \
		arki/dataset/segment/lines.h \
		arki/dataset/segment/dir.h \
		arki/dataset/segment/tests.h \
		arki/dataset/discard.h \
		arki/dataset/empty.h \
		arki/dataset/file.h \
		arki/dataset/gridquery.h \
		arki/dataset.h \
		arki/dataset/http/inbound.h \
		arki/dataset/http/server.h \
		arki/dataset/http/test-utils.h \
		arki/dataset/index.h \
		arki/dataset/index/attr.h \
		arki/dataset/index/base.h \
		arki/dataset/index/aggregate.h \
		arki/dataset/index/summarycache.h \
		arki/dataset/index/manifest.h \
		arki/dataset/index/contents.h \
		arki/dataset/local.h \
		arki/dataset/segmented.h \
		arki/dataset/indexed.h \
		arki/dataset/maintenance.h \
		arki/dataset/memory.h \
		arki/dataset/merged.h \
		arki/dataset/ondisk2.h \
		arki/dataset/ondisk2/reader.h \
		arki/dataset/ondisk2/test-utils.h \
		arki/dataset/ondisk2/writer.h \
		arki/dataset/outbound.h \
		arki/dataset/offline.h \
		arki/datasetpool.h \
		arki/dataset/simple/datafile.h \
		arki/dataset/simple/reader.h \
		arki/dataset/simple/writer.h \
		arki/dataset/step.h \
		arki/dataset/test-scenario.h \
		arki/dataset/tests.h \
		arki/dispatcher.h \
		arki/emitter.h \
		arki/emitter/json.h \
		arki/emitter/memory.h \
		arki/formatter.h \
		arki/validator.h \
		arki/iotrace.h \
		arki/itemset.h \
		arki/matcher/area.h \
		arki/matcher.h \
		arki/matcher/level.h \
		arki/matcher/origin.h \
		arki/matcher/proddef.h \
		arki/matcher/product.h \
		arki/matcher/quantity.h \
		arki/matcher/reftime.h \
		arki/matcher/reftime/parser.h \
		arki/matcher/run.h \
		arki/matcher/source.h \
		arki/matcher/task.h \
		arki/matcher/tests.h \
		arki/matcher/timerange.h \
		arki/matcher/utils.h \
		arki/metadata/collection.h \
		arki/metadata/clusterer.h \
		arki/metadata/xargs.h \
		arki/metadatagrid.h \
		arki/metadata.h \
		arki/metadata/stream.h \
		arki/metadata/tests.h \
		arki/metadata/test-generator.h \
		arki/nag.h \
		arki/postprocess.h \
		arki/querymacro.h \
		arki/runtime/config.h \
		arki/runtime.h \
		arki/runtime/io.h \
		arki/runtime/processor.h \
		arki/scan/any.h \
		arki/scan/dir.h \
		arki/sort.h \
		arki/summary.h \
		arki/summary/intern.h \
		arki/summary/table.h \
		arki/summary/codec.h \
		arki/summary/stats.h \
		arki/targetfile.h \
		arki/tests/tests.h \
		arki/transaction.h \
		arki/types/area.h \
		arki/types/assigneddataset.h \
		arki/types/bbox.h \
		arki/types.h \
		arki/types-init.h \
		arki/types/level.h \
		arki/types/note.h \
		arki/types/origin.h \
		arki/types/proddef.h \
		arki/types/product.h \
		arki/types/quantity.h \
		arki/types/reftime.h \
		arki/types/run.h \
		arki/types/source.h \
		arki/types/source/blob.h \
		arki/types/source/inline.h \
		arki/types/source/url.h \
		arki/types/task.h \
		arki/types.tcc \
		arki/types/tests.h \
		arki/types/time.h \
		arki/types/timerange.h \
		arki/types/utils.h \
		arki/types/value.h \
		arki/types/typevector.h \
		arki/types/typeset.h \
		arki/utils/accounting.h \
		arki/utils/gzip.h \
		arki/utils/compress.h \
		arki/utils/datareader.h \
		arki/utils/files.h \
		arki/utils/geosdef.h \
		arki/utils/geosfwd.h \
		arki/utils/h5.h \
		arki/utils.h \
		arki/utils/intern.h \
		arki/utils/pcounter.h \
		arki/utils/process.h \
		arki/utils/regexp.h \
		arki/utils/sqlite.h \
		arki/utils/raii.h \
		arki/utils/string.h \
		arki/utils/sys.h \
		arki/utils/yaml.h \
		arki/utils/commandline/core.h \
		arki/utils/commandline/doc.h \
		arki/utils/commandline/engine.h \
		arki/utils/commandline/options.h \
		arki/utils/commandline/parser.h \
		arki/utils/commandline/wordwrap.h \
		arki/utils/net/mime.h \
		arki/utils/net/server.h \
		arki/utils/net/http.h \
		arki/values.h

BUILT_SOURCES = \
		$(srcdir)/arki/matcher/reftime/reftime-lex.h \
		$(srcdir)/arki/matcher/reftime/reftime-lex.cc \
		$(srcdir)/arki/matcher/reftime/reftime-parse.hh \
		$(srcdir)/arki/matcher/reftime/reftime-parse.cc

all_built_sources: $(BUILT_SOURCES)

%-lex.h %-lex.cc: %-lex.ll
	$(srcdir)/arki/buildflex $<

%-parse.hh %-parse.cc: %-parse.yy
	$(srcdir)/arki/buildbison $<

lib_LTLIBRARIES += arki/libarkimet.la

# Build these incrementally
arki_libarkimet_la_CPPFLAGS = $(AM_CPPFLAGS) -Werror
arki_libarkimet_la_SOURCES =
arki_libarkimet_la_LDFLAGS = -version-info @LIBARKI_VERSION_INFO@ $(SQLITE3_LDFLAGS) $(ZLIB_LIBS) $(DEVEL_LIBS) -lpthread

arki_tests_arki_test_SOURCES = arki/tests/tut-main.cpp
arki_tests_arki_test_LDADD =


#
# Core functions
#
if DEVEL_DO_TYPES
arki_libarkimet_la_SOURCES += \
		arki/wibble/sys/fs.cpp \
		arki/wibble/sys/filelock.cpp \
		arki/wibble/sys/process.cpp \
		arki/wibble/sys/childprocess.cpp \
		arki/wibble/sys/exec.cpp \
		arki/wibble/sys/signal.cpp \
		arki/wibble/log/file.cpp \
		arki/wibble/log/stream.cpp \
		arki/wibble/log/ostream.cpp \
		arki/wibble/log/syslog.cpp \
		arki/wibble/log/filters.cpp \
		arki/wibble/grcal/grcal.cpp \
		arki/exceptions.cc \
		arki/nag.cc \
		arki/utils.cc \
		arki/utils/accounting.cc \
		arki/utils/gzip.cc \
		arki/utils/files.cc \
		arki/utils/intern.cc \
		arki/utils/pcounter.cc \
		arki/utils/process.cc \
		arki/utils/raii.cc \
		arki/utils/regexp.cc \
		arki/utils/string.cc \
		arki/utils/sys.cc \
		arki/utils/yaml.cc \
		arki/utils/commandline/core.cpp \
		arki/utils/commandline/options.cpp \
		arki/utils/commandline/doc.cpp \
		arki/utils/commandline/parser.cpp \
		arki/utils/commandline/engine.cpp \
		arki/utils/commandline/wordwrap.cpp \
		arki/utils/net/mime.cpp \
		arki/utils/net/server.cpp \
		arki/utils/net/http.cpp \
		arki/binary.cc \
		arki/file.cc \
		arki/configfile.cc \
		arki/transaction.cc \
		arki/emitter.cc \
		arki/emitter/memory.cc \
		arki/emitter/json.cc \
		arki/tests/tests.cc \
		arki/values.cc \
		arki/bbox.cc \
		arki/types.cc \
		arki/types/utils.cc \
		arki/types/source.cc \
		arki/types/source/blob.cc \
		arki/types/source/inline.cc \
		arki/types/source/url.cc \
		arki/types/time.cc \
		arki/types/reftime.cc \
		arki/types/origin.cc \
		arki/types/product.cc \
		arki/types/level.cc \
		arki/types/timerange.cc \
		arki/types/area.cc \
		arki/types/proddef.cc \
		arki/types/note.cc \
		arki/types/assigneddataset.cc \
		arki/types/bbox.cc \
		arki/types/run.cc \
		arki/types/task.cc \
		arki/types/quantity.cc \
		arki/types/value.cc \
		arki/types/typevector.cc \
		arki/types/typeset.cc \
		arki/itemset.cc \
		arki/matcher.cc \
		arki/matcher/utils.cc \
		arki/matcher/origin.cc \
		arki/matcher/product.cc \
		arki/matcher/level.cc \
		arki/matcher/timerange.cc \
		arki/matcher/reftime.cc \
		arki/matcher/reftime/parser.cc \
		arki/matcher/reftime/reftime-lex.cc \
		arki/matcher/reftime/reftime-parse.cc \
		arki/matcher/area.cc \
		arki/matcher/proddef.cc \
		arki/matcher/run.cc \
		arki/matcher/source.cc \
		arki/matcher/task.cc \
		arki/matcher/quantity.cc \
		arki/runtime/config.cc \
		arki/runtime/io.cc \
		arki/types-init.cc

arki_tests_arki_test_SOURCES += \
		arki/tests/tests-tut.cc \
		arki/types/tests.cc \
		arki/utils/tests.cc \
		arki/utils-test.cc \
		arki/utils/accounting-tut.cc \
		arki/utils/gzip-test.cc \
		arki/utils/yaml-test.cc \
		arki/utils/files-tut.cc \
		arki/utils/intern-tut.cc \
		arki/utils/pcounter-tut.cc \
		arki/utils/process-tut.cc \
		arki/utils/raii-tut.cc \
		arki/utils/regexp-test.cc \
		arki/binary-test.cc \
		arki/file-test.cc \
		arki/configfile-test.cc \
		arki/transaction-test.cc \
		arki/emitter/memory-tut.cc \
		arki/emitter/json-tut.cc \
		arki/values-test.cc \
		arki/types-test.cc \
		arki/types/utils-test.cc \
		arki/types/source-test.cc \
		arki/types/time-test.cc \
		arki/types/reftime-test.cc \
		arki/types/origin-test.cc \
		arki/types/product-test.cc \
		arki/types/level-test.cc \
		arki/types/timerange-test.cc \
		arki/types/area-test.cc \
		arki/types/proddef-test.cc \
		arki/types/note-test.cc \
		arki/types/assigneddataset-test.cc \
		arki/types/bbox-test.cc \
		arki/types/run-test.cc \
		arki/types/task-test.cc \
		arki/types/quantity-test.cc \
		arki/types/value-test.cc \
		arki/types/typevector-test.cc \
		arki/types/typeset-test.cc \
		arki/itemset-tut.cc

if IOTRACE
arki_libarkimet_la_SOURCES += arki/iotrace.cc
arki_tests_arki_test_SOURCES += arki/iotrace-test.cc
endif

if LZO
arki_libarkimet_la_LDFLAGS += $(LZO_LIBS)
endif

if LUA
nobase_dist_arkiinclude_HEADERS += arki/report.h arki/formatter/lua.h arki/utils/lua.h arki/tests/lua.h
arki_libarkimet_la_SOURCES += arki/formatter/lua.cc arki/utils/lua.cc
arki_tests_arki_test_SOURCES += arki/formatter/lua-tut.cc arki/utils/lua-tut.cc arki/tests/lua.cc
arki_libarkimet_la_LDFLAGS += $(LUA_LIBS)
arki_tests_arki_test_LDADD += $(LUA_LIBS)
endif

if GEOS
arki_libarkimet_la_LDFLAGS += $(GEOS_LIBS)
endif

if VM2
nobase_dist_arkiinclude_HEADERS += arki/scan/vm2.h arki/utils/vm2.h
arki_libarkimet_la_SOURCES += arki/utils/vm2.cc
arki_libarkimet_la_LDFLAGS += $(VM2_LIBS)
arki_tests_arki_test_SOURCES += arki/utils/vm2-tut.cc
endif

endif

#
# Metadata types
#
if DEVEL_DO_METADATA
arki_libarkimet_la_SOURCES += \
		arki/utils/h5.cc \
		arki/utils/datareader.cc \
		arki/utils/compress.cc \
		arki/utils/sqlite.cc \
		arki/metadata.cc \
		arki/metadata/collection.cc \
		arki/metadata/stream.cc \
		arki/metadata/clusterer.cc \
		arki/metadata/test-generator.cc \
		arki/summary/stats.cc \
		arki/summary/intern.cc \
		arki/summary/table.cc \
		arki/summary/codec.cc \
		arki/summary.cc \
		arki/sort.cc \
		arki/formatter.cc \
		arki/validator.cc \
		arki/postprocess.cc \
		arki/scan/any.cc \
		arki/scan/dir.cc

arki_tests_arki_test_SOURCES += \
		arki/metadata/tests.cc \
		arki/matcher/tests.cc \
		arki/utils/datareader-tut.cc \
		arki/utils/compress-tut.cc \
		arki/utils/sqlite-tut.cc \
		arki/sort-test.cc \
		arki/formatter-tut.cc \
		arki/validator-test.cc \
		arki/bbox-test.cc \
		arki/metadata-test.cc \
		arki/metadata/collection-tut.cc \
		arki/metadata/clusterer-tut.cc \
		arki/metadata/stream-tut.cc \
		arki/metadata/test-generator-tut.cc \
		arki/summary-test.cc \
		arki/summary/intern-tut.cc \
		arki/summary/table-tut.cc \
		arki/summary/stats-tut.cc \
		arki/matcher-test.cc \
		arki/matcher/utils-tut.cc \
		arki/matcher/origin-tut.cc \
		arki/matcher/product-tut.cc \
		arki/matcher/level-tut.cc \
		arki/matcher/timerange-tut.cc \
		arki/matcher/reftime-tut.cc \
		arki/matcher/reftime/parser-tut.cc \
		arki/matcher/area-tut.cc \
		arki/matcher/proddef-tut.cc \
		arki/matcher/run-tut.cc \
		arki/matcher/source-tut.cc \
		arki/matcher/task-tut.cc \
		arki/matcher/quantity-tut.cc \
		arki/scan/any-tut.cc \
		arki/scan/dir-tut.cc \
		arki/runtime/io-tut.cc \
		arki/runtime/config-tut.cc

if GRIBAPI
nobase_dist_arkiinclude_HEADERS += arki/scan/grib.h
arki_libarkimet_la_SOURCES += arki/scan/grib.cc
arki_tests_arki_test_SOURCES += arki/scan/grib-tut.cc
arki_libarkimet_la_LDFLAGS += $(GRIBAPI_LIBS) $(LUA_LIBS)
endif

if DBALLE
nobase_dist_arkiinclude_HEADERS += arki/scan/bufr.h
arki_libarkimet_la_SOURCES += arki/scan/bufr.cc
arki_libarkimet_la_LDFLAGS += $(DBALLE_LIBS)
arki_tests_arki_test_SOURCES += arki/scan/bufr-tut.cc
if LUA
nobase_dist_arkiinclude_HEADERS += arki/scan/bufrlua.h
arki_libarkimet_la_SOURCES += arki/scan/bufrlua.cc
endif
endif

if HDF5
nobase_dist_arkiinclude_HEADERS += arki/scan/odimh5.h
arki_libarkimet_la_SOURCES += arki/scan/odimh5.cc
arki_libarkimet_la_LDFLAGS += $(HDF5_LIBS)
arki_tests_arki_test_SOURCES += arki/utils/h5-tut.cc arki/scan/odimh5-tut.cc
endif

if NETCDF
nobase_dist_arkiinclude_HEADERS += arki/scan/nc.h
arki_libarkimet_la_SOURCES += arki/scan/nc.cc
arki_libarkimet_la_LDFLAGS += $(NETCDF_LIBS)
arki_tests_arki_test_SOURCES += arki/scan/nc-tut.cc
endif

if VM2
arki_libarkimet_la_SOURCES += arki/scan/vm2.cc
arki_tests_arki_test_SOURCES += arki/scan/vm2-test.cc
endif

endif

#
# Datasets
#
if DEVEL_DO_DATASETS
arki_libarkimet_la_SOURCES += \
		arki/metadata/xargs.cc \
		arki/targetfile.cc \
		arki/dataset.cc \
		arki/dataset/reporter.cc \
		arki/dataset/segment.cc \
		arki/dataset/segment/fd.cc \
		arki/dataset/segment/concat.cc \
		arki/dataset/segment/lines.cc \
		arki/dataset/segment/dir.cc \
		arki/dataset/maintenance.cc \
		arki/dataset/memory.cc \
		arki/dataset/index.cc \
		arki/dataset/index/base.cc \
		arki/dataset/index/attr.cc \
		arki/dataset/index/manifest.cc \
		arki/dataset/index/aggregate.cc \
		arki/dataset/index/summarycache.cc \
		arki/dataset/index/contents.cc \
		arki/dataset/step.cc \
		arki/dataset/local.cc \
		arki/dataset/segmented.cc \
		arki/dataset/indexed.cc \
		arki/dataset/file.cc \
		arki/dataset/archive.cc \
		arki/dataset/simple/datafile.cc \
		arki/dataset/simple/reader.cc \
		arki/dataset/simple/writer.cc \
		arki/dataset/ondisk2.cc \
		arki/dataset/ondisk2/reader.cc \
		arki/dataset/ondisk2/writer.cc \
		arki/dataset/http/server.cc \
		arki/dataset/http/inbound.cc \
		arki/dataset/outbound.cc \
		arki/dataset/offline.cc \
		arki/dataset/discard.cc \
		arki/dataset/empty.cc \
		arki/dataset/merged.cc \
		arki/dataset/gridquery.cc \
		arki/dataset/test-scenario.cc \
		arki/datasetpool.cc \
		arki/metadatagrid.cc \
		arki/dispatcher.cc \
		arki/querymacro.cc

arki_tests_arki_test_SOURCES += \
		arki/targetfile-tut.cc \
		arki/metadata/xargs-tut.cc \
		arki/dataset/tests.cc \
		arki/dataset/reporter-test.cc \
		arki/dataset/segment-tut.cc \
		arki/dataset/segment/tests.cc \
		arki/dataset/segment/fd-tut.cc \
		arki/dataset/segment/concat-tut.cc \
		arki/dataset/segment/lines-tut.cc \
		arki/dataset/segment/dir-tut.cc \
		arki/dataset/maintenance-test.cc \
		arki/dataset/memory-tut.cc \
		arki/dataset/index-test.cc \
		arki/dataset/index/base-tut.cc \
		arki/dataset/index/attr-tut.cc \
		arki/dataset/index/manifest-test.cc \
		arki/dataset/index/aggregate-tut.cc \
		arki/dataset/index/summarycache-tut.cc \
		arki/dataset/index/contents-tut.cc \
		arki/dataset/step-test.cc \
		arki/dataset/local-test.cc \
		arki/dataset/segmented-test.cc \
		arki/dataset/indexed-test.cc \
		arki/dataset/file-tut.cc \
		arki/dataset/archive-test.cc \
		arki/dataset/simple/datafile-tut.cc \
		arki/dataset/simple/reader-test.cc \
		arki/dataset/simple/writer-test.cc \
		arki/dataset/ondisk2-tut.cc \
		arki/dataset/ondisk2/reader-tut.cc \
		arki/dataset/ondisk2/writer-test.cc \
		arki/dataset/ondisk2/test-utils.cc \
		arki/dataset/http/server-test.cc \
		arki/dataset/http/inbound-test.cc \
		arki/dataset/http/test-utils.cc \
		arki/dataset/outbound-tut.cc \
		arki/dataset/offline-test.cc \
		arki/dataset/discard-tut.cc \
		arki/dataset/empty-tut.cc \
		arki/dataset/merged-tut.cc \
		arki/dataset/gridquery-tut.cc \
		arki/dataset/test-scenario-test.cc \
		arki/dataset-test.cc \
		arki/dataset-reader-test.cc \
		arki/dataset-writer-test.cc \
		arki/dataset-checker-test.cc \
		arki/metadatagrid-test.cc \
		arki/postprocess-tut.cc \
		arki/querymacro-tut.cc \
		arki/datasetpool-tut.cc \
		arki/dispatcher-tut.cc \
		arki/runtime/processor-test.cc \
		arki/runtime-test.cc

if CURL
nobase_dist_arkiinclude_HEADERS += arki/dataset/http.h
arki_libarkimet_la_SOURCES += arki/dataset/http.cc
arki_libarkimet_la_LDFLAGS += @LIBCURL@
arki_tests_arki_test_SOURCES += arki/dataset/http-tut.cc
endif

if LUA
arki_libarkimet_la_SOURCES += arki/report.cc
arki_tests_arki_test_SOURCES += arki/report-tut.cc
endif

endif

#
# Runtime
#
if DEVEL_DO_RUNTIME
arki_libarkimet_la_SOURCES += \
		arki/runtime/processor.cc \
		arki/runtime.cc
endif

if GCOV
export ARKI_HAS_GCOV=yes
endif

# Tests
check_PROGRAMS += arki/tests/arki-test

arki_tests_arki_test_LDADD += arki/libarkimet.la $(SQLITE3_LDFLAGS) $(DEVEL_LIBS) -lpthread

TESTS += arki/runtest

EXTRA_DIST += arki/runtest arki/buildbison arki/buildflex \
	      arki/matcher/reftime/reftime-lex.ll \
	      arki/matcher/reftime/reftime-parse.yy \
	      $(all_built_sources)
