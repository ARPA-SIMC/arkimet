## Process this file with automake to produce Makefile.in

SUBDIRS = .

arkiincludedir = $(includedir)/arki

arkiinclude_HEADERS = \
		config.h \
		configfile.h \
		dataset.h \
		dataset/transaction.h \
		dataset/targetfile.h \
		dataset/local.h \
		dataset/file.h \
		dataset/ondisk.h \
		dataset/ondisk/common.h \
		dataset/ondisk/maintenance.h \
		dataset/ondisk/fetcher.h \
		dataset/ondisk/reader.h \
		dataset/ondisk/writer.h \
		dataset/ondisk/writer/datafile.h \
		dataset/ondisk/writer/directory.h \
		dataset/ondisk/maint/datafile.h \
		dataset/ondisk/maint/directory.h \
		dataset/ondisk/test-utils.h \
		dataset/outbound.h \
		dataset/discard.h \
		dataset/empty.h \
		dataset/merged.h \
		dataset/index.h \
		dataset/index/sqlite.h \
		dataset/index/base.h \
		dataset/index/attr.h \
		dataset/test-utils.h \
		datasetpool.h \
		dispatcher.h \
		matcher.h \
		matcher/test-utils.h \
		matcher/utils.h \
		matcher/origin.h \
		matcher/product.h \
		matcher/level.h \
		matcher/timerange.h \
		matcher/reftime.h \
		matcher/reftime/parser.h \
		matcher/area.h \
		matcher/ensemble.h \
		matcher/run.h \
		metadata.h \
		summary.h \
		summary-old.h \
		refcounted.h \
		runtime.h \
		types.h \
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
		types/ensemble.h \
		types/note.h \
		types/assigneddataset.h \
		types/bbox.h \
		types/run.h \
		sort.h \
		formatter.h \
		postprocess.h \
		values.h \
		utils.h

dist_noinst_HEADERS = \
		tests/test-utils.h \
		tests/metadata.h

BUILT_SOURCES = $(top_builddir)/arki/matcher/reftime/reftime-lex.h \
		$(top_builddir)/arki/matcher/reftime/reftime-lex.cc \
		$(top_builddir)/arki/matcher/reftime/reftime-parse.h \
		$(top_builddir)/arki/matcher/reftime/reftime-parse.cc

$(top_builddir)/arki/matcher/reftime/reftime-lex.h $(top_builddir)/arki/matcher/reftime/reftime-lex.cc: $(srcdir)/matcher/reftime/reftime-lex.ll
	$(srcdir)/buildflex $<

$(top_builddir)/arki/matcher/reftime/reftime-parse.h $(top_builddir)/arki/matcher/reftime/reftime-parse.cc: $(srcdir)/matcher/reftime/reftime-parse.yy
	$(srcdir)/buildbison $<

lib_LTLIBRARIES = libarkimet.la
libarkimet_la_SOURCES = \
		utils.cc \
		configfile.cc \
		values.cc \
		types.cc \
		types/source.cc \
		types/time.cc \
		types/reftime.cc \
		types/origin.cc \
		types/product.cc \
		types/level.cc \
		types/timerange.cc \
		types/area.cc \
		types/ensemble.cc \
		types/note.cc \
		types/assigneddataset.cc \
		types/bbox.cc \
		types/run.cc \
		sort.cc \
		formatter.cc \
		metadata.cc \
		summary.cc \
		summary-old.cc \
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
		matcher/ensemble.cc \
		matcher/run.cc \
		dataset.cc \
		dataset/index/sqlite.cc \
		dataset/index/base.cc \
		dataset/index/attr.cc \
		dataset/index.cc \
		dataset/transaction.cc \
		dataset/targetfile.cc \
		dataset/local.cc \
		dataset/file.cc \
		dataset/ondisk.cc \
		dataset/ondisk/common.cc \
		dataset/ondisk/maintenance.cc \
		dataset/ondisk/fetcher.cc \
		dataset/ondisk/reader.cc \
		dataset/ondisk/writer.cc \
		dataset/ondisk/writer/directory.cc \
		dataset/ondisk/writer/datafile.cc \
		dataset/ondisk/maint/directory.cc \
		dataset/ondisk/maint/datafile.cc \
		dataset/outbound.cc \
		dataset/discard.cc \
		dataset/empty.cc \
		dataset/merged.cc \
		datasetpool.cc \
		dispatcher.cc \
		postprocess.cc \
		runtime.cc
libarkimet_la_LDFLAGS = -version-info @LIBARKI_VERSION_INFO@ ../wibble/libwibble.la $(SQLITE3_LDFLAGS)

# Tests
TESTS = tests/arki-test
TESTS_ENVIRONMENT = $(top_srcdir)/arki/runtest
check_PROGRAMS = tests/arki-test
tests_arki_test_SOURCES = \
		utils-tut.cc \
		refcounted-tut.cc \
		configfile-tut.cc \
		values-tut.cc \
		types-tut.cc \
		types/source-tut.cc \
		types/time-tut.cc \
		types/reftime-tut.cc \
		types/origin-tut.cc \
		types/product-tut.cc \
		types/level-tut.cc \
		types/timerange-tut.cc \
		types/area-tut.cc \
		types/ensemble-tut.cc \
		types/note-tut.cc \
		types/assigneddataset-tut.cc \
		types/bbox-tut.cc \
		types/run-tut.cc \
		sort-tut.cc \
		formatter-tut.cc \
		metadata-tut.cc \
		summary-tut.cc \
		summary-old-tut.cc \
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
		matcher/ensemble-tut.cc \
		matcher/run-tut.cc \
		dataset/index/sqlite-tut.cc \
		dataset/index/base-tut.cc \
		dataset/index/attr-tut.cc \
		dataset/index-tut.cc \
		dataset/transaction-tut.cc \
		dataset/targetfile-tut.cc \
		dataset/local-tut.cc \
		dataset/file-tut.cc \
		dataset/ondisk-tut.cc \
		dataset/ondisk/common-tut.cc \
		dataset/ondisk/fetcher-tut.cc \
		dataset/ondisk/writer/datafile-tut.cc \
		dataset/ondisk/writer/directory-tut.cc \
		dataset/ondisk/maint/datafile-tut.cc \
		dataset/ondisk/maint/directory-tut.cc \
		dataset/ondisk/maintenance-tut.cc \
		dataset/ondisk/reader-tut.cc \
		dataset/ondisk/test-utils.cc \
		dataset/test-utils.cc \
		dataset/outbound-tut.cc \
		dataset/discard-tut.cc \
		dataset/empty-tut.cc \
		dataset/merged-tut.cc \
		dataset-tut.cc \
		postprocess-tut.cc \
		datasetpool-tut.cc \
		dispatcher-tut.cc \
		runtime-tut.cc \
		tests/tut-main.cpp
tests_arki_test_LDADD = libarkimet.la ../wibble/libwibble.la $(SQLITE3_LDFLAGS)

INCLUDES = -I$(top_srcdir) -I$(top_builddir) $(SQLITE3_CFLAGS) -DCONF_DIR=\"$(confdir)\" -DPOSTPROC_DIR=\"$(postprocdir)\"

if GRIBAPI
arkiinclude_HEADERS += scan/grib.h
libarkimet_la_SOURCES += scan/grib.cc
tests_arki_test_SOURCES += scan/grib-tut.cc
INCLUDES += $(GRIBAPI_CFLAGS) $(LUA_CFLAGS)
libarkimet_la_LDFLAGS += $(GRIBAPI_LIBS) $(LUA_LIBS)
tests_arki_test_LDADD += $(GRIBAPI_LIBS) $(LUA_LIBS)
endif

if LUA
arkiinclude_HEADERS += formatter/lua.h utils-lua.h report.h tests/lua.h
libarkimet_la_SOURCES += formatter/lua.cc utils-lua.cc report.cc
tests_arki_test_SOURCES += formatter/lua-tut.cc utils-lua-tut.cc report-tut.cc tests/lua.cc
INCLUDES += $(LUA_CFLAGS)
libarkimet_la_LDFLAGS += $(LUA_LIBS)
tests_arki_test_LDADD += $(LUA_LIBS)
endif

if DBALLE
arkiinclude_HEADERS += scan/bufr.h
libarkimet_la_SOURCES += scan/bufr.cc
tests_arki_test_SOURCES += scan/bufr-tut.cc
INCLUDES += $(LIBDBALLEPP_CFLAGS)
libarkimet_la_LDFLAGS += $(LIBDBALLEPP_LIBS)
tests_arki_test_LDADD += $(LIBDBALLEPP_LIBS)
endif

if CURL
arkiinclude_HEADERS += dataset/http.h
libarkimet_la_SOURCES += dataset/http.cc
tests_arki_test_SOURCES += dataset/http-tut.cc
INCLUDES += @LIBCURL_CPPFLAGS@
libarkimet_la_LDFLAGS += @LIBCURL@
tests_arki_test_LDADD += @LIBCURL@
endif

if GEOS
INCLUDES += $(GEOS_CFLAGS)
libarkimet_la_LDFLAGS += $(GEOS_LIBS)
tests_arki_test_LDADD += $(GEOS_LIBS)
endif

EXTRA_DIST = runtest buildbison buildflex \
	     matcher/reftime/reftime-lex.ll \
	     matcher/reftime/reftime-parse.yy
