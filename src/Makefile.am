## Process this file with automake to produce Makefile.in

bin_PROGRAMS = arki-check arki-query arki-dump arki-mergeconf arki-benchmark arki-scan arki-xargs

dist_bin_SCRIPTS = arki-server

dist_noinst_HEADERS = bench/benchmark.h

all_libs = ../wibble/libwibble.la $(LIBGRIB_LIBS) $(LIBDBALLEPP_LIBS) $(SQLITE3_LDFLAGS) $(GRIBAPI_LIBS) $(LUA_LIBS)

INCLUDES = -I$(top_srcdir) -I$(top_builddir) $(LIBGRIB_CFLAGS) $(LIBDBALLEPP_CFLAGS) $(SQLITE3_CFLAGS) $(GRIBAPI_CFLAGS) $(LUA_CFLAGS)

if GEOS
all_libs += $(GEOS_LIBS)
INCLUDES += $(GEOS_CFLAGS)
endif

# if LIBGRIB
if CURL
all_libs += @LIBCURL@
INCLUDES += @LIBCURL_CPPFLAGS@
endif

if READLINE
bin_PROGRAMS += arki-config
arki_config_SOURCES = arki-config.cc
arki_config_LDFLAGS = ../arki/libarkimet.la $(all_libs)
endif

if DBALLE
bin_PROGRAMS += arki-bufr-prepare

arki_bufr_prepare_SOURCES = arki-bufr-prepare.cc
arki_bufr_prepare_LDFLAGS = ../arki/libarkimet.la $(all_libs)
endif

arki_scan_SOURCES = arki-scan.cc
arki_scan_LDFLAGS = ../arki/libarkimet.la $(all_libs)

arki_benchmark_SOURCES = arki-benchmark.cc bench/benchmark.cc
arki_benchmark_LDFLAGS = ../arki/libarkimet.la $(all_libs)

arki_mergeconf_SOURCES = arki-mergeconf.cc
arki_mergeconf_LDFLAGS = ../arki/libarkimet.la $(all_libs)

arki_query_SOURCES = arki-query.cc
arki_query_LDFLAGS = ../arki/libarkimet.la $(all_libs)

arki_check_SOURCES = arki-check.cc
arki_check_LDFLAGS = ../arki/libarkimet.la $(all_libs)

arki_dump_SOURCES = arki-dump.cc
arki_dump_LDFLAGS = ../arki/libarkimet.la $(all_libs)

arki_xargs_SOURCES = arki-xargs.cc
arki_xargs_LDFLAGS = ../arki/libarkimet.la $(all_libs)

TESTS = test-arki-server test-arki-scan run-test-src
check_PROGRAMS = test-src
test_src_SOURCES = \
		arki-server-tut.cc \
		../arki/tests/tut-main.cpp
test_src_LDADD = ../arki/libarkimet.la $(all_libs)

EXTRA_DIST=testlib.sh test-arki-server run-test-src
