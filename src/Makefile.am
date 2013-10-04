## Process this file with automake to produce Makefile.in

bin_PROGRAMS = arki-check arki-query arki-dump arki-gzip arki-mergeconf arki-benchmark arki-scan arki-xargs arki-server arki-inbound
noinst_PROGRAMS = arki-test

dist_noinst_HEADERS = bench/benchmark.h

all_libs = ../arki/libarkimet.la ../wibble/libwibble.la $(LIBGRIB_LIBS) $(DBALLE_LIBS) $(SQLITE3_LDFLAGS) $(GRIBAPI_LIBS) $(LUA_LIBS) $(ZLIB_LIBS) $(DEVEL_LIBS) -lpthread

AM_CPPFLAGS = -I$(top_srcdir) -I$(top_builddir) $(LIBGRIB_CFLAGS) $(DBALLE_CFLAGS) $(SQLITE3_CFLAGS) $(GRIBAPI_CFLAGS) $(LUA_CFLAGS) $(ZLIB_CFLAGS) $(DEVEL_CFLAGS) -DSERVER_DIR=\"$(serverdir)\"

if GEOS
all_libs += $(GEOS_LIBS)
AM_CPPFLAGS += $(GEOS_CFLAGS)
endif

# if LIBGRIB
if CURL
all_libs += @LIBCURL@
AM_CPPFLAGS += @LIBCURL_CPPFLAGS@
endif

if LZO
all_libs += $(LZO_LIBS)
AM_CPPFLAGS += $(LZO_CFLAGS)
endif

if READLINE
bin_PROGRAMS += arki-config
arki_config_SOURCES = arki-config.cc
arki_config_LDADD = $(all_libs)
endif

if DBALLE
bin_PROGRAMS += arki-bufr-prepare

arki_bufr_prepare_SOURCES = arki-bufr-prepare.cc
arki_bufr_prepare_LDADD = $(all_libs)
endif

if LUA
bin_PROGRAMS += arki-shell

arki_shell_SOURCES = arki-shell.cc
arki_shell_LDADD = $(all_libs)
endif

arki_scan_SOURCES = arki-scan.cc
arki_scan_LDADD = $(all_libs)

arki_inbound_SOURCES = arki-inbound.cc
arki_inbound_LDADD = $(all_libs)

arki_benchmark_SOURCES = arki-benchmark.cc bench/benchmark.cc
arki_benchmark_LDADD = $(all_libs)

arki_mergeconf_SOURCES = arki-mergeconf.cc
arki_mergeconf_LDADD = $(all_libs)

arki_query_SOURCES = arki-query.cc
arki_query_LDADD = $(all_libs)

arki_check_SOURCES = arki-check.cc
arki_check_LDADD = $(all_libs)

arki_dump_SOURCES = arki-dump.cc
arki_dump_LDADD = $(all_libs)

arki_xargs_SOURCES = arki-xargs.cc
arki_xargs_LDADD = $(all_libs)

arki_gzip_SOURCES = arki-gzip.cc
arki_gzip_LDADD = $(all_libs)

arki_server_SOURCES = arki-server.cc
arki_server_LDADD = $(all_libs)

arki_test_SOURCES = arki-test.cc
arki_test_LDADD = $(all_libs)

test_scripts = test-arki-server test-arki-scan test-arki-query test-arki-xargs run-test-src
check_PROGRAMS = test-src

check-local:
	for test in $(test_scripts); do \
		./$$test ; \
	done

test_src_SOURCES = \
		arki-server-tut.cc \
		../arki/tests/tut-main.cpp
test_src_LDADD = $(all_libs)

EXTRA_DIST = testlib.sh $(test_scripts)
