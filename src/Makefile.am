## Process this file with automake to produce Makefile.in

bin_PROGRAMS = arki-dump arki-mergeconf arki-xargs
dist_bin_SCRIPTS = arki-query arki-scan arki-check arki-server arki-testtar
noinst_PROGRAMS = arki-test

all_libs = ../arki/libarkimet.la $(LIBGRIB_LIBS) $(DBALLE_LIBS) $(SQLITE3_LDFLAGS) $(GRIBAPI_LIBS) $(LUA_LIBS) $(ZLIB_LIBS) $(DEVEL_LIBS) -lpthread

AM_CPPFLAGS = -I$(top_srcdir) -I$(top_builddir) $(LIBGRIB_CFLAGS) $(DBALLE_CFLAGS) $(SQLITE3_CFLAGS) $(GRIBAPI_CFLAGS) $(LUA_CFLAGS) $(ZLIB_CFLAGS) $(DEVEL_CFLAGS) -DSERVER_DIR=\"$(serverdir)\" $(EXTRA_CXXFLAGS) -Werror
AM_LDFLAGS = $(EXTRA_LDFLAGS)

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

if DBALLE
bin_PROGRAMS += arki-bufr-prepare

arki_bufr_prepare_SOURCES = arki-bufr-prepare.cc
arki_bufr_prepare_LDADD = $(all_libs)
endif

arki_mergeconf_SOURCES = arki-mergeconf.cc
arki_mergeconf_LDADD = $(all_libs)

arki_dump_SOURCES = arki-dump.cc
arki_dump_LDADD = $(all_libs)

arki_xargs_SOURCES = arki-xargs.cc
arki_xargs_LDADD = $(all_libs)

arki_test_SOURCES = arki-test.cc
arki_test_LDADD = $(all_libs)

test_scripts = test-arki-xargs test-arki-mergeconf run-test-src
check_PROGRAMS = test-src

check-local:
	for test in $(test_scripts); do \
		$(srcdir)/$$test ; \
	done

test_src_SOURCES = arki-server-test.cc
test_src_LDADD = $(all_libs) ../arki/libarkimet-test.la

EXTRA_DIST = testlib.sh $(test_scripts)
