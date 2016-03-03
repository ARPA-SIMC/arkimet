## Process this file with automake to produce Makefile.in

ACLOCAL_AMFLAGS = -I m4

AM_CPPFLAGS = -I$(top_srcdir) -I$(top_builddir) $(SQLITE3_CFLAGS) -DCONF_DIR=\"$(confdir)\" -DPOSTPROC_DIR=\"$(postprocdir)\" $(ZLIB_CFLAGS)
AM_CXXFLAGS = $(EXTRA_CXXFLAGS) $(DEVEL_CXXFLAGS)
AM_LDFLAGS = $(EXTRA_LDFLAGS) -lpthread

if FILE_OFFSET_BITS_64
AM_CPPFLAGS += -D_FILE_OFFSET_BITS=64
endif
if LZO
AM_CPPFLAGS += $(LZO_CFLAGS)
endif
if LUA
AM_CPPFLAGS += $(LUA_CFLAGS)
endif
if GEOS
AM_CPPFLAGS += $(GEOS_CFLAGS)
endif
if VM2
AM_CPPFLAGS += $(VM2_CFLAGS)
endif
if GRIBAPI
AM_CPPFLAGS += $(GRIBAPI_CFLAGS) $(LUA_CFLAGS)
endif
if DBALLE
AM_CPPFLAGS += $(DBALLE_CFLAGS)
endif
if HDF5
AM_CPPFLAGS += $(HDF5_CFLAGS)
endif
if NETCDF
AM_CPPFLAGS += $(NETCDF_CFLAGS)
endif
if CURL
AM_CPPFLAGS += @LIBCURL_CPPFLAGS@
endif

EXTRA_DIST = \
	     README \
	     README.Cineca \
	     update-tags \
	     autogen.sh

CLEANFILES = $(man_MANS)

dist_noinst_HEADERS =
noinst_LTLIBRARIES =
noinst_PROGRAMS =
lib_LTLIBRARIES =
check_PROGRAMS =
bin_PROGRAMS =
doc_DATA =
dist_doc_DATA =

TESTS =
LOG_COMPILER = ./arki/runtest

# SUBDIRS = conf embedded arki src doc test
include $(srcdir)/conf/Makefile.am
include $(srcdir)/embedded/Makefile.am
include $(srcdir)/arki/Makefile.am
include $(srcdir)/src/Makefile.am
include $(srcdir)/doc/Makefile.am
include $(srcdir)/test

man_MANS = \
	arki-dump.1 \
	arki-gzip.1 \
	arki-query.1 \
	arki-check.1 \
	arki-inbound.1 \
	arki-mergeconf.1 \
	arki-scan.1 \
	arki-server.1 \
	arki-xargs.1

if READLINE
man_MANS += arki-config.1
endif

if DBALLE
man_MANS += arki-bufr-prepare.1
endif

arki-%.1: src/arki-% doc/mhooks-%
	$< --manpage="$(word 2,$^)" > $@
