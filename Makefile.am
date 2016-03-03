## Process this file with automake to produce Makefile.in

ACLOCAL_AMFLAGS = -I m4

EXTRA_DIST = \
	     README \
	     README.Cineca \
	     update-tags \
	     autogen.sh

CLEANFILES = $(man_MANS)

dist_noinst_HEADERS =
noinst_LTLIBRARIES =
noinst_PROGRAMS =

# SUBDIRS = conf embedded arki src doc test
include $(srcdir)/conf/Makefile.am
include $(srcdir)/embedded/Makefile.am

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
