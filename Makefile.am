## Process this file with automake to produce Makefile.in

ACLOCAL_AMFLAGS = -I m4

SUBDIRS = conf embedded arki
if DO_PYTHON
SUBDIRS += python
endif
SUBDIRS += src doc test

man_MANS = \
	arki-dump.1 \
	arki-query.1 \
	arki-check.1 \
	arki-mergeconf.1 \
	arki-scan.1 \
	arki-server.1 \
	arki-testtar.1 \
	arki-xargs.1

if DBALLE
man_MANS += arki-bufr-prepare.1
endif

arki-mergeconf.1: src/arki-mergeconf doc/mhooks-mergeconf
	COLUMNS=200 $(srcdir)/run-local help2man --include doc/mhooks-mergeconf --name='Build a single Arkimet configuration file merging configurations from multiple sources' --section=1 --no-info --version-string="$(PACKAGE_VERSION)" $< > $@ || rm -f $@

arki-server.1: src/arki-server
	COLUMNS=200 $(srcdir)/run-local help2man --name='Arkimet server for remote dataset access' --section=1 --no-info --version-string="$(PACKAGE_VERSION)" $< > $@ || rm -f $@

arki-testtar.1: src/arki-testtar
	COLUMNS=200 $(srcdir)/run-local help2man --name='Build a .tar version of a dataset for testing, without storing the actual data' --section=1 --no-info --version-string="$(PACKAGE_VERSION)" $< > $@ || rm -f $@

arki-bufr-prepare.1: src/arki-bufr-prepare doc/mhooks-bufr-prepare
	COLUMNS=200 $(srcdir)/run-local help2man --include doc/mhooks-bufr-prepare --name='Read BUFR messages, and encode each subsection in a separate message' --section=1 --no-info --version-string="$(PACKAGE_VERSION)" $< > $@ || rm -f $@

arki-dump.1: src/arki-dump doc/mhooks-dump
	COLUMNS=200 $(srcdir)/run-local help2man --include doc/mhooks-dump --name='Read data from the given input file (or stdin), and dump the in human readable format on stdout' --section=1 --no-info --version-string="$(PACKAGE_VERSION)" $< > $@ || rm -f $@

arki-%.1: src/arki-% doc/mhooks-%
	$(srcdir)/run-local $< --manpage="$(word 2,$^)" > $@ || rm -f $@

#%.1: tools/%
#	( cd tools && ./`basename $<` help manpage ) > $@

EXTRA_DIST = \
	     README.md \
	     README.Cineca \
	     NEWS.md \
	     update-tags \
	     autogen.sh \
	     run-local

CLEANFILES = $(man_MANS)
