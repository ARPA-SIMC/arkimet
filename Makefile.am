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

arki-xargs.1: src/arki-xargs doc/mhooks-xargs
	COLUMNS=200 $(srcdir)/run-local help2man --include doc/mhooks-xargs --name='For every item of data read from standard input, save it on a temporary file and run "command [initial-arguments] filename" on it' --section=1 --no-info --version-string="$(PACKAGE_VERSION)" $< > $@ || rm -f $@

arki-query.1: src/arki-query doc/mhooks-query
	COLUMNS=200 $(srcdir)/run-local help2man --include doc/mhooks-query --name='Query the datasets in the given config file for data matching the given expression, and output the matching metadata.' --section=1 --no-info --version-string="$(PACKAGE_VERSION)" $< > $@ || rm -f $@

arki-scan.1: src/arki-scan doc/mhooks-scan
	COLUMNS=200 $(srcdir)/run-local help2man --include doc/mhooks-scan --name='Query the datasets in the given config file for data matching the given expression, and output the matching metadata.' --section=1 --no-info --version-string="$(PACKAGE_VERSION)" $< > $@ || rm -f $@

arki-check.1: src/arki-check doc/mhooks-check
	COLUMNS=200 $(srcdir)/run-local help2man --include doc/mhooks-check --name='Perform maintenance on arkimet datasets.' --section=1 --no-info --version-string="$(PACKAGE_VERSION)" $< > $@ || rm -f $@

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
