## Process this file with automake to produce Makefile.in

ACLOCAL_AMFLAGS = -I m4

SUBDIRS = conf arki
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

arki-mergeconf.1: src/arki-mergeconf doc/mhooks-arki-mergeconf
	COLUMNS=200 $(srcdir)/run-local help2man --include doc/mhooks-arki-mergeconf --name='Build a single Arkimet configuration file merging configurations from multiple sources' --section=1 --no-info --version-string="$(PACKAGE_VERSION)" $< > $@ || rm -f $@

arki-server.1: src/arki-server
	COLUMNS=200 $(srcdir)/run-local help2man --name='Arkimet server for remote dataset access' --section=1 --no-info --version-string="$(PACKAGE_VERSION)" $< > $@ || rm -f $@

arki-testtar.1: src/arki-testtar
	COLUMNS=200 $(srcdir)/run-local help2man --name='Build a .tar version of a dataset for testing, without storing the actual data' --section=1 --no-info --version-string="$(PACKAGE_VERSION)" $< > $@ || rm -f $@

arki-bufr-prepare.1: src/arki-bufr-prepare doc/mhooks-arki-bufr-prepare
	COLUMNS=200 $(srcdir)/run-local help2man --include doc/mhooks-arki-bufr-prepare --name='Read BUFR messages, and encode each subsection in a separate message' --section=1 --no-info --version-string="$(PACKAGE_VERSION)" $< > $@ || rm -f $@

arki-dump.1: src/arki-dump doc/mhooks-arki-dump
	COLUMNS=200 $(srcdir)/run-local help2man --include doc/mhooks-arki-dump --name='Read data from the given input file (or stdin), and dump the in human readable format on stdout' --section=1 --no-info --version-string="$(PACKAGE_VERSION)" $< > $@ || rm -f $@

arki-xargs.1: src/arki-xargs doc/mhooks-arki-xargs
	COLUMNS=200 $(srcdir)/run-local help2man --include doc/mhooks-arki-xargs --name='For every item of data read from standard input, save it on a temporary file and run "command [initial-arguments] filename" on it' --section=1 --no-info --version-string="$(PACKAGE_VERSION)" $< > $@ || rm -f $@

arki-query.1: src/arki-query doc/mhooks-arki-query
	COLUMNS=200 $(srcdir)/run-local help2man --include doc/mhooks-arki-query --name='Query the datasets in the given config file for data matching the given expression, and output the matching metadata.' --section=1 --no-info --version-string="$(PACKAGE_VERSION)" $< > $@ || rm -f $@

arki-scan.1: src/arki-scan doc/mhooks-arki-scan
	COLUMNS=200 $(srcdir)/run-local help2man --include doc/mhooks-arki-scan --name='Reads messages from files and datasets then outputs it or imports it into arkimet datasets.' --section=1 --no-info --version-string="$(PACKAGE_VERSION)" $< > $@ || rm -f $@

arki-check.1: src/arki-check doc/mhooks-arki-check
	COLUMNS=200 $(srcdir)/run-local help2man --include doc/mhooks-arki-check --name='Perform maintenance on arkimet datasets.' --section=1 --no-info --version-string="$(PACKAGE_VERSION)" $< > $@ || rm -f $@

#%.1: tools/%
#	( cd tools && ./`basename $<` help manpage ) > $@

EXTRA_DIST = \
	     README.md \
	     NEWS.md \
	     update-tags \
	     autogen.sh \
	     run-local

CLEANFILES = $(man_MANS)
