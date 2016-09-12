## Process this file with automake to produce Makefile.in

ACLOCAL_AMFLAGS = -I m4

SUBDIRS = conf embedded arki
if DO_PYTHON
SUBDIRS += python
endif
SUBDIRS += src doc test

man_MANS = \
	arki-dump.1 \
	arki-gzip.1 \
	arki-query.1 \
	arki-check.1 \
	arki-mergeconf.1 \
	arki-scan.1 \
	arki-server.1 \
	arki-xargs.1

if DBALLE
man_MANS += arki-bufr-prepare.1
endif

arki-server.1: src/arki-server
	COLUMNS=200 $(srcdir)/run-local help2man --name='Arkimet server for remote dataset access' --section=1 --no-info --version-string="$(PACKAGE_VERSION)" $< > $@ || rm -f $@


arki-%.1: src/arki-% doc/mhooks-%
	$(srcdir)/run-local $< --manpage="$(word 2,$^)" > $@ || rm -f $@

#%.1: tools/%
#	( cd tools && ./`basename $<` help manpage ) > $@

EXTRA_DIST = \
	     README \
	     README.Cineca \
	     NEWS.md \
	     update-tags \
	     autogen.sh

CLEANFILES = $(man_MANS)
