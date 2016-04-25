## Process this file with automake to produce Makefile.in

TESTS_ENVIRONMENT = $(top_srcdir)/arki/runtest

#TESTS = test.py

pkgpython_PYTHON = arkimet/__init__.py

pyexec_LTLIBRARIES = _arkimet.la

_arkimet_la_SOURCES = \
    common.cc \
    arkimet.cc
_arkimet_la_CPPFLAGS = $(PYTHON_CFLAGS) -I$(top_srcdir)
_arkimet_la_LDFLAGS = -module -avoid-version -export-symbols-regex init_arkimet
_arkimet_la_LIBADD = ../arki/libarkimet.la

#doc_DATA = python-arkimet.rst

_arkimet.so: _arkimet.la
	test -e .libs/_arkimet.so || ( echo ".libs/_arkimet.so not built" >&2 ; exit 1 )
	ln -fs .libs/_arkimet.so $@

#python-arkimet.rst: doc-arkimet $(pkgpython_PYTHON) _arkimet.so
#	$(PYTHON) $(srcdir)/doc-arkimet > $@ || (rm -f $@; false)
#
#python-arkimet.html: python-arkimet.rst
#	rst2html $< > $@ || (rm -f $@; false)

clean-local:
	rm -f _arkimet.so
	rm -rf __pycache__ arkimet/__pycache__

EXTRA_DIST = \
    common.h \
    test.py
#    doc-arkimet \
#    record.h \
#    db.h \
#    cursor.h \
#    testlib.py \
#    $(arkimet_python_tests)

check-local: _arkimet.so
	PYTHON="$(PYTHON)" $(TESTS_ENVIRONMENT) nose2-3

#CLEANFILES = python-arkimet.rst python-arkimet.html
