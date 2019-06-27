## Process this file with automake to produce Makefile.in

TESTS_ENVIRONMENT = $(top_srcdir)/arki/runtest

#TESTS = test.py

nobase_python_PYTHON = arkimet/__init__.py arkimet/server/__init__.py arkimet/server/views.py

pyexec_LTLIBRARIES = _arkimet.la

_arkimet_la_SOURCES = \
    utils/core.cc \
    utils/methods.cc \
    utils/values.cc \
    common.cc \
    metadata.cc \
    summary.cc \
    dataset.cc \
    arki_query.cc \
    arki_scan.cc \
    arkimet.cc
_arkimet_la_CPPFLAGS = $(PYTHON_CFLAGS) -I$(top_srcdir) -I$(top_builddir)
_arkimet_la_LDFLAGS = -module -avoid-version -export-symbols-regex init_arkimet
_arkimet_la_LIBADD = ../arki/libarkimet.la

#doc_DATA = python-arkimet.rst

nodist_noinst_DATA = _arkimet.so

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
    utils/core.h \
    utils/methods.h \
    utils/type.h \
    utils/values.h \
    common.h \
    metadata.h \
    summary.h \
    arki_query.h \
    arki_scan.h \
    dataset.h
#    doc-arkimet \
#    testlib.py \
#    $(arkimet_python_tests)

check-local: _arkimet.so
	PYTHON="$(PYTHON)" $(TESTS_ENVIRONMENT) NOSE

#CLEANFILES = python-arkimet.rst python-arkimet.html
