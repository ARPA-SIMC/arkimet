## Process this file with automake to produce Makefile.in

TESTS_ENVIRONMENT = $(top_srcdir)/arki/runtest

#TESTS = test.py

nobase_python_PYTHON = arkimet/__init__.py arkimet/server/__init__.py arkimet/server/views.py

pyexec_LTLIBRARIES = _arkimet.la

_arkimet_la_SOURCES = \
    utils/core.cc \
    utils/methods.cc \
    utils/values.cc \
    utils/wreport.cc \
    utils/dballe.cc \
    common.cc \
    files.cc \
    structured.cc \
    metadata.cc \
    formatter.cc \
    bbox.cc \
    cfg.cc \
    summary.cc \
    matcher.cc \
    scan.cc \
    dataset.cc \
    dataset/python.cc \
    dataset/querymacro.cc \
    counters.cc \
    cmdline.cc \
    cmdline/processor.cc \
    arki-query.cc \
    arki-scan/results.cc \
    arki-scan/dispatch.cc \
    arki-scan.cc \
    arki-check.cc \
    arki-dump.cc \
    arki-xargs.cc \
    arki-bufr-prepare.cc \
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
    utils/dict.h \
    utils/wreport.h \
    utils/dballe.h \
    common.h \
    files.h \
    structured.h \
    cfg.h \
    metadata.h \
    formatter.h \
    bbox.h \
    summary.h \
    matcher.h \
    scan.h \
    dataset.h \
    dataset/python.h \
    dataset/reporter.h \
    dataset/querymacro.h \
    counters.h \
    cmdline.h \
    cmdline/processor.h \
    arki-query.h \
    arki-scan.h \
    arki-scan/results.h \
    arki-scan/dispatch.h \
    arki-check.h \
    arki-dump.h \
    arki-xargs.h \
    arki-bufr-prepare.h
#    doc-arkimet \
#    testlib.py \
#    $(arkimet_python_tests)

check-local: _arkimet.so
	PYTHON="$(PYTHON)" $(TESTS_ENVIRONMENT) NOSE

#CLEANFILES = python-arkimet.rst python-arkimet.html
