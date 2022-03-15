## Process this file with automake to produce Makefile.in

TESTS_ENVIRONMENT = $(top_srcdir)/arki/runtest

#TESTS = test.py

nobase_python_PYTHON = \
    arkimet/__init__.py \
    arkimet/cfg.py \
    arkimet/counters.py \
    arkimet/formatter/level.py \
    arkimet/formatter/__init__.py \
    arkimet/formatter/eccodes.py \
    arkimet/formatter/timerange.py \
    arkimet/formatter/product.py \
    arkimet/formatter/formatter.py \
    arkimet/formatter/area.py \
    arkimet/formatter/origin.py \
    arkimet/formatter/proddef.py \
    arkimet/dataset/__init__.py \
    arkimet/dataset/http/__init__.py \
    arkimet/dataset/http/aliases.py \
    arkimet/bbox/__init__.py \
    arkimet/bbox/grib.py \
    arkimet/bbox/bbox.py \
    arkimet/bbox/common.py \
    arkimet/bbox/vm2.py \
    arkimet/bbox/odimh5.py \
    arkimet/scan/timedef.py \
    arkimet/scan/__init__.py \
    arkimet/scan/grib.py \
    arkimet/scan/common.py \
    arkimet/scan/bufr.py \
    arkimet/scan/vm2.py \
    arkimet/scan/odimh5.py \
    arkimet/scan/nc.py \
    arkimet/server/views.py \
    arkimet/server/__init__.py \
    arkimet/cmdline/dump.py \
    arkimet/cmdline/query.py \
    arkimet/cmdline/__init__.py \
    arkimet/cmdline/server.py \
    arkimet/cmdline/xargs.py \
    arkimet/cmdline/mergeconf.py \
    arkimet/cmdline/base.py \
    arkimet/cmdline/bufr_prepare.py \
    arkimet/cmdline/scan.py \
    arkimet/cmdline/check.py

pyexec_LTLIBRARIES = _arkimet.la

_arkimet_la_SOURCES = \
    utils/core.cc \
    utils/methods.cc \
    utils/values.cc \
    utils/wreport.cc \
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
    dataset/session.cc \
    dataset/dataset.cc \
    dataset/reader.cc \
    dataset/writer.cc \
    dataset/checker.cc \
    dataset/progress.cc \
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

if DBALLE
_arkimet_la_SOURCES += utils/dballe.cc
endif

#doc_DATA = python-arkimet.rst

nodist_noinst_DATA = _arkimet.so

_arkimet.so: _arkimet.la
	test -e .libs/_arkimet.so || ( echo ".libs/_arkimet.so not built" >&2 ; exit 1 )
	ln -fs .libs/_arkimet.so $@

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
    dataset/session.h \
    dataset/dataset.h \
    dataset/reader.h \
    dataset/writer.h \
    dataset/checker.h \
    dataset/python.h \
    dataset/progress.h \
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
#    testlib.py \
#    $(arkimet_python_tests)

check-local: _arkimet.so
	PYTHON="$(PYTHON)" NOSE="@NOSE@" $(TESTS_ENVIRONMENT) PYTHON

#CLEANFILES = python-arkimet.rst python-arkimet.html
