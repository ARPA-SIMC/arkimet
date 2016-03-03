## Process this file with automake to produce Makefile.in

bin_PROGRAMS += %reldir%/arki-check %reldir%/arki-query %reldir%/arki-dump %reldir%/arki-gzip %reldir%/arki-mergeconf %reldir%/arki-benchmark %reldir%/arki-scan %reldir%/arki-xargs %reldir%/arki-server %reldir%/arki-inbound

noinst_PROGRAMS += %reldir%/arki-test

dist_noinst_HEADERS += %reldir%/bench/benchmark.h

# TODO: -Werror for all bins

if READLINE
bin_PROGRAMS += %reldir%/arki-config
%canon_reldir%_arki_config_SOURCES = %reldir%/arki-config.cc
%canon_reldir%_arki_config_LDADD = arki/libarkimet.la
endif

if DBALLE
bin_PROGRAMS += %reldir%/arki-bufr-prepare
%canon_reldir%_arki_bufr_prepare_SOURCES = %reldir%/arki-bufr-prepare.cc
%canon_reldir%_arki_bufr_prepare_LDADD = arki/libarkimet.la
endif

if LUA
bin_PROGRAMS += %reldir%/arki-shell
%canon_reldir%_arki_shell_SOURCES = %reldir%/arki-shell.cc
%canon_reldir%_arki_shell_LDADD = arki/libarkimet.la
endif

%canon_reldir%_arki_scan_SOURCES = %reldir%/arki-scan.cc
%canon_reldir%_arki_scan_LDADD = arki/libarkimet.la

%canon_reldir%_arki_inbound_SOURCES = %reldir%/arki-inbound.cc
%canon_reldir%_arki_inbound_LDADD = arki/libarkimet.la

%canon_reldir%_arki_benchmark_SOURCES = %reldir%/arki-benchmark.cc %reldir%/bench/benchmark.cc
%canon_reldir%_arki_benchmark_LDADD = arki/libarkimet.la

%canon_reldir%_arki_mergeconf_SOURCES = %reldir%/arki-mergeconf.cc
%canon_reldir%_arki_mergeconf_LDADD = arki/libarkimet.la

%canon_reldir%_arki_query_SOURCES = %reldir%/arki-query.cc
%canon_reldir%_arki_query_LDADD = arki/libarkimet.la

%canon_reldir%_arki_check_SOURCES = %reldir%/arki-check.cc
%canon_reldir%_arki_check_LDADD = arki/libarkimet.la

%canon_reldir%_arki_dump_SOURCES = %reldir%/arki-dump.cc
%canon_reldir%_arki_dump_LDADD = arki/libarkimet.la

%canon_reldir%_arki_xargs_SOURCES = %reldir%/arki-xargs.cc
%canon_reldir%_arki_xargs_LDADD = arki/libarkimet.la

%canon_reldir%_arki_gzip_SOURCES = %reldir%/arki-gzip.cc
%canon_reldir%_arki_gzip_LDADD = arki/libarkimet.la

%canon_reldir%_arki_server_SOURCES = %reldir%/arki-server.cc
%canon_reldir%_arki_server_CPPFLAGS = $(AM_CPPFLAGS) -DSERVER_DIR=\"$(serverdir)\"
%canon_reldir%_arki_server_LDADD = arki/libarkimet.la

%canon_reldir%_arki_test_SOURCES = %reldir%/arki-test.cc
%canon_reldir%_arki_test_LDADD = arki/libarkimet.la

TESTS += %reldir%/test-arki-server %reldir%/test-arki-scan %reldir%/test-arki-query %reldir%/test-arki-xargs %reldir%/run-test-src
check_PROGRAMS += %reldir%/test-src

%canon_reldir%_test_src_SOURCES = \
		%reldir%/arki-server-tut.cc \
		arki/utils/tests.cc \
		arki/types/tests.cc \
		arki/tests/tut-main.cpp

%canon_reldir%_test_src_LDADD = arki/libarkimet.la

EXTRA_DIST += %reldir%/testlib.sh
