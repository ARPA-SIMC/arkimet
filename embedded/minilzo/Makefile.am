## Process this file with automake to produce Makefile.in

dist_noinst_HEADERS += %reldir%/minilzo.h %reldir%/lzoconf.h %reldir%/lzodefs.h

noinst_PROGRAMS += %reldir%/testmini
%canon_reldir%_testmini_SOURCES = %reldir%/testmini.c
%canon_reldir%_testmini_LDADD = %reldir%/libminilzo.la

noinst_LTLIBRARIES += %reldir%/libminilzo.la
%canon_reldir%_libminilzo_la_SOURCES = %reldir%/minilzo.c

EXTRA_DIST += %reldir%/README.LZO %reldir%/README
