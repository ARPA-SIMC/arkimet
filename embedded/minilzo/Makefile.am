## Process this file with automake to produce Makefile.in

dist_noinst_HEADERS = minilzo.h

noinst_PROGRAMS = testmini
testmini_SOURCES = testmini.c
testmini_LDADD = libminilzo.la

noinst_LTLIBRARIES = libminilzo.la
libminilzo_la_SOURCES = minilzo.c

EXTRA_DIST = README.LZO README
