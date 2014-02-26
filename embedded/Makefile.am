## Process this file with automake to produce Makefile.in

SUBDIRS =

if WIBBLE_STANDALONE
else
SUBDIRS += wibble/wibble
endif

if SQLITE_EMBEDDED
SUBDIRS += sqlite
endif

if LZO_EMBEDDED
SUBDIRS += minilzo
endif

EXTRA_DIST = README
