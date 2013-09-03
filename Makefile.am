## Process this file with automake to produce Makefile.in

ACLOCAL_AMFLAGS = -I m4

SUBDIRS = conf embedded wibble arki src .

man_MANS = \
	arki-config.1 \
	arki-dump.1 \
	arki-gzip.1 \
	arki-query.1 \
	arki-check.1 \
	arki-inbound.1 \
	arki-mergeconf.1 \
	arki-scan.1 \
	arki-server.1 \
	arki-xargs.1

if DBALLE
man_MANS += arki-bufr-prepare.1
endif

arki-%.1: src/arki-% doc/mhooks-%
	$< --manpage="$(word 2,$^)" > $@

#%.1: tools/%
#	( cd tools && ./`basename $<` help manpage ) > $@

EXTRA_DIST = README \
	     README.Cineca \
	     update-tags \
	     doc/DATASET \
	     doc/adding-formats.txt \
	     doc/mhooks-config \
	     doc/mhooks-dump \
	     doc/mhooks-gzip \
	     doc/mhooks-query \
	     doc/mhooks-scan \
	     doc/mhooks-check \
	     doc/mhooks-inbound \
	     doc/mhooks-mergeconf \
	     doc/mhooks-server \
	     doc/mhooks-xargs \
	     doc/mhooks-bufr-prepare \
	     doc/lua-examples/area.lua \
	     doc/lua-examples/bbox.lua \
	     doc/lua-examples/proddef.lua \
	     doc/lua-examples/origin.lua \
	     doc/lua-examples/level.lua \
	     doc/lua-examples/product.lua \
	     doc/lua-examples/quantity.lua \
	     doc/lua-examples/reftime.lua \
	     doc/lua-examples/run.lua \
	     doc/lua-examples/task.lua \
	     doc/lua-examples/time.lua \
	     doc/lua-examples/timerange.lua \
	     doc/matcher/origin.txt \
	     doc/matcher/product.txt \
	     doc/matcher/level.txt \
	     doc/matcher/timerange.txt \
	     doc/matcher/reftime.txt \
	     doc/matcher/area.txt \
	     doc/matcher/proddef.txt \
	     doc/matcher/run.txt \
	     doc/matcher/aliases.txt \
	     doc/matcher/expressions.txt \
	     test/conf/match-alias.conf \
	     test/data/cosmo \
	     test/data/layer.grib1 \
	     test/data/test.bufr \
	     test/data/test.grib1 \
	     test/data/test-sorted.grib1 \
	     test/data/padded.grib1 \
	     test/data/padded.bufr \
	     test/data/pollution.bufr \
	     test/data/odim1.arkimet \
	     test/data/old.summary \
	     test/data/all.summary \
	     test/error/config \
	     test/dataset1/config \
	     test/misc/erse00.expa \
	     test/misc/rec-ts-alias.conf \
	     test/postproc/checkfiles \
	     test/postproc/error \
	     test/postproc/null \
	     test/postproc/say \
	     test/postproc/testcountbytes

CLEANFILES = $(man_MANS)

EXTRA_DIST += doc/arkiguide.it.txt

if ASCIIDOC

doc/arkiguide.it.html: doc/arkiguide.it.txt
	asciidoc $< > $@

EXTRA_DIST += doc/arkiguide.it.html

CLEANFILES += doc/arkiguide.it.html

endif
