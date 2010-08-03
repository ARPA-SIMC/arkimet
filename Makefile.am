## Process this file with automake to produce Makefile.in

ACLOCAL_AMFLAGS = -I m4

SUBDIRS = embedded wibble arki src .

man_MANS = \
	arki-config.1 \
	arki-dump.1 \
	arki-query.1 \
	arki-check.1 \
	arki-mergeconf.1 \
	arki-scan.1 \
	arki-xargs.1

if DBALLE
man_MANS += arki-bufr-prepare.1
endif

arki-%.1: src/arki-% doc/mhooks-%
	$< --manpage="$(word 2,$^)" > $@

#%.1: tools/%
#	( cd tools && ./`basename $<` help manpage ) > $@

bboxconfdir = $(confdir)/bbox
sg1confdir = $(confdir)/scan-grib1
sg2confdir = $(confdir)/scan-grib2
sbconfdir = $(confdir)/scan-bufr
fmtconfdir = $(confdir)/format
reportconfdir = $(confdir)/report
qmconfdir = $(confdir)/qmacro
tfconfdir = $(confdir)/targetfile

dist_conf_DATA = conf/match-alias.conf
dist_sg1conf_DATA = conf/scan-grib1/01-common.lua
dist_sg2conf_DATA = conf/scan-grib2/01-common.lua
dist_sbconf_DATA = \
	conf/scan-bufr/acars.lua \
	conf/scan-bufr/airep.lua \
	conf/scan-bufr/amdar.lua \
	conf/scan-bufr/buoy.lua \
	conf/scan-bufr/metar.lua \
	conf/scan-bufr/pilot.lua \
	conf/scan-bufr/ship.lua \
	conf/scan-bufr/synop.lua \
	conf/scan-bufr/temp.lua \
	conf/scan-bufr/temp_ship.lua
dist_bboxconf_DATA = conf/bbox/01-common
dist_fmtconf_DATA = \
	conf/format/01-gribapi \
	conf/format/05-gribtables \
	conf/format/10-area \
	conf/format/10-ensemble \
	conf/format/10-level \
	conf/format/10-origin \
	conf/format/10-product \
	conf/format/10-timerange
dist_reportconf_DATA = conf/report/count conf/report/datasets conf/report/mdstats
dist_qmconf_DATA = conf/qmacro/noop.lua conf/qmacro/expa.lua conf/qmacro/gridspace.lua
dist_tfconf_DATA = conf/targetfile/mars

EXTRA_DIST = README \
	     README.Cineca \
	     doc/DATASET \
	     doc/adding-formats.txt \
	     conf/qmacro/test0.lua \
	     conf/qmacro/noopcopy.lua \
             arkimet.spec \
	     doc/mhooks-config \
	     doc/mhooks-dump \
	     doc/mhooks-query \
	     doc/mhooks-scan \
	     doc/mhooks-check \
	     doc/mhooks-mergeconf \
	     doc/mhooks-xargs \
	     doc/mhooks-bufr-prepare \
	     doc/lua-examples/area.lua \
	     doc/lua-examples/bbox.lua \
	     doc/lua-examples/ensemble.lua \
	     doc/lua-examples/origin.lua \
	     doc/lua-examples/level.lua \
	     doc/lua-examples/product.lua \
	     doc/lua-examples/reftime.lua \
	     doc/lua-examples/run.lua \
	     doc/lua-examples/time.lua \
	     doc/lua-examples/timerange.lua \
	     doc/matcher/origin.txt \
	     doc/matcher/product.txt \
	     doc/matcher/level.txt \
	     doc/matcher/timerange.txt \
	     doc/matcher/reftime.txt \
	     doc/matcher/area.txt \
	     doc/matcher/ensemble.txt \
	     doc/matcher/run.txt \
	     doc/matcher/aliases.txt \
	     doc/matcher/expressions.txt \
	     test/data/layer.grib1 \
	     test/data/test.bufr \
	     test/data/test.grib1 \
	     test/data/test-sorted.grib1 \
	     test/data/padded.grib1 \
	     test/data/padded.bufr \
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
