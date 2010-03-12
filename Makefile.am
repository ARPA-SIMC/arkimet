## Process this file with automake to produce Makefile.in

ACLOCAL_AMFLAGS = -I m4

SUBDIRS = wibble arki src .

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
fmtconfdir = $(confdir)/format
reportconfdir = $(confdir)/report
qmconfdir = $(confdir)/qmacro
tfconfdir = $(confdir)/targetfile

dist_conf_DATA = conf/match-alias.conf
dist_sg1conf_DATA = conf/scan-grib1/01-common
dist_sg2conf_DATA = conf/scan-grib2/01-common
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
dist_reportconf_DATA = conf/report/mdstats
dist_qmconf_DATA = conf/qmacro/noop.lua conf/qmacro/expa.lua conf/qmacro/gridspace.lua
dist_tfconf_DATA = conf/targetfile/mars

EXTRA_DIST = doc/MATCHER \
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
	     test/data/test.bufr \
	     test/data/test.grib1 \
	     test/data/padded.grib1 \
	     test/data/padded.bufr \
	     test/error/config \
	     test/dataset1/config \
	     test/misc/erse00.expa \
	     test/postproc/say

CLEANFILES = $(man_MANS)
