EXTRA_DIST =
BUILT_SOURCES =
noinst_DATA =
doc_DATA =

dist_doc_DATA = \
		DATASET \
		arkiguide.it.txt \
		adding-formats.txt \
		mhooks-dump \
		mhooks-gzip \
		mhooks-query \
		mhooks-scan \
		mhooks-check \
		mhooks-mergeconf \
		mhooks-xargs \
		mhooks-bufr-prepare

doc_matcherdir = $(docdir)/matcher

CLEANFILES =

if ASCIIDOC

arkiguide.it.html: arkiguide.it.txt
	asciidoc -a toc $< > $@

doc_DATA += arkiguide.it.html

CLEANFILES += arkiguide.it.html

endif

if DO_DOCS

# Sphinx sources

sphinx_packaged_sources = \
	datasets.rst segments.rst \
	datasets/archive.rst \
	segments/concat.rst segments/dir.rst segments/gz.rst \
	segments/tar.rst segments/zip.rst \
	matcher.rst \
	matcher/aliases.rst \
	matcher/area.rst \
	matcher/level.rst \
	matcher/origin.rst \
	matcher/proddef.rst \
	matcher/product.rst \
	matcher/quantity.rst \
	matcher/reftime.rst \
	matcher/run.rst \
	matcher/task.rst \
	matcher/timerange.rst \
	http-api.rst

sphinx_generated_sources = \
	datasets/simple.rst datasets/ondisk2.rst datasets/iseg.rst

EXTRA_DIST += \
	update \
	datasets/simple.rst.j2 datasets/ondisk2.rst.j2 datasets/iseg.rst.j2

sphinx_sources = $(sphinx_packaged_sources) $(sphinx_generated_sources)

%.rst: %.rst.j2 $(top_srcdir)/arki/tests/testdata.json
	$(srcdir)/update --srcdir=$(srcdir) --dstdir=$(builddir) --testsuite=$(top_srcdir)/arki/tests/testdata.json $<


EXTRA_DIST += $(sphinx_packaged_sources)

# See https://stackoverflow.com/questions/19822435/multiple-targets-from-one-recipe-and-parallel-execution#19822767
.INTERMEDIATE: build-docs

# Build doxygen + sphinx documentation
build-docs: $(sphinx_sources)
	rm -rf html
	mkdir html
	$(top_srcdir)/run-local $(SPHINX_BUILD) -b html . html/

html/index.html: build-docs

# aliases.dox: $(top_srcdir)/dballe/core/aliases.gperf mksnippet
# 	$(top_srcdir)/doc/mksnippet alias dox < $< > $@ || rm $@
# 
# ltypes.dox: $(top_srcdir)/dballe/msg/ltypes.txt mksnippet
# 	$(top_srcdir)/doc/mksnippet levels dox < $< > $@ || rm $@
# 
# tranges.dox: mksnippet
# 	$(top_srcdir)/doc/mksnippet tranges dox > $@ || rm $@
# 
# src/btable.rst: ../tables/dballe.txt mksnippet
# 	$(top_srcdir)/doc/mksnippet btable rst < $< > $@ || rm $@
# 
# src/ltypes.rst: $(top_srcdir)/dballe/msg/ltypes.txt mksnippet
# 	$(top_srcdir)/doc/mksnippet levels rst < $< > $@ || rm $@
# 
# src/tranges.rst: mksnippet
# 	$(top_srcdir)/doc/mksnippet tranges rst > $@ || rm $@
# 
# src/aliases.rst: $(top_srcdir)/dballe/core/aliases.gperf mksnippet
# 	$(top_srcdir)/doc/mksnippet alias rst < $< > $@ || rm $@
#
# EXTRA_DIST += make_fortran_reference

noinst_DATA += build-docs

# make install machinery

install-data-local:
	find html -type d -exec $(mkinstalldirs) '$(DESTDIR)$(docdir)/{}'  \; ;
	find html -type f -not -path '*/.*' -exec $(INSTALL_DATA) '{}' '$(DESTDIR)$(docdir)/{}' \; ;

uninstall-local:
	rm -rf $(DESTDIR)$(docdir)/html

endif
