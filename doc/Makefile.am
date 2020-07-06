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
	datasets.rst \
	datasets/archive.rst \
	datasets/iseg.rst \
	datasets/ondisk2.rst \
	datasets/simple.rst \
	http-api.rst \
	index.rst \
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
	python/index.rst \
	python/arkimet.bbox.rst \
	python/arkimet.cfg.rst \
	python/arkimet.dataset.rst \
	python/arkimet.formatter.rst \
	python/arkimet.rst \
	python/arkimet.scan.rst \
	python-howtos/index.rst \
	python-howtos/read_inline_file.rst \
	segments.rst \
	segments/concat.rst segments/dir.rst segments/gz.rst \
	segments/tar.rst segments/zip.rst


sphinx_sources = $(sphinx_packaged_sources) $(sphinx_generated_sources)

EXTRA_DIST += $(sphinx_packaged_sources)

# See https://stackoverflow.com/questions/19822435/multiple-targets-from-one-recipe-and-parallel-execution#19822767
.INTERMEDIATE: build-docs

# Build doxygen + sphinx documentation
build-docs: $(sphinx_sources)
	rm -rf html
	mkdir html
	$(top_srcdir)/run-local $(SPHINX_BUILD) -b html . html/

html/index.html: build-docs

noinst_DATA += build-docs

# make install machinery

install-data-local:
	find html -type d -exec $(mkinstalldirs) '$(DESTDIR)$(docdir)/{}'  \; ;
	find html -type f -not -path '*/.*' -exec $(INSTALL_DATA) '{}' '$(DESTDIR)$(docdir)/{}' \; ;

uninstall-local:
	rm -rf $(DESTDIR)$(docdir)/html

endif
