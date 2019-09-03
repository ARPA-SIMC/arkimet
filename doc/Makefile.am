md_docs = datasets.md ds-archive.md segments.md seg-concat.md seg-dir.md web-api.md

generated_md_docs = ds-simple.md ds-ondisk2.md ds-iseg.md

doc_DATA = \
             $(generated_md_docs) \
             $(md_docs)

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

dist_doc_matcher_DATA = \
		   matcher/origin.md \
		   matcher/product.md \
		   matcher/level.md \
		   matcher/timerange.md \
		   matcher/reftime.md \
		   matcher/area.md \
		   matcher/proddef.md \
		   matcher/run.md \
		   matcher/aliases.md \
		   matcher/expressions.md

CLEANFILES =

if ASCIIDOC

arkiguide.it.html: arkiguide.it.txt
	asciidoc -a toc $< > $@

doc_DATA += arkiguide.it.html

CLEANFILES += arkiguide.it.html

endif

BUILT_SOURCES = $(generated_md_docs)

EXTRA_DIST = update \
             $(generated_md_docs) \
             $(md_docs) \
             ds-simple.md.j2 ds-ondisk2.md.j2 ds-iseg.md.j2

%.md: %.md.j2 $(top_srcdir)/arki/tests/testdata.json
	$(srcdir)/update --srcdir=$(srcdir) --dstdir=$(builddir) --testsuite=$(top_srcdir)/arki/tests/testdata.json $<
