md_docs = datasets.md ds-archive.md segments.md seg-concat.md seg-dir.md

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
		mhooks-bufr-prepare \
		lua-examples/area.lua \
		lua-examples/bbox.lua \
		lua-examples/proddef.lua \
		lua-examples/origin.lua \
		lua-examples/level.lua \
		lua-examples/product.lua \
		lua-examples/quantity.lua \
		lua-examples/reftime.lua \
		lua-examples/run.lua \
		lua-examples/task.lua \
		lua-examples/time.lua \
		lua-examples/timerange.lua

doc_matcherdir = $(docdir)/matcher

dist_doc_matcher_DATA = \
		   matcher/origin.txt \
		   matcher/product.txt \
		   matcher/level.txt \
		   matcher/timerange.txt \
		   matcher/reftime.txt \
		   matcher/area.txt \
		   matcher/proddef.txt \
		   matcher/run.txt \
		   matcher/aliases.txt \
		   matcher/expressions.txt

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
	$(srcdir)/update --srcdir=$(srcdir) --dstdir=$(builddir) --testsuite=$(top_srcdir/arki/tests/testdata.json $<
