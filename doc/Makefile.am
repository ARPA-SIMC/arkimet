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

BUILT_SOURCES = ds-simple.md seg-concat.md seg-dir.md testdoc.json

EXTRA_DIST = update \
			 datasets.md ds-archive.md \
			 ds-indexed.j2 ds-simple.md.j2 \
			 segments.md \
			 seg-base.j2 seg-concat.md.j2 seg-dir.md.j2
			 $(BUILT_SOURCES)

testdoc.json:
	$(srcdir)/update --dstdir $(srcdir) --testdoc $(top_builddir)/arki/tests/arki-test

%.md: %.md.j2 testdoc.json
	$(srcdir)/update $<
