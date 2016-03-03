dist_doc_DATA += \
		%reldir%/DATASET \
		%reldir%/arkiguide.it.txt \
		%reldir%/adding-formats.txt \
		%reldir%/mhooks-config \
		%reldir%/mhooks-dump \
		%reldir%/mhooks-gzip \
		%reldir%/mhooks-query \
		%reldir%/mhooks-scan \
		%reldir%/mhooks-check \
		%reldir%/mhooks-inbound \
		%reldir%/mhooks-mergeconf \
		%reldir%/mhooks-server \
		%reldir%/mhooks-xargs \
		%reldir%/mhooks-bufr-prepare \
		%reldir%/lua-examples/area.lua \
		%reldir%/lua-examples/bbox.lua \
		%reldir%/lua-examples/proddef.lua \
		%reldir%/lua-examples/origin.lua \
		%reldir%/lua-examples/level.lua \
		%reldir%/lua-examples/product.lua \
		%reldir%/lua-examples/quantity.lua \
		%reldir%/lua-examples/reftime.lua \
		%reldir%/lua-examples/run.lua \
		%reldir%/lua-examples/task.lua \
		%reldir%/lua-examples/time.lua \
		%reldir%/lua-examples/timerange.lua

doc_matcherdir = $(docdir)/matcher

dist_doc_matcher_DATA = \
		   %reldir%/matcher/origin.txt \
		   %reldir%/matcher/product.txt \
		   %reldir%/matcher/level.txt \
		   %reldir%/matcher/timerange.txt \
		   %reldir%/matcher/reftime.txt \
		   %reldir%/matcher/area.txt \
		   %reldir%/matcher/proddef.txt \
		   %reldir%/matcher/run.txt \
		   %reldir%/matcher/aliases.txt \
		   %reldir%/matcher/expressions.txt

if ASCIIDOC

%reldir%/arkiguide.it.html: %reldir%/arkiguide.it.txt
	asciidoc -a toc $< > $@

doc_DATA += %reldir%/arkiguide.it.html

CLEANFILES += %reldir%/arkiguide.it.html

endif
