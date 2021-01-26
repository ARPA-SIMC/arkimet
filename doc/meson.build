if asciidoc.found()

custom_target('arkiguide',
              input: ['arkiguide.it.txt'],
              output: ['arkiguide.it.html'],
              install: true,
              install_dir: doc_dir,
              capture: true,
              command: [asciidoc, '-a', 'toc', '@INPUT@'])
endif

sphinx_doc_sources = [
    'conf.py',
	'datasets.rst',
	'datasets/archive.rst',
	'datasets/iseg.rst',
	'datasets/ondisk2.rst',
	'datasets/simple.rst',
    'datasets/discard.rst',
    'datasets/duplicates.rst',
    'datasets/error.rst',
    'datasets/outbound.rst',
    'datasets/remote.rst',
	'http-api.rst',
	'index.rst',
	'matcher.rst',
	'matcher/aliases.rst',
	'matcher/area.rst',
	'matcher/level.rst',
	'matcher/origin.rst',
	'matcher/proddef.rst',
	'matcher/product.rst',
	'matcher/quantity.rst',
	'matcher/reftime.rst',
	'matcher/run.rst',
	'matcher/task.rst',
	'matcher/timerange.rst',
	'python/index.rst',
	'python/arkimet.bbox.rst',
	'python/arkimet.cfg.rst',
	'python/arkimet.dataset.rst',
	'python/arkimet.formatter.rst',
	'python/arkimet.rst',
	'python/arkimet.scan.rst',
	'python-howtos/format_metadata.rst',
	'python-howtos/index.rst',
	'python-howtos/read_inline_file.rst',
	'segments.rst',
	'segments/concat.rst',
    'segments/dir.rst',
    'segments/gz.rst',
	'segments/tar.rst',
    'segments/zip.rst',
]


# Generate documentation
#
# Because of issues like https://github.com/mesonbuild/meson/issues/2320
# we cannot express the chain of what's going on here using meson.
# As a workaround, we build a script that runs doxygen and sphinx, and run it
# in a custom target.

built_docs = custom_target('built_docs',
    input: [sphinx_doc_sources],
    output: ['html'],
    depends: [libarkimet, py_arkimet],
    command: [sphinx, '-b', 'html', meson.current_source_dir(), '@OUTPUT@'],
    build_by_default: false,
    install_dir: doc_dir,
    install: true,
    console: true,
)
