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
# In order to be able to set PYTHONPATH for sphinx, we cannot call it in a
# custom_target directly, but through a wrapper script

build_docs = configure_file(output: 'build-docs', input: 'build-docs.in', configuration: {
    'sphinx_build': sphinx.path(),
    'doc_src': meson.current_source_dir(),
    'doc_build': meson.current_build_dir(),
    'top_builddir': meson.build_root(),
    'pythonpath_src': meson.source_root() / 'python',
    'pythonpath_build': meson.build_root() / 'python',
})

built_docs = custom_target('built_docs',
    input: [sphinx_doc_sources],
    output: ['html'],
    depends: [libarkimet, py_arkimet],
    # command: [sphinx, '-b', 'html', meson.current_source_dir(), '@OUTPUT@'],
    command: [build_docs],
    build_by_default: false,
    install_dir: doc_dir,
    install: true,
    console: true,
)
