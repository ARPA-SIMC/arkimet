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
    'index.rst',
    'howtos/new-data-format.md',
    'howtos/python-api/format_metadata.md',
    'howtos/python-api/read_inline_file.md',
    'references/datasets.rst',
    'references/datasets/archive.rst',
    'references/datasets/iseg.rst',
    'references/datasets/ondisk2.rst',
    'references/datasets/simple.rst',
    'references/datasets/discard.rst',
    'references/datasets/duplicates.rst',
    'references/datasets/error.rst',
    'references/datasets/outbound.rst',
    'references/datasets/remote.rst',
    'references/config.rst',
    'references/http-api.rst',
    'references/match-alias.rst',
    'references/matcher.rst',
    'references/matcher/area.rst',
    'references/matcher/level.rst',
    'references/matcher/origin.rst',
    'references/matcher/proddef.rst',
    'references/matcher/product.rst',
    'references/matcher/quantity.rst',
    'references/matcher/reftime.rst',
    'references/matcher/run.rst',
    'references/matcher/task.rst',
    'references/matcher/timerange.rst',
    'references/metadata.rst',
    'references/segments.rst',
    'references/segments/concat.rst',
    'references/segments/dir.rst',
    'references/segments/gz.rst',
    'references/segments/tar.rst',
    'references/segments/zip.rst',
    'references/python-api/arkimet.rst',
    'references/python-api/arkimet.bbox.rst',
    'references/python-api/arkimet.cfg.rst',
    'references/python-api/arkimet.dataset.rst',
    'references/python-api/arkimet.formatter.rst',
    'references/python-api/arkimet.scan.rst',
]


# Generate documentation
#
# In order to be able to set PYTHONPATH for sphinx, we cannot call it in a
# custom_target directly, but through a wrapper script

build_docs = configure_file(output: 'build-docs', input: 'build-docs.in', configuration: {
    'sphinx_build': sphinx.full_path(),
    'doc_src': meson.current_source_dir(),
    'doc_build': meson.current_build_dir(),
    'top_builddir': meson.project_build_root(),
    'pythonpath_src': meson.project_source_root() / 'python',
    'pythonpath_build': meson.project_build_root() / 'arki' / 'python',
})

built_docs = custom_target('built_docs',
    input: [sphinx_doc_sources],
    output: ['html'],
    depends: [libarkimet, py_arkimet],
    command: [build_docs],
    build_by_default: false,
    install_dir: doc_dir,
    install: true,
    console: true,
)
