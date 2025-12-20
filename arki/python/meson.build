extra_warnings = cpp.get_supported_arguments([
  '-Wno-redundant-decls',
])

py_arkimet_sources = files(
    'utils/core.cc',
    'utils/methods.cc',
    'utils/values.cc',
    'utils/wreport.cc',
    'common.cc',
    'files.cc',
    'structured.cc',
    'metadata.cc',
    'formatter.cc',
    'bbox.cc',
    'cfg.cc',
    'summary.cc',
    'matcher.cc',
    'scan.cc',
    'scan/grib.cc',
    'scan/bufr.cc',
    'scan/netcdf.cc',
    'scan/odimh5.cc',
    'scan/vm2.cc',
    'dataset.cc',
    'dataset/session.cc',
    'dataset/dataset.cc',
    'dataset/reader.cc',
    'dataset/writer.cc',
    'dataset/checker.cc',
    'dataset/progress.cc',
    'dataset/python.cc',
    'dataset/querymacro.cc',
    'counters.cc',
    'cmdline.cc',
    'cmdline/processor.cc',
    'arki-query.cc',
    'arki-scan/results.cc',
    'arki-scan/dispatch.cc',
    'arki-scan.cc',
    'arki-check.cc',
    'arki-dump.cc',
    'arki-xargs.cc',
    'arki-bufr-prepare.cc',
    'arkimet.cc',
)

if dballe_dep.found()
    py_arkimet_sources += files('utils/dballe.cc')
endif

py_arkimet = python3.extension_module('_arkimet',
    py_arkimet_sources,
    cpp_pch: '../pch/arkimet_pch.h',
    cpp_args: extra_warnings,
    include_directories: toplevel_inc,
    link_with: libarkimet,
    install: true,
    # Meson does not currently autodetect the right target for python modules:
    # https://github.com/mesonbuild/meson/issues/6331
    install_dir: python3.get_path('platlib'),
    dependencies: [python3.dependency()],
)


