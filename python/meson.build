toplevel_inc = include_directories('..')

py_arkimet_sources = [
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
]

if dballe_dep.found()
    py_arkimet_sources += 'utils/dballe.cc'
endif

py_arkimet = python3.extension_module('_arkimet',
    py_arkimet_sources,
    cpp_pch: '../arki/pch/arkimet_pch.h',
    include_directories: toplevel_inc,
    dependencies: python3.dependency(),
    link_with: libarkimet,
    install: true,
    # Meson does not currently autodetect the right target for python modules:
    # https://github.com/mesonbuild/meson/issues/6331
    install_dir: python3.get_path('platlib'),
)

python3.install_sources(
    'arkimet/__init__.py',
    'arkimet/cfg.py',
    'arkimet/counters.py',
    subdir: 'arkimet',
)

python3.install_sources(
    'arkimet/formatter/level.py',
    'arkimet/formatter/__init__.py',
    'arkimet/formatter/eccodes.py',
    'arkimet/formatter/timerange.py',
    'arkimet/formatter/product.py',
    'arkimet/formatter/formatter.py',
    'arkimet/formatter/area.py',
    'arkimet/formatter/origin.py',
    'arkimet/formatter/proddef.py',
    subdir: 'arkimet/formatter',
)

python3.install_sources(
    'arkimet/dataset/__init__.py',
    'arkimet/dataset/http.py',
    subdir: 'arkimet/dataset',
)

python3.install_sources(
    'arkimet/bbox/__init__.py',
    'arkimet/bbox/grib.py',
    'arkimet/bbox/bbox.py',
    'arkimet/bbox/common.py',
    'arkimet/bbox/vm2.py',
    'arkimet/bbox/odimh5.py',
    subdir: 'arkimet/bbox',
)

python3.install_sources(
    'arkimet/scan/timedef.py',
    'arkimet/scan/__init__.py',
    'arkimet/scan/grib.py',
    'arkimet/scan/common.py',
    'arkimet/scan/bufr.py',
    'arkimet/scan/vm2.py',
    'arkimet/scan/odimh5.py',
    subdir: 'arkimet/scan',
)

python3.install_sources(
    'arkimet/server/views.py',
    'arkimet/server/__init__.py',
    subdir: 'arkimet/server',
)

python3.install_sources(
    'arkimet/cmdline/dump.py',
    'arkimet/cmdline/query.py',
    'arkimet/cmdline/__init__.py',
    'arkimet/cmdline/server.py',
    'arkimet/cmdline/xargs.py',
    'arkimet/cmdline/mergeconf.py',
    'arkimet/cmdline/base.py',
    'arkimet/cmdline/bufr_prepare.py',
    'arkimet/cmdline/scan.py',
    'arkimet/cmdline/check.py',
    subdir: 'arkimet/cmdline',
)


runtest = find_program('../arki/runtest')

# TODO: from meson 0.55.0, we can add env: 'NOSE=' + nose.full_path()
test('python', runtest, args: ['NOSE'],
     env: [
         'PYTHON=' + python3_path,
         'PYTHONPATH=' + meson.current_source_dir() + ':' + meson.current_build_dir(),
     ],
)
