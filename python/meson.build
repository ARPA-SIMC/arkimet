toplevel_inc = include_directories('..')

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
    subdir: 'arkimet/dataset',
)

python3.install_sources(
    'arkimet/dataset/http/__init__.py',
    'arkimet/dataset/http/aliases.py',
    subdir: 'arkimet/dataset/http',
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
    'arkimet/scan/utils/timedef.py',
    'arkimet/scan/__init__.py',
    'arkimet/scan/grib.py',
    'arkimet/scan/utils/common.py',
    'arkimet/scan/bufr.py',
    'arkimet/scan/vm2.py',
    'arkimet/scan/odimh5.py',
    'arkimet/scan/nc.py',
    'arkimet/scan/jpeg.py',
    subdir: 'arkimet/scan',
)

python3.install_sources(
    'arkimet/server/views.py',
    'arkimet/server/__init__.py',
    subdir: 'arkimet/server',
)

python3.install_sources(
    'arkimet/cmdline/__init__.py',
    'arkimet/cmdline/cmdline.py',
    'arkimet/cmdline/base.py',
    'arkimet/cmdline/dump.py',
    'arkimet/cmdline/query.py',
    'arkimet/cmdline/server.py',
    'arkimet/cmdline/xargs.py',
    'arkimet/cmdline/mergeconf.py',
    'arkimet/cmdline/bufr_prepare.py',
    'arkimet/cmdline/scan.py',
    'arkimet/cmdline/check.py',
    'arkimet/cmdline/maint.py',
    subdir: 'arkimet/cmdline',
)


runtest = find_program('../arki/runtest')

test('python', runtest, args: ['PYTHON'], timeout: 600,
     depends: [py_arkimet],
     env: [
         'PYTHON=' + python3_path,
         'PYTHONPATH=' + meson.current_source_dir() + ':' + meson.current_build_dir(),
     ],
)
