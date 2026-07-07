toplevel_inc = include_directories('..')


python3.install_sources(
    'arkimet/__init__.py',
    'arkimet/cfg.py',
    'arkimet/counters.py',
    subdir: 'arkimet',
)

python3.install_sources(
    'arkimet/formatter/area.py',
    'arkimet/formatter/eccodes.py',
    'arkimet/formatter/formatter.py',
    'arkimet/formatter/__init__.py',
    'arkimet/formatter/level.py',
    'arkimet/formatter/origin.py',
    'arkimet/formatter/proddef.py',
    'arkimet/formatter/product.py',
    'arkimet/formatter/timerange.py',
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
    'arkimet/bbox/bbox.py',
    'arkimet/bbox/common.py',
    'arkimet/bbox/grib.py',
    'arkimet/bbox/__init__.py',
    'arkimet/bbox/odimh5.py',
    'arkimet/bbox/vm2.py',
    subdir: 'arkimet/bbox',
)

python3.install_sources(
    'arkimet/scan/bufr.py',
    'arkimet/scan/grib.py',
    'arkimet/scan/__init__.py',
    'arkimet/scan/jpeg.py',
    'arkimet/scan/netcdf.py',
    'arkimet/scan/odimh5.py',
    'arkimet/scan/vm2.py',
    subdir: 'arkimet/scan',
)

python3.install_sources(
    'arkimet/scan/utils/timedef.py',
    'arkimet/scan/utils/common.py',
    'arkimet/scan/utils/jpeg.py',
    subdir: 'arkimet/scan/utils',
)

python3.install_sources(
    'arkimet/server/views.py',
    'arkimet/server/__init__.py',
    subdir: 'arkimet/server',
)

python3.install_sources(
    'arkimet/cmdline/base.py',
    'arkimet/cmdline/bufr_prepare.py',
    'arkimet/cmdline/check.py',
    'arkimet/cmdline/cmdline.py',
    'arkimet/cmdline/dump.py',
    'arkimet/cmdline/__init__.py',
    'arkimet/cmdline/maint.py',
    'arkimet/cmdline/mergeconf.py',
    'arkimet/cmdline/query.py',
    'arkimet/cmdline/scan.py',
    'arkimet/cmdline/server.py',
    'arkimet/cmdline/xargs.py',
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
