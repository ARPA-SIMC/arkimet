project('arkimet', 'cpp', version: '1.52', license : 'GPL-2.0-or-later', default_options: ['warning_level=3', 'cpp_std=c++17'])
# TODO: use warning_level=everything from meson 1.0

cpp = meson.get_compiler('cpp')

warning_control = [
  # Turn some warning classes to errors
  '-Werror=format',
  '-Werror=suggest-override',
  '-Werror=deprecated-copy-dtor',
  '-Werror=missing-declarations',
  '-Werror=overloaded-virtual',
  '-Werror=cast-qual',
  '-Werror=duplicated-branches',
  '-Werror=logical-op',
  '-Werror=catch-value',
  '-Werror=conditionally-supported',
  '-Werror=noexcept',
  '-Werror=c++23-extensions',
  '-Werror=dangling-else',
  # '-Werror=suggest-attribute=format',
  # '-Werror=deprecated-declarations',

  '-Wno-padded',
  '-Wno-abi-tag',
  '-Wno-unused-macros',
  '-Wno-missing-include-dirs',
  '-Wno-multiple-inheritance',
  '-Wno-sign-promo',
  '-Wswitch',
  '-Wno-switch-enum',
  '-Wno-effc++',

  # TODO: remove the following ones over time
  '-Wno-shadow',
  '-Wno-zero-as-null-pointer-constant',
  '-Wno-mismatched-tags',
  '-Wno-unused-const-variable',
  '-Wno-redundant-tags',
  '-Wno-useless-cast',
  '-Wno-switch-default',
  '-Wno-old-style-cast',
  '-Wno-unused-parameter',

  # These ones can be activated from time to time
  '-Wno-float-equal',
  '-Wno-suggest-attribute=noreturn',
  '-Wno-format-truncation',
  '-Wno-arith-conversion',
  '-Wno-conversion',
]

add_project_arguments(
  cpp.get_supported_arguments(warning_control),
  language : 'cpp')

conf_dir = get_option('sysconfdir') / 'arkimet'
doc_dir = get_option('datadir') / 'doc' / 'arkimet'

conf_data = configuration_data()
conf_data.set_quoted('PACKAGE_VERSION', meson.project_version())
conf_data.set('ARKI_IOTRACE', true)
conf_data.set_quoted('CONF_DIR', conf_dir)
conf_data.set_quoted('POSTPROC_DIR', get_option('libdir') / 'arkimet')
if get_option('arpae-tests')
  conf_data.set('ARPAE_TESTS', true)
endif

toplevel_inc = include_directories('.')

# Enable profiling if requested
if get_option('profiling') == 'gprof'
    add_global_arguments('-pg', '-fprofile-arcs', '-fprofile-generate', language : 'cpp')
    add_global_link_arguments('-pg', '-fprofile-arcs', '-fprofile-generate', language : 'cpp')
endif
if get_option('profiling') == 'gperftools'
    add_global_arguments('-DUSE_GPERFTOOLS', language : 'cpp')
    gperftools_dep = dependency('libprofiler')
endif

# Dependencies

compiler = meson.get_compiler('cpp')
if compiler.has_function('splice', prefix : '#include <fcntl.h>')
    conf_data.set('ARKI_HAVE_SPLICE', true)
endif

flex = find_program('flex')
bison = find_program('bison')
help2man = find_program('help2man')

thread_dep = dependency('threads')

geos_dep = dependency('', required: false)
geos_config = find_program('geos-config', required: false)
if geos_config.found()
    geos_includes = run_command(geos_config, '--includes').stdout().split()
    geos_cflags = run_command(geos_config, '--cflags').stdout().split()
    geos_clibs = run_command(geos_config, '--clibs').stdout().split()
    geos_version = run_command(geos_config, '--version').stdout().strip()
    geos_dep = declare_dependency(
        include_directories: geos_includes,
        compile_args: geos_cflags,
        link_args: geos_clibs,
        version: geos_version,
    )
endif
conf_data.set('HAVE_GEOS', geos_dep.found())

foreach name : ['lua', 'lua5.1', 'lua5.2', 'lua5.3']
    lua_dep = dependency(name, required: false)
    if lua_dep.found()
        break
    endif
endforeach
conf_data.set('HAVE_LUA', lua_dep.found())

lzo_dep = dependency('lzo2', required: false)
if not lzo_dep.found()
  lzo_dep = compiler.find_library('lzo2', required: true)
endif
conf_data.set('HAVE_LZO2', lzo_dep.found())

eccodes_dep = dependency('eccodes', required: false)
if not eccodes_dep.found()
    eccodes_dep = dependency('grib_api', required: false)
endif
conf_data.set('HAVE_GRIBAPI', eccodes_dep.found())

dballe_dep = dependency('libdballe', required: false, version: '>= 8.14')
conf_data.set('HAVE_DBALLE', dballe_dep.found())

meteo_vm2_dep = dependency('meteo-vm2', required: false, version: '>= 1.0')
conf_data.set('HAVE_VM2', meteo_vm2_dep.found())

libarchive_dep = dependency('libarchive', required: false, version: '>= 3.2.0')
conf_data.set('HAVE_LIBARCHIVE', libarchive_dep.found())

libzip_dep = dependency('libzip', required: false, version: '>= 1.1')
conf_data.set('HAVE_LIBZIP', libzip_dep.found())

zlib_dep = dependency('zlib')

libcurl_dep = dependency('libcurl', required: false)
conf_data.set('HAVE_LIBCURL', libcurl_dep.found())

libssl_dep = dependency('libssl')
libcrypto_dep = dependency('libcrypto')

sqlite3_dep = dependency('sqlite3')

pymod = import('python')
python3 = pymod.find_installation('python3', required: false)

# dnl Check for python
# if test $enable_python != no
# then
#     AX_PYTHON_MODULE(werkzeug, yes)
#     AX_PYTHON_MODULE(setproctitle, yes)
#     AX_PYTHON_MODULE(jinja2, yes)
#     AX_PYTHON_MODULE(wreport, yes)
#     AX_PYTHON_MODULE_VERSION(dballe, 8.3)
#     AX_PYTHON_MODULE(h5py)
#     AX_PYTHON_MODULE(shapely)
#     AX_PYTHON_MODULE(netcdf4)
# fi

if python3.found()
    # FIXME: python3.path() is only available from meson 0.50: this is a workaround
    python3_path = python3.get_variable('BINDIR') / python3.get_variable('PYTHON') + python3.language_version()

    sphinx = find_program('sphinx-build', 'sphinx-build-3', 'sphinx-build-' + python3.language_version(), required: false)
    asciidoc = find_program('asciidoc', required: false)

    build_docs = sphinx.found()
    docdir = get_option('datadir') / 'doc' / meson.project_name()
else
    warning('Documentation disabled, requires doxygen, sphinx, and the breathe python module')
    build_docs = false
endif

# Generate config.h
configure_file(output: 'config.h', configuration: conf_data)

# Generate the builddir's version of run-local
run_local_cfg = configure_file(output: 'run-local', input: 'run-local.in', configuration: {
    'top_srcdir': meson.source_root(),
    'top_builddir': meson.build_root(),
})
# Just using the configure_file object in a custom_target command gives:
#   'AttributeError: 'File' object has no attribute 'replace'
# Using find_program on the resulting file works around that
# See https://github.com/mesonbuild/meson/issues/8039
run_local = find_program(run_local_cfg)

subdir('arki')

if python3.found()
    subdir('python')
endif

subdir('src')
subdir('conf')

if build_docs
    subdir('doc')
endif

# In order not to wait for sphinx to build documentation before tests are run,
# `built_docs` now has build_by_default: false, and we use this to add a 'ninja
# doc' target. It feeles kludgy, better would be to find out why built_docs is
# seen as a dependency of the tests
run_target('doc', command: ['/bin/true'], depends: [built_docs])

#AC_SYS_LARGEFILE
#
#dnl Check for missing functions in libc
#AC_CHECK_FUNC(vasprintf, , [
#       AC_DEFINE([USE_OWN_VASPRINTF], 1, [we need to use our own vasprintf])
#])
#AC_CHECK_FUNC(bswap_32, , [
#       AC_DEFINE([USE_OWN_BSWAP], 1, [we need to use our own bswap_* functions])
#])
#AC_CHECK_FUNC(strcasestr, [
#       AC_DEFINE([HAVE_STRCASESTR], 1, [we can use strcasestr])
#])
#
#dnl Enable extra compiler warnings
#AX_CHECK_COMPILE_FLAG([-Wredundant-move], [has_redundant_move=yes], [has_redundant_move=no])
#if test $has_redundant_move = yes
#then
#       AX_APPEND_FLAG([-Wno-error=redundant-move])
#fi
