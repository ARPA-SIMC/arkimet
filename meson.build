project('arkimet', 'cpp', version: '1.29', license : 'GPL-2.0-or-later', default_options: ['warning_level=3', 'cpp_std=c++11'])

add_global_arguments('-Wextra', '-Wno-unused-parameter', language : 'cpp')

conf_dir = get_option('sysconfdir') / 'arkimet'
doc_dir = get_option('datadir') / 'arkimet'

conf_data = configuration_data()
conf_data.set_quoted('PACKAGE_VERSION', meson.project_version())
conf_data.set('ARKI_IOTRACE', true)
conf_data.set_quoted('CONF_DIR', conf_dir)
conf_data.set_quoted('POSTPROC_DIR', get_option('libdir') / 'arkimet')

toplevel_inc = include_directories('.')

# Dependencies
flex = find_program('flex')
bison = find_program('bison')
help2man = find_program('help2man')

thread_dep = dependency('threads')

geos_dep = dependency('', required: false)
geos_config = find_program('geos-config', required: false)
if geos_config.found()
    geos_includes = run_command(geos_config, '--includes').stdout().split()
    geos_cflags = run_command(geos_config, '--cflags').stdout().split()
    geos_cclibs = run_command(geos_config, '--cclibs').stdout().split()
    geos_version = run_command(geos_config, '--version').stdout().strip()
    geos_dep = declare_dependency(
        include_directories: geos_includes,
        compile_args: geos_cflags,
        link_args: geos_cclibs,
        version: geos_version,
    )
endif
conf_data.set('HAVE_GEOS', geos_dep.found())

lua_dep = dependency('lua5.2', required: false)
if not lua_dep.found()
    lua_dep = dependency('lua5.1', required: false)
endif
if not lua_dep.found()
    lua_dep = dependency('lua', required: false)
endif
conf_data.set('HAVE_LUA', lua_dep.found())

lzo_dep = dependency('lzo2')
conf_data.set('HAVE_LZO', lzo_dep.found())

eccodes_dep = dependency('eccodes', required: false)
if not eccodes_dep.found()
    eccodes_dep = dependency('grib_api', required: false)
endif
conf_data.set('HAVE_GRIBAPI', eccodes_dep.found())

dballe_dep = dependency('libdballe', required: false, version: '>= 8.3')
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
# if test $enable_python = yes
# then
# AM_PATH_PYTHON([3.3], [enable_python=$am_cv_python_version], [enable_python=no])
# if test $enable_python != no
# then
#     PYTHON_CFLAGS=`unset CFLAGS; $PYTHON-config --cflags`
#     PYTHON_LDFLAGS=`unset CFLAGS; $PYTHON-config --ldflags`
#     AC_SUBST(PYTHON_CFLAGS)
#     AC_SUBST(PYTHON_LDFLAGS)
#     AC_DEFINE(HAVE_PYTHON, 1, [Have Python bindings])
#     AX_PYTHON_MODULE(werkzeug, yes)
#     AX_PYTHON_MODULE(setproctitle, yes)
#     AC_CHECK_PROGS(NOSE, [nose2-3 nosetests-3.6 nosetests-3.5 nosetests-3.4 nosetests-3.3 nosetests-$PYTHON_VERSION])
#     if test x$NOSE = x
#     then
#         AC_MSG_ERROR([Python nose not found])
#     fi
#     AX_PYTHON_MODULE(jinja2, yes)
#     AX_PYTHON_MODULE(wreport, yes)
#     AX_PYTHON_MODULE_VERSION(dballe, 8.3)
#     AX_PYTHON_MODULE(h5py)
#     AX_PYTHON_MODULE(shapely)
#     AX_PYTHON_MODULE(netcdf4)
# fi
# fi
# AM_CONDITIONAL([DO_PYTHON], [test "$enable_python" != "no"])

if python3.found()
    # FIXME: python3.path() is only available from meson 0.50: this is a workaround
    python3_path = python3.get_variable('BINDIR') / python3.get_variable('PYTHON') + python3.language_version()

    nose = find_program('nose2-3', 'nosetests-3.6', 'nosetests-3.5', 'nosetests-3.4', 'nosetests-3.3', 'nosetests-' + python3.language_version())

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
