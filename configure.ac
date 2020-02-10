dnl Process this file with autoconf to produce a configure script.

AC_INIT(arkimet, 1.20, [enrico@enricozini.org])
AC_CONFIG_SRCDIR([configure.ac])
AM_INIT_AUTOMAKE([foreign subdir-objects nostdinc])
AC_CONFIG_HEADER(config.h)
AC_CONFIG_HEADER(arki/libconfig.h)
AC_CONFIG_MACRO_DIR([m4])

dnl Use libtool
dnl AM_ENABLE_STATIC
dnl AM_DISABLE_SHARED
LT_INIT
LT_LANG([C++])
AC_PROG_LIBTOOL

AC_ARG_ENABLE([docs],
    [AS_HELP_STRING(
        [--enable-docs],
        [enable building of the documentation])],
    [],
    [enable_docs="yes"])

AC_ARG_ENABLE([bufr],
    [AS_HELP_STRING(
        [--enable-bufr],
        [build BUFR support])],
    [],
    [enable_bufr="yes"])

AC_ARG_ENABLE([vm2],
	      [AS_HELP_STRING(
	       [--enable-vm2],
	       [build VM2 support])],
	      [],
	      [enable_vm2="yes"])

AC_ARG_ENABLE([iotrace],
    [AS_HELP_STRING(
        [--enable-iotrace],
        [build with I/O profiling support])],
    [],
    [enable_iotrace="yes"])

AC_ARG_ENABLE([gcov],
    [AS_HELP_STRING(
        [--enable-gcov],
        [build binaries that generate code coverage information (used for development only)])],
    [],
    [enable_gcov="no"])

AC_ARG_ENABLE([geos],
    [AS_HELP_STRING(
        [--enable-geos],
        [build with geos support])],
    [],
    [enable_geos="yes"])

AC_ARG_ENABLE([python],
    [AS_HELP_STRING(
        [--enable-python],
        [build with python support])],
    [],
    [enable_python="yes"])

AC_ARG_ENABLE([arpae-tests],
    [AS_HELP_STRING(
        [--enable-arpae-tests],
        [Enable Arpae tests])])

if test x$enable_gcov = xyes
then
	AM_DISABLE_SHARED
fi

dnl To use subdirs
AC_PROG_MAKE_SET

AC_LANG([C++])

AC_ISC_POSIX
AC_PROG_CC
AC_PROG_CXX

dnl Use c++11
AX_CXX_COMPILE_STDCXX_11(noext)

AM_PROG_LEX
AC_PROG_YACC

AM_PROG_CC_STDC
AC_HEADER_STDC

dnl Needed by subdir-objects
AM_PROG_CC_C_O

PKG_PROG_PKG_CONFIG

dnl Support large files on 32bit systems
AC_SYS_LARGEFILE
AC_FUNC_FSEEKO

dnl See http://www.gossamer-threads.com/lists/xen/devel/275132
FILE_OFFSET_BITS_64=
if test $ac_cv_sys_file_offset_bits = 64
then
    FILE_OFFSET_BITS_64=yes
fi
AM_CONDITIONAL([FILE_OFFSET_BITS_64], [test x"$FILE_OFFSET_BITS_64" = x"yes"])

dnl Check what libc features are available on this system
gl_CHECK_TYPE_STRUCT_DIRENT_D_TYPE
ez_FUNC_SPLICE

dnl Call in pkg-config explicitly, so we can safely put the first
dnl PKG_CHECK_EXISTS inside an if
PKG_PROG_PKG_CONFIG

dnl Compulsory libraries
AX_LIB_SQLITE3([3.14])
if test x"$success" = x"no"; then
	AC_MSG_WARN([Cannot find an appropriate version of SQLite, using the embedded version])

	SQLITE3_CFLAGS="-I`pwd`/embedded/sqlite/"
        AC_SUBST(SQLITE3_CFLAGS)
	SQLITE3_LDFLAGS="`pwd`/embedded/sqlite/libsqlite3.la"
        AC_SUBST(SQLITE3_LDFLAGS)
	SQLITE3_VERSION="embedded"
        AC_SUBST(SQLITE3_VERSION)
        AC_DEFINE(HAVE_SQLITE3, [], [SQLite 3 is available])
fi
AM_CONDITIONAL([SQLITE_EMBEDDED], [test x"$SQLITE3_VERSION" = x"embedded"])

dnl Optional readline
VL_LIB_READLINE
if test "$vl_cv_lib_readline" != "no"; then
	have_readline=yes
else
	have_readline=no
fi
AM_CONDITIONAL([READLINE], [test x"$have_readline" = x"yes"])

PKG_CHECK_EXISTS([libarchive > 3.2.0], [have_libarchive=yes], [have_libarchive=no])
if test $have_libarchive = yes
then
	AC_DEFINE([HAVE_LIBARCHIVE], 1, [libarchive functions are available])
	PKG_CHECK_MODULES(LIBARCHIVE, [libarchive])
else
	AC_MSG_WARN([compiling without libarchive])
fi

PKG_CHECK_EXISTS([libzip >= 1.1], [have_libzip=yes], [have_libzip=no])
if test $have_libzip = yes
then
	AC_DEFINE([HAVE_LIBZIP], 1, [libzip functions are available])
	PKG_CHECK_MODULES(LIBZIP, [libzip])
else
	AC_MSG_WARN([compiling without libzip])
fi

dnl Look for DB-All.e C++ libraries, for BUFR support
if test $enable_bufr = yes
then
	dnl Check for DB-All.e C++ libraries (this needs to be outside of the if, 
	PKG_CHECK_EXISTS([libdballe >= 8.3], [have_dballe=yes], [have_dballe=no])
	if test $have_dballe = yes
	then
		AC_DEFINE([HAVE_DBALLE], 1, [dballe functions are available])
		PKG_CHECK_MODULES(DBALLE,[libdballe >= 8.3])
	else
		AC_MSG_WARN([compiling without DB-All.e: BUFR support will be disabled])
	fi
else
	have_dballe=no
fi
AM_CONDITIONAL([DBALLE], [test x"$have_dballe" = x"yes"])

dnl Check for zlib
PKG_CHECK_EXISTS([zlib], [have_zlib=yes], [have_zlib=no])
if test x$have_zlib = xyes
then
	PKG_CHECK_MODULES(ZLIB, zlib)
else
	dnl We don't always have the luxury of .pc files for zlib, it seems
	have_zlib=yes
	AC_CHECK_LIB([z], [gzopen], [true], [have_zlib=no])
	AC_CHECK_HEADER([zlib.h], [true], [have_zlib=no])
	ZLIB_CFLAGS=
	ZLIB_LIBS="-lz"
fi
AC_SUBST(ZLIB_CFLAGS)
AC_SUBST(ZLIB_LIBS)

dnl Check for lua
PKG_CHECK_EXISTS([lua5.2], [have_lua=yes], [have_lua=no])
if test x$have_lua = xyes
then
    PKG_CHECK_MODULES(LUA,lua5.2,,[have_lua=no])
else
	PKG_CHECK_EXISTS([lua5.1], [have_lua=yes], [have_lua=no])
	if test x$have_lua = xyes
	then
		PKG_CHECK_MODULES(LUA,lua5.1,,[have_lua=no])
	else
		PKG_CHECK_EXISTS([lua], [have_lua=yes], [have_lua=no])
		if test x$have_lua = xyes
		then
			PKG_CHECK_MODULES(LUA,lua,,[have_lua=no])
		else
			dnl We don't always have the luxury of .pc files for lua, it seems
			have_lua=yes
			AC_CHECK_LIB([lua], [lua_gettop], [true], [have_lua=no], [-lm])
			AC_CHECK_HEADER([lua.h], [true], [have_lua=no])
			LUA_CFLAGS=
			LUA_LIBS="-llua"
		fi
	fi
fi
if test x$have_lua = xyes
then
    AC_DEFINE([HAVE_LUA], 1, [lua is available])
    AC_SUBST(LUA_CFLAGS)
    AC_SUBST(LUA_LIBS)
fi
AM_CONDITIONAL([LUA], [test x"$have_lua" = x"yes"])

dnl Check for eccodes
PKG_CHECK_EXISTS([eccodes], [have_gribapi=yes], [have_gribapi=no])
if test x$have_gribapi = xyes
then
	PKG_CHECK_MODULES(GRIBAPI, [eccodes], , [have_gribapi=no])
fi

if test x$have_gribapi = xno
then
	dnl Check for grib_api
	have_gribapi=yes
	AC_CHECK_LIB([grib_api], [grib_get_long], [true], [have_gribapi=no], [-ljpeg -lm])
	AC_CHECK_HEADER([grib_api.h], [true], [have_gribapi=no])
	GRIBAPI_CFLAGS=
	GRIBAPI_LIBS="-lgrib_api -ljpeg -lm"
fi

if test $have_gribapi = yes
then
	AC_DEFINE([HAVE_GRIBAPI], 1, [grib_api functions are available])
	AC_SUBST(GRIBAPI_CFLAGS)
	AC_SUBST(GRIBAPI_LIBS)

	if test x$have_lua = xno
	then
		AC_MSG_WARN([Cannot find lua: grib scanning will be disabled])
		have_gribapi=no
	fi	

else
	AC_MSG_WARN([compiling without grib_api])
fi
AM_CONDITIONAL([GRIBAPI], [test x"$have_gribapi" = x"yes"])

dnl Check for Curl
LIBCURL_CHECK_CONFIG([], [], [have_curl=yes], [have_curl=no])
AM_CONDITIONAL([CURL], [test x"$have_curl" = x"yes"])

PKG_CHECK_MODULES([LIBSSL], [libssl])
AC_SUBST(LIBSSL_CFLAGS)
AC_SUBST(LIBSSL_LIBS)

dnl Check for Geos
if test x$enable_geos = xyes
then
    have_geos=yes
    AC_PATH_PROG(GEOS_CONFIG, geos-config, NOTFOUND)
    if test $GEOS_CONFIG = NOTFOUND
    then
        AC_MSG_WARN([geos-config not found])
        have_geos=no
    else
        GEOS_VERSION=`geos-config --version | cut -d. -f1`
        GEOS_CFLAGS=`geos-config --cflags`
        GEOS_LIBS=`geos-config --libs`
        AC_SUBST(GEOS_CFLAGS)
        AC_SUBST(GEOS_LIBS)
    fi
    if test x$have_geos = xyes
    then
        AC_DEFINE([HAVE_GEOS], 1, [GEOS is available])
        AC_DEFINE_UNQUOTED([GEOS_VERSION], $GEOS_VERSION, [GEOS version number])
    fi
else
    have_geos=no
fi
AM_CONDITIONAL([GEOS], [test x"$have_geos" = x"yes"])

dnl Check for liblzo2
have_lzo=yes
AC_CHECK_LIB([lzo2], [__lzo_init_v2], [true], [have_lzo=no], [])
AC_CHECK_HEADER([lzo/lzoconf.h], [true], [have_lzo=no])
if test $have_lzo = yes
then
	LZO_CFLAGS=
	LZO_LIBS="-llzo2"
	LZO_VERSION=`sed -nre 's/#define LZO_VERSION_STRING[[:blank:]]+"(.+)"/\1/p' /usr/include/lzo/lzoconf.h`
	AC_DEFINE([HAVE_LZO], 1, [lzo functions are available])
else
	AC_MSG_WARN([Cannot find an appropriate version of LZO, using the embedded version])
	LZO_CFLAGS="-I`pwd`/embedded/minilzo/"
	LZO_LIBS="-L`pwd`/embedded/minilzo/ -lminilzo"
	LZO_VERSION="embedded"
	AC_DEFINE([HAVE_MINILZO], 1, [minilzo functions are available])
	have_lzo=yes
fi
AC_SUBST(LZO_CFLAGS)
AC_SUBST(LZO_LIBS)
AC_SUBST(LZO_VERSION)
AM_CONDITIONAL([LZO], [test x"$have_lzo" = x"yes"])
AM_CONDITIONAL([LZO_EMBEDDED], [test x"$LZO_VERSION" = x"embedded"])

dnl Check for vm2 support
if test $enable_vm2 = yes
then
	dnl Check for VM2 library
	PKG_CHECK_EXISTS([meteo-vm2 >= 1.0], [have_vm2=yes], [have_vm2=no])
	if test $have_vm2 = yes
	then
		AC_DEFINE([HAVE_VM2], 1, [vm2 format enabled])
		PKG_CHECK_MODULES([VM2], [meteo-vm2])
	else
		AC_MSG_WARN([compiling without libvm2: VM2 support will be disabled])
	fi
else
	have_vm2=no
fi
AM_CONDITIONAL([VM2], [test x"$have_vm2" = x"yes" ])


dnl Check for python
if test $enable_python = yes
then
AM_PATH_PYTHON([3.3], [enable_python=$am_cv_python_version], [enable_python=no])
if test $enable_python != no
then
    PYTHON_CFLAGS=`unset CFLAGS; $PYTHON-config --cflags`
    PYTHON_LDFLAGS=`unset CFLAGS; $PYTHON-config --ldflags`
    AC_SUBST(PYTHON_CFLAGS)
    AC_SUBST(PYTHON_LDFLAGS)
    AC_DEFINE(HAVE_PYTHON, 1, [Have Python bindings])
    AX_PYTHON_MODULE(werkzeug, yes)
    AX_PYTHON_MODULE(setproctitle, yes)
    AC_CHECK_PROGS(NOSE, [nose2-3 nosetests-3.6 nosetests-3.5 nosetests-3.4 nosetests-3.3 nosetests-$PYTHON_VERSION])
    if test x$NOSE = x
    then
        AC_MSG_ERROR([Python nose not found])
    fi
    AX_PYTHON_MODULE(jinja2, yes)
    AX_PYTHON_MODULE(wreport, yes)
    AX_PYTHON_MODULE_VERSION(dballe, 8.3)
    AX_PYTHON_MODULE(h5py)
    AX_PYTHON_MODULE(shapely)
fi
fi
AM_CONDITIONAL([DO_PYTHON], [test "$enable_python" != "no"])

if test x$enable_docs = xyes
then
    AC_CHECK_PROGS(SPHINX_BUILD, [sphinx-build sphinx-build-3 sphinx-build-3.6], [no])
    if test $SPHINX_BUILD = no
    then
        enable_docs=no
    fi
fi
AC_SUBST(SPHINX_BUILD)
AM_CONDITIONAL([DO_DOCS], [test x"$enable_docs" = x"yes"])

dnl Enable tests that require ARPAE-specific grib_api/eccodes definition patches
if test "x$enable_arpae_tests" = xyes
then
    AC_DEFINE([ARPAE_TESTS], 1, [enable ARPAE-specific unit tests])
fi


dnl Enable compile-time options for I/O profiling, if requested
if test $enable_iotrace = yes
then
	AC_DEFINE([ARKI_IOTRACE], 1, [I/O profiling is enabled])
fi
AM_CONDITIONAL([IOTRACE], [test x"$enable_iotrace" = x"yes"])

LIBARKI_VERSION_INFO=`echo $VERSION | awk -F. '{ printf "%d:%d:%d", $1+$2, $3, $2 }'`
AC_SUBST(LIBARKI_VERSION_INFO)

if test x$docdir = x
then
	docdir='${datadir}/doc/${PACKAGE_TARNAME}'
	AC_SUBST(docdir)
fi

confdir='$(sysconfdir)'"/$PACKAGE"
AC_SUBST(confdir)

postprocdir='$(libdir)'"/$PACKAGE"
AC_SUBST(postprocdir)

serverdir='$(prefix)'"/share/$PACKAGE/server"
AC_SUBST(serverdir)

tabledir='$(pkgdatadir)'
AC_SUBST(tabledir)
dnl def_tabledir=`echo "${sharedstatedir}/lib/$PACKAGE/tables"`
dnl AC_DEFINE_UNQUOTED(TABLE_DIR, "$def_tabledir", "Default location for table files")

if test x"$GXX" = x"yes"; then
	EXTRA_CXXFLAGS="-Wall"
	EXTRA_LDFLAGS=""
fi
if test x"$CXX" = x"xlc" -o x"$CXX" = x"xlC"; then
	dnl We need _XOPEN_SOURCE=700 to get mkdtemp on AIX
	EXTRA_CXXFLAGS="-qrtti=all -qtls=initial-exec $CXXFLAGS"
	EXTRA_LDFLAGS="-qrtti=all -qtls=initial-exec $LDFLAGS"
fi
AC_SUBST(EXTRA_CXXFLAGS)
AC_SUBST(EXTRA_LDFLAGS)

if test x$enable_gcov = xyes
then
	DEVEL_CXXFLAGS="--coverage"
	DEVEL_LIBS="--coverage"
fi
AC_SUBST(DEVEL_CXXFLAGS)
AC_SUBST(DEVEL_LIBS)

dnl Conditional to enable GCOV snippets
AM_CONDITIONAL([GCOV], [test x"$enable_gcov" = x"yes"])

dnl asciidoc for arkiguide
AC_CHECK_PROG([have_asciidoc], [asciidoc], [yes], [no])
AM_CONDITIONAL([ASCIIDOC], [test x"$have_asciidoc" = x"yes"])

dnl Developer only: allow to build/test only part of the library
AM_CONDITIONAL([DEVEL_DO_DATASETS], [true])

AC_CONFIG_FILES([
Makefile
conf/Makefile
embedded/Makefile
embedded/sqlite/Makefile
embedded/minilzo/Makefile
arki/Makefile
src/Makefile
python/Makefile
doc/Makefile
test/Makefile
])
AC_OUTPUT

AC_MSG_NOTICE([
===================================================
$PACKAGE_NAME-$PACKAGE_VERSION configuration:
AS_HELP_STRING([BUFR via DB-All.e:], [$have_dballe])
dnl AS_HELP_STRING([GRIB 1 via Meteosatlib:], [$have_libgrib])
AS_HELP_STRING([GRIB 1+2 via grib_api:], [$have_gribapi])
AS_HELP_STRING([VM2 support:], [$have_vm2])
AS_HELP_STRING([CURL support:], [$have_curl])
AS_HELP_STRING([Lua support:], [$have_lua])
AS_HELP_STRING([GEOS support:], [$have_geos, version $GEOS_VERSION])
AS_HELP_STRING([GNU readline support:], [$have_readline])
AS_HELP_STRING([libarchive support:], [$have_libarchive])
AS_HELP_STRING([libzip support:], [$have_libzip])
AS_HELP_STRING([LZO compression:], [$LZO_VERSION])
AS_HELP_STRING([Zlib compression:], [$have_zlib])
AS_HELP_STRING([SQLite version:], [$SQLITE3_VERSION])
AS_HELP_STRING([Python:], [$enable_python])
AS_HELP_STRING([ARPAE specific tests:], [$enable_arpae_tests])
AS_HELP_STRING([I/O profiling:], [$enable_iotrace])
AS_HELP_STRING([documentation:], [$enable_docs])
AS_HELP_STRING([Code coverage:], [$enable_gcov])
dnl AS_HELP_STRING([BUFR support via DB-All.e:], [$have_dballe])
dnl AS_HELP_STRING([GRIB1 support via Meteosatlib's libgrib:], [$have_libgrib])
===================================================])

