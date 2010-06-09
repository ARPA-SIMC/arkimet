dnl Process this file with autoconf to produce a configure script.

AC_INIT(arkimet, 0.43, [enrico@enricozini.org])
AC_CONFIG_SRCDIR([configure.ac])
AM_INIT_AUTOMAKE([foreign subdir-objects nostdinc])
AM_CONFIG_HEADER(config.h)
AM_CONFIG_HEADER(arki/config.h)

AC_CONFIG_MACRO_DIR([m4])

AC_ARG_ENABLE([bufr],
    [AS_HELP_STRING(
        [--enable-bufr],
        [build BUFR support])],
    [],
    [enable_bufr="yes"])

AC_ARG_ENABLE([gcov],
    [AS_HELP_STRING(
        [--enable-gcov],
        [build binaries that generate code coverage information (used for development only)])],
    [],
    [enable_gcov="no"])

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

AM_PROG_LEX
AC_PROG_YACC

AM_PROG_CC_STDC
AC_HEADER_STDC

dnl Check that exceptions are working
dnl AC_CHECK_CXX_EH

dnl Needed by subdir-objects
AM_PROG_CC_C_O

dnl Use libtool
dnl AM_ENABLE_STATIC
dnl AM_DISABLE_SHARED
AM_PROG_LIBTOOL

dnl Call in pkg-config explicitly, so we can safely put the first
dnl PKG_CHECK_EXISTS inside an if
PKG_PROG_PKG_CONFIG

dnl Compulsory libraries
AX_LIB_SQLITE3([3.6])
if test x"$success" = x"no"; then
	AC_MSG_WARN([Cannot find an appropriate version of SQLite, using the embedded version])

	SQLITE3_CFLAGS="-I`pwd`/embedded/sqlite/"
        AC_SUBST(SQLITE3_CFLAGS)
	SQLITE3_LDFLAGS="-L`pwd`/embedded/sqlite/ -lsqlite3"
        AC_SUBST(SQLITE3_LDFLAGS)
	SQLITE3_VERSION="embedded"
        AC_SUBST(SQLITE3_VERSION)
        AC_DEFINE(HAVE_SQLITE3, [], [SQLite 3 is available])
fi
AM_CONDITIONAL([SQLITE_EMBEDDED], [test x"$SQLITE3_VERSION" = x"embedded"])

dnl Wibble is embedded
dnl LIBWIBBLE_DEFS
AM_CONDITIONAL([WIBBLE_STANDALONE], [false])

dnl Optional readline
AC_LIB_READLINE
if test "$ac_cv_lib_readline" != "no"; then
	have_readline=yes
else
	have_readline=no
fi
AM_CONDITIONAL([READLINE], [test x"$have_readline" = x"yes"])

dnl Look for DB-All.e C++ libraries, for BUFR support
if test $enable_bufr = yes
then
	dnl Check for DB-All.e C++ libraries (this needs to be outside of the if, 
	PKG_CHECK_EXISTS([libdballepp >= 4.0.15], [have_dballe=yes], [have_dballe=no])
	if test $have_dballe = yes
	then
		AC_DEFINE([HAVE_DBALLE], 1, [dballe functions are available])
		LIBDBALLEPP_DEFS
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
		LUA_LIBS="-llualib -llua"
	fi
fi
if test x$have_lua = xyes
then
	AC_DEFINE([HAVE_LUA], 1, [lua is available])
fi
AM_CONDITIONAL([LUA], [test x"$have_lua" = x"yes"])

dnl Check for grib_api
have_gribapi=yes
AC_CHECK_LIB([grib_api], [grib_get_api_version], [true], [have_gribapi=no], [-lm -ljasper])
AC_CHECK_HEADER([grib_api.h], [true], [have_gribapi=no])
if test $have_gribapi = yes
then
	AC_DEFINE([HAVE_GRIBAPI], 1, [grib_api functions are available])
	GRIBAPI_CFLAGS=
	GRIBAPI_LIBS="-lgrib_api"
	AC_SUBST(GRIBAPI_CFLAGS)
	AC_SUBST(GRIBAPI_LIBS)

	if test x$have_lua = xno
	then
		AC_MSG_WARN([Cannot find lua: grib scanning will be disabled])
		have_gribapi=no
	fi	

	AC_SUBST(LUA_CFLAGS)
	AC_SUBST(LUA_LIBS)
else
	AC_MSG_WARN([compiling without grib_api])
fi
AM_CONDITIONAL([GRIBAPI], [test x"$have_gribapi" = x"yes"])

dnl Check for Curl
LIBCURL_CHECK_CONFIG([], [], [have_curl=yes], [have_curl=no])
AM_CONDITIONAL([CURL], [test x"$have_curl" = x"yes"])

dnl Check for Geos
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
fi
AC_SUBST(LZO_CFLAGS)
AC_SUBST(LZO_LIBS)
AC_SUBST(LZO_VERSION)
AM_CONDITIONAL([LZO], [test x"$have_lzo" = x"yes"])
AM_CONDITIONAL([LZO_EMBEDDED], [test x"$LZO_VERSION" = x"embedded"])

dnl Check for python
AM_PATH_PYTHON

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

tabledir='$(pkgdatadir)'
AC_SUBST(tabledir)
dnl def_tabledir=`echo "${sharedstatedir}/lib/$PACKAGE/tables"`
dnl AC_DEFINE_UNQUOTED(TABLE_DIR, "$def_tabledir", "Default location for table files")

if test x"$GXX" = x"yes"; then
	CXXFLAGS="-Wall $CXXFLAGS"
fi
LIBS="$LIBS -lm"

if test x$enable_gcov = xyes
then
	DEVEL_CFLAGS="--coverage"
	DEVEL_LIBS="--coverage"
fi
AC_SUBST(DEVEL_CFLAGS)
AC_SUBST(DEVEL_LIBS)

dnl Conditional to enable GCOV snippets
AM_CONDITIONAL([GCOV], [test x"$enable_gcov" = x"yes"])


AC_CONFIG_FILES([
Makefile
embedded/Makefile
embedded/sqlite/Makefile
embedded/minilzo/Makefile
wibble/Makefile
arki/Makefile
src/Makefile
])
AC_OUTPUT

AC_MSG_NOTICE([
===================================================
$PACKAGE_NAME-$PACKAGE_VERSION configuration:
AS_HELP_STRING([BUFR via DB-All.e:], [$have_dballe])
dnl AS_HELP_STRING([GRIB 1 via Meteosatlib:], [$have_libgrib])
AS_HELP_STRING([GRIB 1+2 via grib_api:], [$have_gribapi])
AS_HELP_STRING([CURL support:], [$have_curl])
AS_HELP_STRING([Lua support:], [$have_lua])
AS_HELP_STRING([GEOS support:], [$have_geos, version $GEOS_VERSION])
AS_HELP_STRING([GNU readline support:], [$have_readline])
AS_HELP_STRING([LZO compression:], [$LZO_VERSION])
AS_HELP_STRING([SQLite version:], [$SQLITE3_VERSION])
AS_HELP_STRING([Code coverage:], [$enable_gcov])
dnl AS_HELP_STRING([BUFR support via DB-All.e:], [$have_dballe])
dnl AS_HELP_STRING([GRIB1 support via Meteosatlib's libgrib:], [$have_libgrib])
dnl AS_HELP_STRING([GRIB1 and GRIB2 support via grib_api and Lua:], [$have_gribapi])
===================================================])

