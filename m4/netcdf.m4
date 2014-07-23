dnl Copyright (C) 2006--2010  Enrico Zini <enrico@enricozini.org>
dnl
dnl This program is free software; you can redistribute it and/or modify
dnl it under the terms of the GNU General Public License as published by
dnl the Free Software Foundation; either version 2, or (at your option)
dnl any later version.
dnl
dnl This program is distributed in the hope that it will be useful,
dnl but WITHOUT ANY WARRANTY; without even the implied warranty of
dnl MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
dnl GNU General Public License for more details.
dnl
dnl You should have received a copy of the GNU General Public License
dnl along with this program; if not, write to the Free Software Foundation,
dnl Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.

dnl usage: AC_CHECK_NETCDF([action if found], [action if not found])

AC_DEFUN([AC_CHECK_NETCDF],
[
dnl Look for NetCDF3 (which does not use pkg-config)
NETCDF_CFLAGS=

dnl Look for the headers
AC_CHECK_HEADER([netcdfcpp.h], [have_netcdf=yes], [have_netcdf=no])

if test $have_netcdf = no; then
    AC_CHECK_HEADER([netcdf-3/netcdfcpp.h], [have_netcdf=yes], [have_netcdf=no])
    NETCDF_CFLAGS=-I/usr/include/netcdf-3
fi

if test $have_netcdf = no; then
    AC_CHECK_HEADER([netcdf/netcdfcpp.h], [have_netcdf=yes], [have_netcdf=no])
    NETCDF_CFLAGS=-I/usr/include/netcdf
fi

dnl Look for the library
if test $have_netcdf = yes; then
    AC_CHECK_LIB([netcdf], [ncopts], [have_netcdf=yes], [have_netcdf=no])
fi

dnl Check that the library has what we need
if test $have_netcdf = yes; then
    saved_CPPFLAGS="$CFLAGS"
    saved_CFLAGS="$CFLAGS"
    saved_LIBS="$LIBS"
    CPPFLAGS="$CFLAGS $NETCDF_CFLAGS"
    CFLAGS="$CFLAGS $NETCDF_CFLAGS"
    LIBS="$LIBS -lnetcdf_c++ -lnetcdf"
    AC_MSG_CHECKING([for NcFile in -lnetcdf_c++])
    AC_LINK_IFELSE(
        [AC_LANG_PROGRAM(
            [#include <netcdfcpp.h>],
            [NcFile nc("example.nc")])],
        [
            AC_MSG_RESULT([yes])
            have_netcdf=yes
        ],
        [
            AC_MSG_RESULT([no])
            have_netcdf=no
        ]
    )
    CPPFLAGS="$saved_CPPFLAGS"
    CFLAGS="$saved_CFLAGS"
    LIBS="$saved_LIBS"
fi

dnl Define what is needed
if test $have_netcdf = yes; then
    if test -d /usr/lib/netcdf-3 ; then
        NETCDF_LIBS="-L/usr/lib/netcdf-3"
    fi
    NETCDF_LIBS="$NETCDF_LIBS -lnetcdf_c++ -lnetcdf"
    m4_default([$1],[])
else
    m4_default([$2],[AC_MSG_ERROR([NetCDF not found])])
fi
])
