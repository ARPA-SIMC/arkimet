# splice.m4 serial 6
dnl Copyright (C) 2011 Enrico Zini
dnl This program is free software. It comes without any warranty, to
dnl the extent permitted by applicable law. You can redistribute it
dnl and/or modify it under the terms of the Do Whatever You Want To
dnl Public License, Version 2, as published by Sam Hocevar. See
dnl http://sam.zoy.org/wtfpl/COPYING for more details.

AC_DEFUN([ez_FUNC_SPLICE],
[
  AC_REQUIRE([gl_STDLIB_H_DEFAULTS])
  AC_REPLACE_FUNCS([splice])
  if test $ac_cv_func_splice = no; then
    HAVE_SPLICE=0
  fi
])
