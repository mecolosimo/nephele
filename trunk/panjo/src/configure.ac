#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.
#
# Originally (?) from E. Jason Riedy's Spring 2004 CS 267 code.
#

AC_PREREQ(2.50)
AC_INIT(panjo, 1.0, bullard@stat.berkeley.edu)

AM_INIT_AUTOMAKE

AC_CONFIG_HEADER([config.h])
AC_CONFIG_MACRO_DIR([m4])

# Checks for programs.
AC_PROG_CC([icc ecc xlc_r xlc gcc cc])
AC_ISC_POSIX
AC_PROG_RANLIB

# Checks for libraries.
AC_CHECK_LIB([m], [sqrt])
ACX_MPI([HAVE_MPI=true])
AM_CONDITIONAL(COND_HAVE_MPI,[test x$HAVE_MPI = xtrue])
ACX_C_OPENMP([HAVE_OPENMP=true])
AC_CHECK_FUNCS(fmax)
AC_SUBST(HAVE_FMAX)
AC_CHECK_FUNCS(isnan)
AC_SUBST(HAVE_ISNAN)

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([float.h stdlib.h string.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_INLINE
AC_C_RESTRICT

AC_CONFIG_FILES([Makefile])
dnl AC_CONFIG_FILES([mpi/Makefile openmp/Makefile])
dnl AC_CONFIG_FILES([upc/Makefile titanium/Makefile])
AC_OUTPUT

echo ""
if test x$HAVE_MPI = xtrue; then
  echo  "              mpi"
fi

