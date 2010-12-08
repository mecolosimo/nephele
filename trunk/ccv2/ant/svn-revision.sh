#!/usr/bin/env bash
if [ ! -d .svn ]; then
    echo "release"
    exit
fi
SVN=`which svn`
if [ "x$SVN" != "x" ]; then
    # first try to update
    SVN up 2>&1 >/dev/null
    # now get revision
    REVISION=`SVN info | grep Revision: | awk {'print $2'}`
    echo $REVISION
else
    echo "release"
fi