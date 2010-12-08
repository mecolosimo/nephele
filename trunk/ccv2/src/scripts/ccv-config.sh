#!/usr/bin/env bash
# $Id: ccv-config.sh 1374 2010-10-29 13:07:26Z mcolosimo $

# resolve links - $0 may be a softlink
this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path
bin=`dirname "${this}"`
bin=`cd "${bin}"; pwd`
export CCV_HOME=`dirname "${bin}"`