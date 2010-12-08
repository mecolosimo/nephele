#!/usr/bin/env bash
# $Id: ccv.sh 1381 2010-11-02 15:04:56Z mcolosimo $
# Built on @timestamp@ for Version @version@

set -e
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# get our class paths and everything else
.  "$bin/ccv-config.sh"

exec java -Xms256m -Xmx256m -jar ${CCV_HOME}/ccv-@version@.jar "$@"