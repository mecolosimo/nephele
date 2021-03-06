#!/usr/bin/env bash
# $Id$
# Built on @timestamp@ for Version @version@

set -e
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# get our class paths and everything else
.  "$bin/ccv-config.sh"

exec java -Xms256m -Xmx256m -cp ${CCV_ROOT}/ccv-@version@.jar org.mitre.bio.CCV.CCVComparisonMain "$@"

