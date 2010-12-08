#!/usr/bin/env bash
# $Id: classifier.sh 1374 2010-10-29 13:07:26Z mcolosimo $
# Built on @timestamp@ for Version @version@

set -e
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# get our class paths and everything else
.  "$bin/ccv-config.sh"

java -Xms2000m -Xmx2000m -cp ${CCV_ROOT}/ccv-@version@.jar org.mitre.ccv.weka.CompositionCompositionVectorClassifiers "$@"
