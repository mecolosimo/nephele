#!/usr/bin/env bash
# ${ccv.replaceNote}
# ${mvn.timestamp}
CCV_ROOT=$(dirname "$0")
exec java -Xms256m -Xmx256m -cp ${CCV_ROOT}/ccv-@version@.jar org.mitre.bio.refdb.ReferenceDatabaseLoader "$@"