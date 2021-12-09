#!/bin/bash

if [[ `basename "$PWD"` != "MQClient-GCP" && $PWD != "/home/circleci/project" ]] ; then
	echo "ERROR: Run from 'MQClient-GCP/' (not '$PWD')"
	exit 1
fi

export WIPACTEL_EXPORT_STDOUT=${WIPACTEL_EXPORT_STDOUT:="TRUE"}
export OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4318/v1/traces"
export WIPACTEL_SERVICE_NAME_PREFIX=mqclient-gcp

pip install tox
tox --notest -vv
. .tox/py/bin/activate
./resources/gcp-install.sh

`dirname "$0"`/run.sh
