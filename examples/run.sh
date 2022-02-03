#!/bin/bash

if [[ `basename "$PWD"` != "MQClient-NATS" && $PWD != "/home/circleci/project" ]] ; then
	echo "ERROR: Run from 'MQClient-NATS/' (not '$PWD')"
	exit 1
fi

python examples/worker.py &
python examples/server.py