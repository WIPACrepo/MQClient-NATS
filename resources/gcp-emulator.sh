#!/bin/bash
docker run --rm -it -p 8080:8085 \
    gcr.io/google.com/cloudsdktool/cloud-sdk:344.0.0-emulators \
    gcloud beta emulators pubsub start \
        --project=abc123 \
        --host-port=0.0.0.0:8085