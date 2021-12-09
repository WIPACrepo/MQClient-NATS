#!/bin/bash
docker run --rm -it \
    --name gcloud-config \
    -v `pwd`/tokens/gcp-service-account-token.json:/tmp/gcp-service-account-token.json \
    google/cloud-sdk gcloud auth \
    activate-service-account \
    --key-file /tmp/gcp-service-account-token.json