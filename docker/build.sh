#!/bin/bash

# Prerequisites:
#   - the presto-server-${VERSION}.tar.gz and presto-cli-${VERSION}-executable.jar shall exist
#     in the same directory.
#   - docker build driver is setup and ready to build multiple platform images. If not
#     check the documentation here: https://docs.docker.com/build/building/multi-platform/
#   - login to the container registry where images will be published to
set -e

if [ "$#" != "1" ]; then
    echo "usage: build.sh <version>"
    exit 1
fi

VERSION=$1; shift
TAG="${TAG:-latest}"
IMAGE_NAME="${IMAGE_NAME:-presto}"
REG_ORG="${REG_ORG:-docker.io/prestodb}"
PUBLISH="${PUBLISH:-false}"

# Compose full image name with tag
FULL_IMAGE_NAME="${REG_ORG}/${IMAGE_NAME}:${TAG}"

# Build image using regular docker build
docker build \
    --build-arg="PRESTO_VERSION=${VERSION}" \
    --build-arg="JMX_PROMETHEUS_JAVAAGENT_VERSION=0.20.0" \
    -t "${FULL_IMAGE_NAME}" \
    -f Dockerfile .

# Optionally push the image
if [[ "$PUBLISH" == "true" ]]; then
    docker push "${FULL_IMAGE_NAME}"
fi
