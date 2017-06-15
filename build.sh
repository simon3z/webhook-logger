#!/bin/sh
IMAGENAME=${IMAGENAME:-juliusv/webhook-logger}
EXTRACTNAME=${EXTRACTNAME:-webhook-logger-extract}

echo Building ${IMAGENAME}:build

docker build -t juliusv/webhook-logger:build . -f Dockerfile.build

docker create --name ${EXTRACTNAME} juliusv/webhook-logger:build
docker cp ${EXTRACTNAME}:/go/bin/webhook-logger webhook-logger
docker rm -f ${EXTRACTNAME}

echo Building ${IMAGENAME}:latest

docker build --no-cache -t ${IMAGENAME:-juliusv/webhook-logger}:latest .
