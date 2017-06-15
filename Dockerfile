FROM        quay.io/prometheus/busybox:latest
MAINTAINER  The Prometheus Authors <prometheus-developers@googlegroups.com>

COPY         webhook-logger /bin/webhook-logger

EXPOSE       9099
VOLUME       [ "/webhook-logger" ]
WORKDIR      /webhook-logger
ENTRYPOINT   [ "/bin/webhook-logger" ]
