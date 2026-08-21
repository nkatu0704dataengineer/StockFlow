#!/bin/bash

set -e

export SPARK_HOME=/opt/spark

mkdir -p /opt/spark/logs

exec ${SPARK_HOME}/bin/spark-class \
    org.apache.spark.deploy.worker.Worker \
    --webui-port ${SPARK_WORKER_WEBUI_PORT:-8081} \
    ${SPARK_MASTER}