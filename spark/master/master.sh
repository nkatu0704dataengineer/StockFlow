#!/bin/bash

set -e

export SPARK_HOME=/opt/spark
export SPARK_MASTER_HOST=${SPARK_MASTER_HOST:-$(hostname)}

mkdir -p /opt/spark/logs

exec ${SPARK_HOME}/bin/spark-class \
    org.apache.spark.deploy.master.Master \
    --host ${SPARK_MASTER_HOST} \
    --port ${SPARK_MASTER_PORT:-7077} \
    --webui-port ${SPARK_MASTER_WEBUI_PORT:-8080}