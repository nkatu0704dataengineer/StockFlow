FROM apache/spark:3.5.1

LABEL maintainer="Gezim Sejdiu <g.sejdiu@gmail.com>, Giannis Mouchakis <gmouchakis@gmail.com>"

USER root

COPY worker.sh /worker.sh

RUN chmod +x /worker.sh && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    mv hadoop-aws-3.3.4.jar /opt/spark/jars/ && \
    mv aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/

ENV SPARK_WORKER_WEBUI_PORT=8081
ENV SPARK_WORKER_LOG=/opt/spark/logs
ENV SPARK_MASTER=spark://stockflow-spark-master:7077

EXPOSE 8081

ENTRYPOINT ["/bin/bash", "/worker.sh"] 