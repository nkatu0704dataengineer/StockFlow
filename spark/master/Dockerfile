FROM apache/spark:3.5.1

LABEL maintainer="Gezim Sejdiu <g.sejdiu@gmail.com>, Giannis Mouchakis <gmouchakis@gmail.com>"

USER root

COPY master.sh /master.sh
RUN chmod +x /master.sh

RUN apt-get update && \
    apt-get install -y wget && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar && \
    mv hadoop-aws-3.3.2.jar /opt/spark/jars/ && \
    mv aws-java-sdk-bundle-1.11.1026.jar /opt/spark/jars/ && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV SPARK_MASTER_PORT=7077
ENV SPARK_MASTER_WEBUI_PORT=8080
ENV SPARK_MASTER_LOG=/opt/spark/logs

EXPOSE 7077 8080 6066

USER spark

CMD ["/bin/bash", "/master.sh"]