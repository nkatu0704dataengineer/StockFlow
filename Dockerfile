FROM apache/spark:3.5.1

USER root

# Install dependencies to build Python 3.13
RUN apt-get update && \
    apt-get install -y \
        wget \
        build-essential \
        libssl-dev \
        zlib1g-dev \
        libbz2-dev \
        libreadline-dev \
        libsqlite3-dev \
        libffi-dev \
        liblzma-dev \
        libncurses5-dev \
        libgdbm-dev \
        tk-dev \
        uuid-dev \
        xz-utils \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Build Python 3.13
RUN cd /tmp && \
    wget https://www.python.org/ftp/python/3.13.13/Python-3.13.13.tgz && \
    tar -xzf Python-3.13.13.tgz && \
    cd Python-3.13.13 && \
    ./configure --enable-optimizations && \
    make -j"$(nproc)" && \
    make altinstall && \
    cd / && \
    rm -rf /tmp/Python-3.13.13*

# Tell Spark to use Python 3.13
ENV PYSPARK_PYTHON=/usr/local/bin/python3.13
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.13

COPY worker.sh /worker.sh

RUN chmod +x /worker.sh

ENV SPARK_WORKER_WEBUI_PORT=8081
ENV SPARK_WORKER_LOG=/opt/spark/logs
ENV SPARK_MASTER=spark://stockflow-spark-master:7077

EXPOSE 8081

ENTRYPOINT ["/bin/bash", "/worker.sh"]