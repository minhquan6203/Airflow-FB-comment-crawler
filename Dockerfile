FROM apache/airflow:2.9.2

# USER root
# RUN apt-get update \
#   && apt-get install -y --no-install-recommends \
#          default-jdk \
#   && apt-get autoremove -yqq --purge \
#   && apt-get clean \
#   && rm -rf /var/lib/apt/lists/*
# USER airflow
# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# RUN pip install --no-cache-dir "apache-airflow==2.9.2" apache-airflow-providers-apache-spark==3.1.1

COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
