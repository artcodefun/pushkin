FROM cassandra:5.0

ARG JMX_EXPORTER_VERSION=1.5.0

RUN mkdir --parents /opt/jmx-exporter

ADD https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar /opt/jmx-exporter/jmx_prometheus_javaagent.jar

RUN chmod 0644 /opt/jmx-exporter/jmx_prometheus_javaagent.jar

COPY jmx-exporter.yaml /opt/jmx-exporter/config.yaml
