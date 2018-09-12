# Build the nar file

FROM maven:3.5 as builder

ARG BASE_DIR=/source

COPY . ${BASE_DIR}

WORKDIR ${BASE_DIR}

RUN mvn clean package -DskipTests


# ----------------
# Build a new Nifi image with the newly generated nar file included

FROM apache/nifi:1.4.0

ARG BASE_DIR=/source

ENV NIFI_BASE_DIR /opt/nifi
ENV NIFI_HOME ${NIFI_BASE_DIR}/nifi-1.4.0

COPY --from=builder ${BASE_DIR}/nifi-opcua-nar/target/*.nar ${NIFI_HOME}/lib/nifi-opcua.nar

EXPOSE 8080 8443 10000

USER nifi

WORKDIR ${NIFI_HOME}

# Startup NiFi
ENTRYPOINT ["bin/nifi.sh"]
CMD ["run"]
