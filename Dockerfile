ARG BASE_DIR=/source
ARG NIFI_VERSION=1.4.0

# Build the nar file

FROM maven:3.5 as builder

COPY . ${BASE_DIR}

WORKDIR ${BASE_DIR}

RUN mvn clean install


# ----------------
# Build a new Nifi image with the newly generated nar file included

FROM apache/nifi:${NIFI_VERSION}

ENV NIFI_BASE_DIR /opt/nifi
ENV NIFI_HOME=${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}

COPY --from=builder ${BASE_DIR}/nifi-opcua-nar/target/*.nar ${NIFI_HOME}/lib/nifi-opcua.nar

EXPOSE 8080 8443 10000

USER nifi

WORKDIR ${NIFI_HOME}

# Startup NiFi
ENTRYPOINT ["bin/nifi.sh"]
CMD ["run"]
