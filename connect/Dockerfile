ARG DEBEZIUM_VERSION=3.4
FROM quay.io/debezium/connect:${DEBEZIUM_VERSION}

USER root
ARG ORACLE_DRIVER_VERSION=23.26.2.0.0
ARG ORACLE_PLUGIN_DIR=/kafka/connect/debezium-connector-oracle

RUN set -eux; \
    mkdir -p "${ORACLE_PLUGIN_DIR}"; \
    curl -fSL "https://repo.maven.apache.org/maven2/com/oracle/database/jdbc/ojdbc11/${ORACLE_DRIVER_VERSION}/ojdbc11-${ORACLE_DRIVER_VERSION}.jar" \
      -o "${ORACLE_PLUGIN_DIR}/ojdbc11.jar"; \
    curl -fSL "https://repo.maven.apache.org/maven2/com/oracle/database/xml/xdb/${ORACLE_DRIVER_VERSION}/xdb-${ORACLE_DRIVER_VERSION}.jar" \
      -o "${ORACLE_PLUGIN_DIR}/xdb.jar"; \
    curl -fSL "https://repo.maven.apache.org/maven2/com/oracle/database/xml/xmlparserv2/${ORACLE_DRIVER_VERSION}/xmlparserv2-${ORACLE_DRIVER_VERSION}.jar" \
      -o "${ORACLE_PLUGIN_DIR}/xmlparserv2.jar"; \
    chown -R kafka:kafka "${ORACLE_PLUGIN_DIR}"

USER kafka
