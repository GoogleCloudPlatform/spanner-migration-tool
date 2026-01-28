# --- Go Builder Stage ---
FROM golang:1.24-bullseye AS go-builder

# Disable cgo to remove gcc dependency
ENV CGO_ENABLED=0

RUN apt-get update && apt-get install -y git

WORKDIR /app

# Clone the ZDM Proxy repo
RUN git clone https://github.com/datastax/zdm-proxy.git

WORKDIR /app/zdm-proxy/proxy

# Build ZDM Proxy
RUN go build -o zdm-proxy

# --- Java Builder Stage ---
FROM maven:3.9-eclipse-temurin-17 AS java-builder

WORKDIR /app

# Clone the Java Spanner Cassandra repo
# Using specific tag or main branch. For stability, usually better to pin, but instructions enable main.
RUN git clone -b v1.1.0 https://github.com/googleapis/java-spanner-cassandra.git

WORKDIR /app/java-spanner-cassandra

# Build the project to get the shaded jar
RUN mvn clean install -DskipTests

# --- Final Image ---
FROM alpine:3.22

# Install bash, ca-certificates, openssl, gettext (for envsubst), and Java 17 JRE
RUN apk update && \
    apk upgrade && \
    apk add --no-cache bash ca-certificates openssl gettext openjdk17-jre

EXPOSE 9042 14002

WORKDIR /app

# Copy ZDM Proxy binary
COPY --from=go-builder /app/zdm-proxy/proxy/zdm-proxy .

# Copy Java Spanner Cassandra Proxy JAR and dependencies
# The build produces spanner-cassandra-launcher.jar and a lib/ directory with dependencies.
COPY --from=java-builder /app/java-spanner-cassandra/google-cloud-spanner-cassandra/target/spanner-cassandra-launcher.jar spanner-cassandra-proxy.jar
COPY --from=java-builder /app/java-spanner-cassandra/google-cloud-spanner-cassandra/target/lib ./lib

# Copy configuration template
COPY spanner-cassandra-config.yaml .

# Copy entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]