FROM golang:1.24-bullseye AS builder

# Disable cgo to remove gcc dependency
ENV CGO_ENABLED=0

RUN apt-get update && apt-get install -y git curl

WORKDIR /app

# Clone the ZDM Proxy repo
RUN git clone https://github.com/datastax/zdm-proxy.git

WORKDIR /app/zdm-proxy/proxy

# Build ZDM Proxy
RUN go build -o zdm-proxy

# --- Cassandra-to-Spanner CQL Proxy ---
WORKDIR /app

RUN git clone "https://github.com/googleapis/go-spanner-cassandra.git"

WORKDIR /app/go-spanner-cassandra

# Build CQL Proxy
RUN go build -o cassandra-spanner-proxy

# --- Final Image ---
FROM alpine:3.21

# Install bash, ca-certificates, openssl, and update packages
RUN apk update && \
    apk upgrade && \
    apk add --no-cache bash ca-certificates openssl

EXPOSE 9042 14002

# Copy in the binaries
COPY --from=builder /app/zdm-proxy/proxy/zdm-proxy .
COPY --from=builder /app/go-spanner-cassandra/cassandra-spanner-proxy .

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]