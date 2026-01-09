#!/bin/bash

# Generated configuration file from template using envsubst
# envsubst does not support default values in the template (e.g. ${VAR:-default}).
# We must set the defaults here in the shell before substitution.
export GRPC_CHANNELS=${GRPC_CHANNELS:-4}
export MAX_COMMIT_DELAY=${MAX_COMMIT_DELAY:-0}

# Users should ensure SPANNER_PROJECT, SPANNER_INSTANCE, SPANNER_DATABASE are set.
envsubst < /app/spanner-cassandra-config.yaml > /app/generated-config.yaml

# Start Java Spanner Cassandra Proxy and store PID
# We use the generated config file.
java -DconfigFilePath=/app/generated-config.yaml -jar /app/spanner-cassandra-proxy.jar &
cass_pid=$!

sleep 5

# Start ZDM Proxy
/app/zdm-proxy --config="$ZDM_CONFIG" &
zdm_pid=$!

# Monitor specific PIDs, exit if either of the processes die.
while true; do
    if ! kill -0 $cass_pid 2>/dev/null; then
        echo "java-spanner-cassandra-proxy (PID $cass_pid) died, shutting down container"
        exit 1
    fi
    if ! kill -0 $zdm_pid 2>/dev/null; then
        echo "zdm-proxy (PID $zdm_pid) died, shutting down container"
        exit 1
    fi
    sleep 1
done