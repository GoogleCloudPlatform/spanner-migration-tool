#!/bin/bash

# Start processes and store their PIDs
/cassandra-spanner-proxy --database-uri="projects/$SPANNER_PROJECT/instances/$SPANNER_INSTANCE/databases/$SPANNER_DATABASE" --num-grpc-channels=500 &
cass_pid=$!
sleep 5
/zdm-proxy --config="$ZDM_CONFIG" &
zdm_pid=$!

# Monitor specific PIDs, exit if either of the processes die.
while true; do
    if ! kill -0 $cass_pid 2>/dev/null; then
        echo "cassandra-to-spanner-proxy (PID $cass_pid) died, shutting down container"
        exit 1
    fi
    if ! kill -0 $zdm_pid 2>/dev/null; then
        echo "zdm-proxy (PID $zdm_pid) died, shutting down container"
        exit 1
    fi
    sleep 1
done