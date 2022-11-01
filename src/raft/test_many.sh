#!/usr/bin/env bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 numTrials"
    exit 1
fi

trap 'kill -INT -$pid; exit 1' INT

runs=$1
# chmod +x test-mr.sh

for i in $(seq 1 $runs); do
    go test -race -run 2A &
    pid=$!
    if ! wait $pid; then
        echo '*' FAILED TESTS IN TRIAL $i
        exit 1
    fi
done
echo '*' PASSED ALL $i TESTING TRIALS
