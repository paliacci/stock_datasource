#!/bin/bash

echo "Stopping existing backend services on port 8008..."
TARGET_PID=$(lsof -t -i :8008)
if [ ! -z "$TARGET_PID" ]; then
    kill -9 $TARGET_PID
    echo "Killed process $TARGET_PID."
else
    echo "No process is using port 8008."
fi

echo "Starting backend service..."
source $HOME/.local/bin/env
uv run python -m stock_datasource.services.http_server
