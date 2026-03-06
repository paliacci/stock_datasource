#!/bin/bash

echo "Stopping existing backend services on port 3389..."
TARGET_PID=$(lsof -t -i :3389)
if [ ! -z "$TARGET_PID" ]; then
    kill -9 $TARGET_PID
    echo "Killed process $TARGET_PID."
else
    echo "No process is using port 3389."
fi

echo "Starting backend service..."
if [ -f "$HOME/.local/bin/env" ]; then
    source "$HOME/.local/bin/env"
elif [ -f "$HOME/.cargo/env" ]; then
    source "$HOME/.cargo/env"
fi

uv run python -m stock_datasource.services.http_server
