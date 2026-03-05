#!/bin/bash

echo "Stopping existing frontend services on port 3008..."
TARGET_PID=$(lsof -t -i :3008)
if [ ! -z "$TARGET_PID" ]; then
    kill -9 $TARGET_PID
    echo "Killed process $TARGET_PID."
else
    echo "No process is using port 3008."
fi

echo "Starting frontend service on port 3008..."
cd frontend && npm run dev -- --port 3008
