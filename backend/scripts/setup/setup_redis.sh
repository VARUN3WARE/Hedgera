#!/bin/bash
# Setup Redis

echo "Setting up Redis..."

# Check if Redis is installed
if ! command -v redis-server &> /dev/null; then
    echo "Redis not found. Installing..."
    brew install redis
fi

# Start Redis
echo "Starting Redis..."
redis-server --daemonize yes

echo "Redis setup complete!"
