#!/bin/bash
# Cleanup logs

echo "Cleaning up old logs..."

find logs/ -name "*.log" -mtime +30 -delete

echo "Log cleanup complete!"
