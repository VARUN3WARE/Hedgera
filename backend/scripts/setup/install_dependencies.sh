#!/bin/bash
# Install dependencies using UV

echo "Installing Python dependencies..."
pip install uv
uv sync

echo "Dependencies installed successfully!"
