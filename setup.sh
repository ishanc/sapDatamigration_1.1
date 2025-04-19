#!/bin/bash

# Delete existing virtual environment if it exists
if [ -d "venv" ]; then
    echo "Removing existing virtual environment..."
    rm -rf venv
fi

# Create a new virtual environment
echo "Creating a new virtual environment..."
python3 -m venv venv

# Activate the virtual environment
echo "Activating the virtual environment..."
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip
pip install --upgrade pip setuptools
pip install -r requirements.txt

echo "Virtual environment setup complete. To activate, run: source venv/bin/activate"