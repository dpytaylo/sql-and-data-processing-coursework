#!/bin/bash

# Check if the virtual environment already exists
if [ ! -d "venv" ]; then
  # Create a virtual environment
  python -m venv venv
fi

# Activate the virtual environment
source ./venv/bin/activate

# Install the required packages
pip install -r requirements.txt

# Run the Python script
python ./main.py

# Deactivate the virtual environment
deactivate
