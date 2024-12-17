#!/bin/bash

# Exit on error
set -e

echo "Setting up Bitbucket Statistics project..."

# Check if Homebrew is installed
if ! command -v brew &> /dev/null; then
    echo "Homebrew is not installed. Please install it first:"
    echo '/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"'
    exit 1
fi

# Install system dependencies
echo "Installing system dependencies..."
brew install pango libffi cairo gobject-introspection || true

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install requirements
echo "Installing dependencies..."
pip install -r requirements.txt

# Ensure .env file exists
if [ ! -f ".env" ]; then
    echo "Warning: .env file not found. Please make sure to set up your environment variables."
    exit 1
fi

# Run the main script
echo "Starting the application..."
python main.py

# Deactivate virtual environment
deactivate
