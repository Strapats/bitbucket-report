#!/bin/bash

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to create template .env file
create_env_template() {
    if [ ! -f .env ]; then
        echo "Creating template .env file..."
        cat > .env << EOL
# Bitbucket Configuration
BITBUCKET_WORKSPACE=your-workspace-here
BITBUCKET_USERNAME=your-username-here
BITBUCKET_APP_PASSWORD=your-app-password-here
EOL
        echo "Please edit the .env file with your Bitbucket credentials"
        echo "You can find instructions for creating an app password at:"
        echo "https://support.atlassian.com/bitbucket-cloud/docs/app-passwords/"
        return 1
    fi
    return 0
}

# Function to check .env values
check_env_values() {
    source .env
    local missing=0
    
    if [ "$BITBUCKET_WORKSPACE" = "your-workspace-here" ] || [ -z "$BITBUCKET_WORKSPACE" ]; then
        echo "ERROR: Please set BITBUCKET_WORKSPACE in .env file"
        missing=1
    fi
    
    if [ "$BITBUCKET_USERNAME" = "your-username-here" ] || [ -z "$BITBUCKET_USERNAME" ]; then
        echo "ERROR: Please set BITBUCKET_USERNAME in .env file"
        missing=1
    fi
    
    if [ "$BITBUCKET_APP_PASSWORD" = "your-app-password-here" ] || [ -z "$BITBUCKET_APP_PASSWORD" ]; then
        echo "ERROR: Please set BITBUCKET_APP_PASSWORD in .env file"
        missing=1
    fi
    
    return $missing
}

# Check for Python 3
if ! command_exists python3; then
    echo "Python 3 is required but not installed. Please install Python 3 and try again."
    exit 1
fi

# Check for pip3
if ! command_exists pip3; then
    echo "pip3 is required but not installed. Please install pip3 and try again."
    exit 1
fi

# Check for Homebrew on macOS
if [[ "$OSTYPE" == "darwin"* ]]; then
    if ! command_exists brew; then
        echo "Installing Homebrew..."
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    fi
    
    # Install required system dependencies
    echo "Installing system dependencies..."
    brew install pango libffi cairo gobject-introspection
fi

# Create and activate virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Create and check .env file
create_env_template
if [ $? -eq 1 ]; then
    echo "Please edit the .env file with your credentials and run this script again."
    exit 1
fi

# Check .env values
check_env_values
if [ $? -eq 1 ]; then
    echo "Please update the .env file with proper values and run this script again."
    exit 1
fi

# Run the application
echo "Starting the application..."
python main.py
