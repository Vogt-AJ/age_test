#!/bin/bash
# install_agload.sh
# Script to install Apache AGE's AGLoad bulk loader

echo "========================================================================"
echo "AGLoad Installation Script"
echo "========================================================================"
echo ""

# Check if age directory exists
if [ ! -d "$HOME/age" ]; then
    echo "Error: AGE source directory not found at $HOME/age"
    echo ""
    echo "Please clone AGE first:"
    echo "  cd ~"
    echo "  git clone https://github.com/apache/age.git"
    echo "  cd age"
    echo "  git checkout release/1.6.0"
    echo ""
    exit 1
fi

echo "Found AGE source at $HOME/age"
echo ""

# Navigate to tools directory
cd "$HOME/age/tools"

if [ ! -d "$HOME/age/tools" ]; then
    echo "Error: AGE tools directory not found"
    exit 1
fi

echo "Installing dependencies..."
sudo apt-get update
sudo apt-get install -y build-essential libpq-dev postgresql-server-dev-17

echo ""
echo "Building AGLoad..."
make

if [ $? -ne 0 ]; then
    echo ""
    echo "Build failed. Trying alternative method..."
    gcc -o age_load age_load.c -lpq -I/usr/include/postgresql
    
    if [ $? -ne 0 ]; then
        echo "Alternative build also failed."
        echo "Please check the error messages above."
        exit 1
    fi
fi

echo ""
echo "Installing AGLoad..."
sudo make install

if [ $? -ne 0 ]; then
    echo ""
    echo "Install failed. Trying manual install..."
    sudo cp age_load /usr/local/bin/
    sudo chmod +x /usr/local/bin/age_load
fi

echo ""
echo "Verifying installation..."
which age_load

if [ $? -eq 0 ]; then
    echo ""
    echo "========================================================================"
    echo "✓ AGLoad installed successfully!"
    echo "========================================================================"
    echo ""
    echo "You can now use AGLoad with:"
    echo "  python main_agload.py"
    echo ""
    echo "To verify:"
    echo "  age_load --help"
    echo ""
else
    echo ""
    echo "========================================================================"
    echo "✗ AGLoad installation incomplete"
    echo "========================================================================"
    echo ""
    echo "The binary was built but not found in PATH."
    echo "You can use it directly from: $HOME/age/tools/age_load"
    echo ""
    echo "Or add it to your PATH:"
    echo "  export PATH=\$PATH:$HOME/age/tools"
    echo ""
fi
