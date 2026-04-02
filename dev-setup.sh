#!/bin/bash

set -e  # Exit on error

VENV_DIR_DEFAULT=".venv"
if [ -d "myenv" ] && [ ! -d ".venv" ]; then
    VENV_DIR_DEFAULT="myenv"
fi
VENV_DIR="${VENV_DIR:-$VENV_DIR_DEFAULT}"

PYTHON_BIN="${PYTHON_BIN:-python3}"

echo "🚀 Setting up Akave Python SDK development environment..."
echo ""

echo "📍 Checking Python version..."
python_version=$($PYTHON_BIN --version 2>&1 | awk '{print $2}')
echo "✅ Found Python $python_version"
echo ""

if [ ! -d "$VENV_DIR" ]; then
    echo "📦 Creating virtual environment..."
    $PYTHON_BIN -m venv "$VENV_DIR"
    echo "✅ Virtual environment created"
else
    echo "✅ Virtual environment already exists"
fi
echo ""

echo "🔄 Activating virtual environment..."
if [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]]; then
    source "$VENV_DIR"/Scripts/activate
else
    source "$VENV_DIR"/bin/activate
fi
echo "✅ Virtual environment activated"
echo ""

echo "⬆️  Upgrading pip..."
pip install --upgrade pip --quiet
echo "✅ pip upgraded"
echo ""

echo "📦 Installing project dependencies..."
pip install -r requirements.txt --quiet
echo "✅ Project dependencies installed"
echo ""

echo "🔧 Installing development tools..."
pip install black isort flake8 pylint mypy bandit safety pip-audit --quiet
echo "✅ Development tools installed"
echo ""

echo "📁 Creating necessary directories..."
mkdir -p .pytest_cache
mkdir -p .mypy_cache
echo "✅ Directories created"
echo ""

echo "✨ Setup complete! Your development environment is ready."
echo ""
echo "To activate the virtual environment, run:"
echo "  source $VENV_DIR/bin/activate  (Linux/macOS)"
echo "  $VENV_DIR\\Scripts\\activate    (Windows)"
echo ""
echo "To run code quality checks, use:"
echo "  ./run-checks.sh (Linux/macOS)"
echo "  or run individual commands from CONTRIBUTING.md"
echo ""
echo "Happy coding! 🎉"

