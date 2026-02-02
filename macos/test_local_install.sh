#!/bin/bash
# OceanBase Diagnostic Tool - Local Installation Test Script
# This script simulates Homebrew installation for testing purposes

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Installation paths
# Use local directory for non-root testing, or Homebrew paths with sudo
VERSION="4.0.0"
if [ "$EUID" -eq 0 ] || [ -w "/usr/local/Cellar" ]; then
    # Root or writable Cellar - use Homebrew-style paths
    CELLAR_DIR="/usr/local/Cellar/obdiag/$VERSION"
    OPT_DIR="/usr/local/opt/obdiag"
    BIN_DIR="/usr/local/bin"
    INSTALL_MODE="homebrew"
else
    # Non-root - use local paths
    CELLAR_DIR="$HOME/.local/obdiag/$VERSION"
    OPT_DIR="$HOME/.local/opt/obdiag"
    BIN_DIR="$HOME/.local/bin"
    INSTALL_MODE="local"
    echo -e "${YELLOW}Note: Installing to local directory (no root access)${NC}"
    echo "Install path: $CELLAR_DIR"
fi

echo -e "${BLUE}========================================"
echo "OceanBase Diagnostic Tool"
echo "Local Installation Test (Homebrew-style)"
echo -e "========================================${NC}"
echo ""

# Check Python version
check_python() {
    echo -e "${YELLOW}Checking Python version...${NC}"
    
    if ! command -v python3 &> /dev/null; then
        echo -e "${RED}Error: Python 3 is required${NC}"
        echo "Please install Python 3.11+: brew install python@3.11"
        exit 1
    fi
    
    PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
    echo "Found Python $PYTHON_VERSION"
    
    MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
    MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)
    
    # For testing, allow Python 3.8+, but warn if < 3.11
    if [ "$MAJOR" -lt 3 ] || ([ "$MAJOR" -eq 3 ] && [ "$MINOR" -lt 8 ]); then
        echo -e "${RED}Error: Python 3.8+ is required (found $PYTHON_VERSION)${NC}"
        exit 1
    fi
    
    if [ "$MAJOR" -eq 3 ] && [ "$MINOR" -lt 11 ]; then
        echo -e "${YELLOW}Warning: Python 3.11+ is recommended for production (found $PYTHON_VERSION)${NC}"
        echo "Some features may not work properly with older Python versions."
    fi
    
    echo -e "${GREEN}Python version check passed${NC}"
}

# Install dependencies
install_dependencies() {
    echo ""
    echo -e "${YELLOW}Installing Python dependencies...${NC}"
    
    cd "$PROJECT_DIR"
    
    # Check if project has existing venv with Python 3.11+
    if [ -d "$PROJECT_DIR/.venv" ] && [ -f "$PROJECT_DIR/.venv/bin/python" ]; then
        VENV_PYTHON=$("$PROJECT_DIR/.venv/bin/python" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
        VENV_MAJOR=$(echo $VENV_PYTHON | cut -d. -f1)
        VENV_MINOR=$(echo $VENV_PYTHON | cut -d. -f2)
        
        if [ "$VENV_MAJOR" -eq 3 ] && [ "$VENV_MINOR" -ge 11 ]; then
            echo -e "${GREEN}Using existing project venv (Python $VENV_PYTHON)${NC}"
            USE_PROJECT_VENV=true
        fi
    fi
    
    VENV_DIR="$CELLAR_DIR/libexec"
    
    if [ "$USE_PROJECT_VENV" = true ]; then
        # Link to existing venv
        mkdir -p "$(dirname "$VENV_DIR")"
        ln -sf "$PROJECT_DIR/.venv" "$VENV_DIR"
        echo "Linked to project venv: $PROJECT_DIR/.venv"
    else
        # Create new virtual environment
        python3 -m venv "$VENV_DIR"
        
        # Activate and install
        source "$VENV_DIR/bin/activate"
        pip install --upgrade pip wheel
        
        # Install from pyproject.toml: prefer [macos] extra (pyzipper); fallback to base then add pyzipper
        pip install -e ".[macos]" || pip install -e .
        pip uninstall -y pyminizip 2>/dev/null || true
        pip install "pyzipper==0.3.6"
        
        deactivate
    fi
    
    echo -e "${GREEN}Dependencies installed${NC}"
}

# Copy files
copy_files() {
    echo ""
    echo -e "${YELLOW}Copying files to Cellar...${NC}"
    
    # Create directories
    mkdir -p "$CELLAR_DIR"/{src,plugins,conf,example,resources,bin}
    
    # Copy source files
    cp -r "$PROJECT_DIR/src/"* "$CELLAR_DIR/src/"
    cp -r "$PROJECT_DIR/plugins/"* "$CELLAR_DIR/plugins/"
    cp -r "$PROJECT_DIR/conf/"* "$CELLAR_DIR/conf/"
    cp -r "$PROJECT_DIR/example/"* "$CELLAR_DIR/example/"
    cp -r "$PROJECT_DIR/resources/"* "$CELLAR_DIR/resources/" 2>/dev/null || true
    
    # Copy installation scripts
    cp "$PROJECT_DIR/macos/install.sh" "$CELLAR_DIR/"
    cp "$PROJECT_DIR/macos/uninstall.sh" "$CELLAR_DIR/"
    
    echo -e "${GREEN}Files copied${NC}"
}

# Create wrapper script
create_wrapper() {
    echo ""
    echo -e "${YELLOW}Creating wrapper script...${NC}"
    
    cat > "$CELLAR_DIR/bin/obdiag" << EOF
#!/bin/bash
# OceanBase Diagnostic Tool wrapper
export PYTHONPATH="$CELLAR_DIR/src:$CELLAR_DIR:\$PYTHONPATH"
export OBDIAG_INSTALL_PATH="$CELLAR_DIR"
exec "$CELLAR_DIR/libexec/bin/python3" "$CELLAR_DIR/src/main.py" "\$@"
EOF
    
    chmod +x "$CELLAR_DIR/bin/obdiag"
    
    echo -e "${GREEN}Wrapper script created${NC}"
}

# Create symlinks
create_symlinks() {
    echo ""
    echo -e "${YELLOW}Creating symlinks...${NC}"
    
    # Create opt link
    mkdir -p "$(dirname "$OPT_DIR")"
    rm -f "$OPT_DIR"
    ln -sf "$CELLAR_DIR" "$OPT_DIR"
    
    # Create bin link
    mkdir -p "$BIN_DIR"
    rm -f "$BIN_DIR/obdiag"
    ln -sf "$CELLAR_DIR/bin/obdiag" "$BIN_DIR/obdiag"
    
    echo -e "${GREEN}Symlinks created${NC}"
}

# Setup user directory
setup_user_dir() {
    echo ""
    echo -e "${YELLOW}Setting up user directory...${NC}"
    
    OBDIAG_HOME="$HOME/.obdiag"
    
    mkdir -p "$OBDIAG_HOME"/{check,gather,display,rca,log}
    
    # Copy plugins
    cp -r "$CELLAR_DIR/plugins/"* "$OBDIAG_HOME/"
    
    # Copy examples
    cp -r "$CELLAR_DIR/example" "$OBDIAG_HOME/"
    
    # Copy AI config example
    [ -f "$CELLAR_DIR/conf/ai.yml.example" ] && cp "$CELLAR_DIR/conf/ai.yml.example" "$OBDIAG_HOME/"
    
    echo -e "${GREEN}User directory setup complete${NC}"
}

# Install shell completions
install_completions() {
    echo ""
    echo -e "${YELLOW}Installing shell completions...${NC}"
    
    if [ "$INSTALL_MODE" = "local" ]; then
        # Local installation - put completions in user directory
        ZSH_COMP_DIR="$HOME/.zsh/completions"
        BASH_COMP_DIR="$HOME/.bash_completion.d"
    else
        ZSH_COMP_DIR="/usr/local/share/zsh/site-functions"
        BASH_COMP_DIR="/usr/local/etc/bash_completion.d"
    fi
    
    # Zsh completion
    if mkdir -p "$ZSH_COMP_DIR" 2>/dev/null; then
        cp "$PROJECT_DIR/macos/completions/_obdiag" "$ZSH_COMP_DIR/" 2>/dev/null && \
            echo "  Zsh completion installed to $ZSH_COMP_DIR" || \
            echo "  Zsh completion skipped (copy failed)"
    else
        echo "  Zsh completion skipped (no permission)"
    fi
    
    # Bash completion
    if mkdir -p "$BASH_COMP_DIR" 2>/dev/null; then
        cp "$PROJECT_DIR/macos/completions/obdiag.bash" "$BASH_COMP_DIR/obdiag" 2>/dev/null && \
            echo "  Bash completion installed to $BASH_COMP_DIR" || \
            echo "  Bash completion skipped (copy failed)"
    else
        echo "  Bash completion skipped (no permission)"
    fi
    
    # For local install, show how to enable completions
    if [ "$INSTALL_MODE" = "local" ]; then
        echo ""
        echo "To enable completions, add to your shell config:"
        echo "  Zsh:  fpath=($ZSH_COMP_DIR \$fpath); autoload -Uz compinit && compinit"
        echo "  Bash: source $BASH_COMP_DIR/obdiag"
    fi
    
    echo -e "${GREEN}Completions setup complete${NC}"
}

# Verify installation
verify_installation() {
    echo ""
    echo -e "${YELLOW}Verifying installation...${NC}"
    
    # For local install, add BIN_DIR to PATH temporarily
    export PATH="$BIN_DIR:$PATH"
    
    if [ -x "$BIN_DIR/obdiag" ]; then
        echo ""
        echo -e "${GREEN}Testing 'obdiag --version':${NC}"
        "$BIN_DIR/obdiag" --version 2>&1 || true
        
        echo ""
        echo -e "${GREEN}Testing 'obdiag --help':${NC}"
        "$BIN_DIR/obdiag" --help 2>&1 | head -20 || true
        
        echo ""
        echo -e "${GREEN}Testing 'obdiag check list':${NC}"
        "$BIN_DIR/obdiag" check list 2>&1 | head -15 || true
        
        echo ""
        echo -e "${GREEN}✅ Installation verified successfully!${NC}"
    else
        echo -e "${RED}❌ Installation failed - obdiag command not found at $BIN_DIR/obdiag${NC}"
        exit 1
    fi
}

# Print summary
print_summary() {
    echo ""
    echo -e "${BLUE}========================================"
    echo "Installation Summary"
    echo -e "========================================${NC}"
    echo ""
    echo "Installation mode: $INSTALL_MODE"
    echo ""
    echo "Installed to:"
    echo "  Main:   $CELLAR_DIR"
    echo "  Opt:    $OPT_DIR"
    echo "  Binary: $BIN_DIR/obdiag"
    echo "  User:   $HOME/.obdiag"
    echo ""
    
    if [ "$INSTALL_MODE" = "local" ]; then
        echo -e "${YELLOW}Important: Add to your PATH:${NC}"
        echo "  export PATH=\"$BIN_DIR:\$PATH\""
        echo ""
        echo "Add this line to your ~/.zshrc or ~/.bashrc"
        echo ""
    fi
    
    echo "To uninstall, run:"
    echo "  $0 uninstall"
    echo ""
    echo "Or use the quick uninstall command:"
    echo "  rm -rf $CELLAR_DIR $OPT_DIR $BIN_DIR/obdiag"
    echo ""
}

# Uninstall function
uninstall() {
    echo -e "${YELLOW}Uninstalling obdiag...${NC}"
    
    # Remove Cellar
    [ -d "$CELLAR_DIR" ] && rm -rf "$CELLAR_DIR" && echo "Removed $CELLAR_DIR"
    
    # Remove opt link
    [ -L "$OPT_DIR" ] && rm -f "$OPT_DIR" && echo "Removed $OPT_DIR"
    
    # Remove bin link
    [ -L "$BIN_DIR/obdiag" ] && rm -f "$BIN_DIR/obdiag" && echo "Removed $BIN_DIR/obdiag"
    
    # Remove completions
    [ -f "/usr/local/share/zsh/site-functions/_obdiag" ] && rm -f "/usr/local/share/zsh/site-functions/_obdiag"
    [ -f "/usr/local/etc/bash_completion.d/obdiag" ] && rm -f "/usr/local/etc/bash_completion.d/obdiag"
    
    echo ""
    read -p "Remove user data (~/.obdiag)? [y/N]: " remove_data
    if [[ "$remove_data" =~ ^[Yy]$ ]]; then
        rm -rf "$HOME/.obdiag"
        echo "User data removed"
    fi
    
    echo -e "${GREEN}Uninstall complete${NC}"
}

# Main
main() {
    case "${1:-install}" in
        install)
            check_python
            
            # Clean previous installation
            [ -d "$CELLAR_DIR" ] && rm -rf "$CELLAR_DIR"
            
            install_dependencies
            copy_files
            create_wrapper
            create_symlinks
            setup_user_dir
            install_completions
            verify_installation
            print_summary
            ;;
        uninstall)
            uninstall
            ;;
        verify)
            verify_installation
            ;;
        *)
            echo "Usage: $0 [install|uninstall|verify]"
            exit 1
            ;;
    esac
}

main "$@"
