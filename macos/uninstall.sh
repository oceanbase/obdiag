#!/bin/bash
# OceanBase Diagnostic Tool - macOS Uninstallation Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
INSTALL_PATH="/usr/local/oceanbase-diagnostic-tool"
HOMEBREW_INSTALL_PATH="/usr/local/Cellar/obdiag"
HOMEBREW_OPT_PATH="/usr/local/opt/obdiag"

# Determine current user
if [ "$SUDO_USER" ]; then
    CURRENT_USER="$SUDO_USER"
    USER_HOME=$(eval echo ~$SUDO_USER)
else
    CURRENT_USER=$(whoami)
    USER_HOME="$HOME"
fi

OBDIAG_HOME="${USER_HOME}/.obdiag"

echo -e "${YELLOW}========================================"
echo "OceanBase Diagnostic Tool - Uninstaller"
echo -e "========================================${NC}"
echo ""

# Detect installation method
detect_install_method() {
    if [ -d "$HOMEBREW_INSTALL_PATH" ] || [ -L "$HOMEBREW_OPT_PATH" ]; then
        echo "homebrew"
    elif [ -d "$INSTALL_PATH" ]; then
        echo "manual"
    else
        echo "none"
    fi
}

# Uninstall via Homebrew
uninstall_homebrew() {
    echo -e "${YELLOW}Uninstalling via Homebrew...${NC}"
    
    if command -v brew &> /dev/null; then
        brew uninstall obdiag 2>/dev/null || true
        brew untap oceanbase/tap 2>/dev/null || true
        echo -e "${GREEN}Homebrew package removed${NC}"
    else
        echo -e "${RED}Homebrew not found, cleaning up manually...${NC}"
        rm -rf "$HOMEBREW_INSTALL_PATH"
        rm -f "$HOMEBREW_OPT_PATH"
        rm -f /usr/local/bin/obdiag
    fi
}

# Uninstall manual installation
uninstall_manual() {
    echo -e "${YELLOW}Removing manual installation...${NC}"
    
    # Remove installation directory
    if [ -d "$INSTALL_PATH" ]; then
        sudo rm -rf "$INSTALL_PATH"
        echo "Removed $INSTALL_PATH"
    fi
    
    # Remove symbolic link
    if [ -L /usr/local/bin/obdiag ]; then
        sudo rm -f /usr/local/bin/obdiag
        echo "Removed /usr/local/bin/obdiag"
    fi
}

# Remove shell completions
remove_completions() {
    echo -e "${YELLOW}Removing shell completions...${NC}"
    
    # Zsh completion
    local zsh_completion="/usr/local/share/zsh/site-functions/_obdiag"
    if [ -f "$zsh_completion" ]; then
        sudo rm -f "$zsh_completion" 2>/dev/null || rm -f "$zsh_completion"
        echo "Removed zsh completion"
    fi
    
    # Bash completion
    local bash_completion="/usr/local/etc/bash_completion.d/obdiag"
    if [ -f "$bash_completion" ]; then
        sudo rm -f "$bash_completion" 2>/dev/null || rm -f "$bash_completion"
        echo "Removed bash completion"
    fi
}

# Remove user data (optional)
remove_user_data() {
    echo ""
    echo -e "${YELLOW}User data directory: $OBDIAG_HOME${NC}"
    read -p "Do you want to remove user data (~/.obdiag)? [y/N]: " remove_data
    
    if [[ "$remove_data" =~ ^[Yy]$ ]]; then
        if [ -d "$OBDIAG_HOME" ]; then
            # Backup before removal
            backup_file="${USER_HOME}/obdiag_backup_$(date +%Y%m%d_%H%M%S).tar.gz"
            echo -e "${YELLOW}Creating backup at: $backup_file${NC}"
            tar -czf "$backup_file" -C "$USER_HOME" .obdiag 2>/dev/null || true
            
            rm -rf "$OBDIAG_HOME"
            echo -e "${GREEN}User data removed (backup saved)${NC}"
        fi
    else
        echo "User data preserved at $OBDIAG_HOME"
    fi
}

# Main uninstall process
main() {
    install_method=$(detect_install_method)
    
    case "$install_method" in
        "homebrew")
            echo "Detected: Homebrew installation"
            uninstall_homebrew
            ;;
        "manual")
            echo "Detected: Manual installation"
            uninstall_manual
            ;;
        "none")
            echo -e "${YELLOW}No installation detected${NC}"
            ;;
    esac
    
    # Always try to clean up completions
    remove_completions
    
    # Ask about user data
    remove_user_data
    
    echo ""
    echo -e "${GREEN}========================================"
    echo "Uninstallation completed!"
    echo -e "========================================${NC}"
    
    # Verify removal
    if command -v obdiag &> /dev/null; then
        echo -e "${YELLOW}Warning: 'obdiag' command still found in PATH${NC}"
        echo "Location: $(which obdiag)"
        echo "You may need to restart your terminal or remove it manually"
    else
        echo -e "${GREEN}obdiag has been completely removed${NC}"
    fi
}

# Check for --force flag
FORCE=false
if [[ "$1" == "--force" ]] || [[ "$1" == "-f" ]]; then
    FORCE=true
fi

if [ "$FORCE" = false ]; then
    echo "This will uninstall OceanBase Diagnostic Tool from your system."
    read -p "Are you sure you want to continue? [y/N]: " confirm
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        echo "Uninstallation cancelled."
        exit 0
    fi
fi

main
