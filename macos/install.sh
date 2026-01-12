#!/bin/bash
# OceanBase Diagnostic Tool - macOS Installation Script
# This script initializes obdiag after installation

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
INSTALL_PATH="${OBDIAG_INSTALL_PATH:-/usr/local/oceanbase-diagnostic-tool}"
OBDIAG_HOME="${OBDIAG_HOME:-$HOME/.obdiag}"

echo -e "${GREEN}========================================"
echo "OceanBase Diagnostic Tool - Post Install"
echo -e "========================================${NC}"

# Determine current user (handle sudo case)
if [ "$SUDO_USER" ]; then
    CURRENT_USER="$SUDO_USER"
    USER_HOME=$(eval echo ~$SUDO_USER)
else
    CURRENT_USER=$(whoami)
    USER_HOME="$HOME"
fi

OBDIAG_HOME="${USER_HOME}/.obdiag"

echo "Installing for user: $CURRENT_USER"
echo "OBDIAG_HOME: $OBDIAG_HOME"

# Create obdiag home directory
echo -e "${YELLOW}Creating obdiag home directory...${NC}"
mkdir -p "$OBDIAG_HOME"
mkdir -p "$OBDIAG_HOME/check"
mkdir -p "$OBDIAG_HOME/gather"
mkdir -p "$OBDIAG_HOME/display"
mkdir -p "$OBDIAG_HOME/rca"
mkdir -p "$OBDIAG_HOME/log"

# Copy plugins and configuration
if [ -d "$INSTALL_PATH/plugins" ]; then
    echo -e "${YELLOW}Copying plugins...${NC}"
    cp -rf "$INSTALL_PATH/plugins/"* "$OBDIAG_HOME/"
fi

if [ -f "$INSTALL_PATH/conf/ai.yml.example" ]; then
    echo -e "${YELLOW}Copying configuration examples...${NC}"
    cp -f "$INSTALL_PATH/conf/ai.yml.example" "$OBDIAG_HOME/"
fi

if [ -d "$INSTALL_PATH/example" ]; then
    echo -e "${YELLOW}Copying example configurations...${NC}"
    cp -rf "$INSTALL_PATH/example" "$OBDIAG_HOME/"
fi

# Set permissions
chown -R "$CURRENT_USER" "$OBDIAG_HOME"
chmod -R 755 "$OBDIAG_HOME"

# Create symbolic link (if running with sudo)
if [ -w /usr/local/bin ]; then
    echo -e "${YELLOW}Creating symbolic link...${NC}"
    ln -sf "$INSTALL_PATH/obdiag" /usr/local/bin/obdiag 2>/dev/null || true
fi

# Setup shell completion
setup_zsh_completion() {
    local completion_dir="/usr/local/share/zsh/site-functions"
    if [ -w "$completion_dir" ] || [ "$SUDO_USER" ]; then
        mkdir -p "$completion_dir"
        cat > "$completion_dir/_obdiag" << 'COMPLETION_EOF'
#compdef obdiag

_obdiag() {
    local -a commands subcommands

    _arguments -C \
        '1: :->command' \
        '*:: :->args'

    case $state in
        command)
            commands=(
                '--version:Show version information'
                'config:Configure obdiag settings'
                'gather:Gather diagnostic information'
                'display:Display cluster information'
                'analyze:Analyze logs and data'
                'check:Run diagnostic checks'
                'rca:Root cause analysis'
                'update:Update obdiag plugins'
                'tool:Utility tools'
            )
            _describe 'command' commands
            ;;
        args)
            case $words[1] in
                gather)
                    subcommands=(
                        'log:Gather observer logs'
                        'clog:Gather clog files'
                        'slog:Gather slog files'
                        'plan_monitor:Gather plan monitor data'
                        'stack:Gather stack traces'
                        'perf:Gather performance data'
                        'sysstat:Gather system statistics'
                        'obproxy_log:Gather obproxy logs'
                        'all:Gather all information'
                        'scene:Gather by scene'
                        'ash:Gather ASH data'
                        'tabledump:Dump table data'
                        'parameter:Gather parameters'
                        'variable:Gather variables'
                    )
                    _describe 'gather command' subcommands
                    ;;
                check)
                    subcommands=(
                        'run:Run diagnostic checks'
                        'list:List available checks'
                    )
                    _describe 'check command' subcommands
                    ;;
                analyze)
                    subcommands=(
                        'log:Analyze logs'
                        'flt_trace:Analyze full link trace'
                        'parameter:Analyze parameters'
                        'variable:Analyze variables'
                        'index_space:Analyze index space'
                        'queue:Analyze queues'
                        'memory:Analyze memory'
                    )
                    _describe 'analyze command' subcommands
                    ;;
                rca)
                    subcommands=(
                        'run:Run root cause analysis'
                        'list:List RCA scenes'
                    )
                    _describe 'rca command' subcommands
                    ;;
                display)
                    subcommands=(
                        'scene:Display scene information'
                    )
                    _describe 'display command' subcommands
                    ;;
                tool)
                    subcommands=(
                        'crypto_config:Encrypt configuration'
                        'ai_assistant:AI diagnostic assistant'
                        'io_performance:IO performance test'
                        'config_check:Check configuration'
                    )
                    _describe 'tool command' subcommands
                    ;;
            esac
            ;;
    esac
}

_obdiag "$@"
COMPLETION_EOF
        echo -e "${GREEN}Zsh completion installed${NC}"
    fi
}

setup_bash_completion() {
    local completion_dir="/usr/local/etc/bash_completion.d"
    if [ -w "$completion_dir" ] || [ "$SUDO_USER" ]; then
        mkdir -p "$completion_dir"
        cat > "$completion_dir/obdiag" << 'COMPLETION_EOF'
_obdiag_completion() {
    local cur_word args type_list
    cur_word="${COMP_WORDS[COMP_CWORD]}"

    case "${COMP_CWORD}" in
        1)
            type_list="--version config gather display analyze check rca update tool"
            COMPREPLY=($(compgen -W "${type_list}" -- "${cur_word}"))
            ;;
        2)
            case "${COMP_WORDS[1]}" in
                check)
                    type_list="run list"
                    ;;
                gather)
                    type_list="log clog slog plan_monitor stack perf sysstat obproxy_log all scene ash tabledump parameter variable"
                    ;;
                analyze)
                    type_list="log flt_trace parameter variable index_space queue memory"
                    ;;
                rca)
                    type_list="run list"
                    ;;
                display)
                    type_list="scene"
                    ;;
                tool)
                    type_list="crypto_config ai_assistant io_performance config_check"
                    ;;
            esac
            COMPREPLY=($(compgen -W "${type_list}" -- "${cur_word}"))
            ;;
    esac
}

complete -F _obdiag_completion obdiag
COMPLETION_EOF
        echo -e "${GREEN}Bash completion installed${NC}"
    fi
}

echo -e "${YELLOW}Setting up shell completions...${NC}"
setup_zsh_completion
setup_bash_completion

# Write version info
if command -v obdiag &> /dev/null; then
    version_line=$(obdiag --version 2>&1 | grep -oE 'OceanBase Diagnostic Tool: [0-9.]+' | grep -oE '[0-9.]+' || echo "unknown")
    echo "obdiag_version: \"$version_line\"" > "$OBDIAG_HOME/version.yaml"
fi

echo ""
echo -e "${GREEN}========================================"
echo "Installation completed successfully!"
echo "========================================"
echo ""
echo "Next steps:"
echo "  1. Configure your cluster:"
echo "     obdiag config"
echo ""
echo "  2. Run a diagnostic check:"
echo "     obdiag check run"
echo ""
echo "  3. View help:"
echo "     obdiag --help"
echo -e "========================================${NC}"
