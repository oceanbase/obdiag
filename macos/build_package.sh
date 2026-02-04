#!/bin/bash
# OceanBase Diagnostic Tool - macOS Package Builder
# This script builds a distributable package for macOS

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

# Version
VERSION="${OBDIAG_VERSION:-4.0.0}"
RELEASE="${RELEASE:-$(date +%Y%m%d%H)}"
ARCH=$(uname -m)  # arm64 or x86_64

# Build directories
BUILD_DIR="$PROJECT_DIR/build_macos"
DIST_DIR="$PROJECT_DIR/dist_macos"
PKG_NAME="obdiag-${VERSION}-${RELEASE}-macos-${ARCH}"
PKG_DIR="$DIST_DIR/$PKG_NAME"

echo -e "${BLUE}========================================"
echo "OceanBase Diagnostic Tool - macOS Builder"
echo "========================================"
echo "Version: $VERSION"
echo "Release: $RELEASE"
echo "Arch:    $ARCH"
echo -e "========================================${NC}"
echo ""

# Check Python version
check_python() {
    echo -e "${YELLOW}Checking Python version...${NC}"
    
    PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
    echo "Python version: $PYTHON_VERSION"
    
    MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
    MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)
    
    if [ "$MAJOR" -lt 3 ] || ([ "$MAJOR" -eq 3 ] && [ "$MINOR" -lt 11 ]); then
        echo -e "${RED}Error: Python 3.11+ is required for building${NC}"
        echo "Please activate a Python 3.11+ environment:"
        echo "  source .venv/bin/activate"
        exit 1
    fi
    
    echo -e "${GREEN}Python version OK${NC}"
}

# Check dependencies
check_dependencies() {
    echo ""
    echo -e "${YELLOW}Checking dependencies...${NC}"
    
    # Check PyInstaller
    if ! python3 -c "import PyInstaller" 2>/dev/null; then
        echo "Installing PyInstaller..."
        pip install pyinstaller
    fi
    
    # Check pyzipper (for macOS)
    if ! python3 -c "import pyzipper" 2>/dev/null; then
        echo "Installing pyzipper..."
        pip install pyzipper
    fi
    
    echo -e "${GREEN}Dependencies OK${NC}"
}

# Clean previous build
clean_build() {
    echo ""
    echo -e "${YELLOW}Cleaning previous build...${NC}"
    
    rm -rf "$BUILD_DIR"
    rm -rf "$DIST_DIR"
    rm -f "$PROJECT_DIR/src/obdiag.py"
    rm -rf "$PROJECT_DIR/build"
    rm -rf "$PROJECT_DIR/dist"
    rm -f "$PROJECT_DIR/obdiag.spec"
    
    echo -e "${GREEN}Clean complete${NC}"
}

# Update version info
update_version() {
    echo ""
    echo -e "${YELLOW}Updating version info...${NC}"
    
    # Backup original version.py
    cp "$PROJECT_DIR/src/common/version.py" "$PROJECT_DIR/src/common/version.py.bak"
    
    # Update version and build time
    BUILD_TIME=$(date "+%Y-%m-%d %H:%M:%S")
    sed -i.tmp "s/<B_TIME>/$BUILD_TIME/g" "$PROJECT_DIR/src/common/version.py"
    sed -i.tmp "s/<VERSION>/$VERSION/g" "$PROJECT_DIR/src/common/version.py"
    rm -f "$PROJECT_DIR/src/common/version.py.tmp"
    
    echo "Version: $VERSION"
    echo "Build Time: $BUILD_TIME"
    echo -e "${GREEN}Version updated${NC}"
}

# Restore version info
restore_version() {
    if [ -f "$PROJECT_DIR/src/common/version.py.bak" ]; then
        mv "$PROJECT_DIR/src/common/version.py.bak" "$PROJECT_DIR/src/common/version.py"
    fi
}

# Build with PyInstaller
build_binary() {
    echo ""
    echo -e "${YELLOW}Building binary with PyInstaller...${NC}"
    
    cd "$PROJECT_DIR"
    
    # Create entry point
    cp src/main.py src/obdiag.py
    
    # Build
    pyinstaller \
        --name obdiag \
        --onefile \
        --hidden-import=decimal \
        --hidden-import=pyzipper \
        --hidden-import=pymysql \
        --hidden-import=paramiko \
        --hidden-import=yaml \
        --hidden-import=jinja2 \
        --hidden-import=requests \
        --hidden-import=tabulate \
        --hidden-import=prettytable \
        --hidden-import=colorama \
        --paths="$PROJECT_DIR/src" \
        --distpath="$DIST_DIR" \
        --workpath="$BUILD_DIR" \
        --specpath="$BUILD_DIR" \
        --clean \
        src/obdiag.py
    
    # Cleanup
    rm -f src/obdiag.py
    
    if [ -f "$DIST_DIR/obdiag" ]; then
        echo -e "${GREEN}Binary built: $DIST_DIR/obdiag${NC}"
        ls -lh "$DIST_DIR/obdiag"
    else
        echo -e "${RED}Build failed!${NC}"
        restore_version
        exit 1
    fi
}

# Create package structure
create_package() {
    echo ""
    echo -e "${YELLOW}Creating package structure...${NC}"
    
    # Create package directory
    mkdir -p "$PKG_DIR"
    mkdir -p "$PKG_DIR/bin"
    mkdir -p "$PKG_DIR/plugins"
    mkdir -p "$PKG_DIR/conf"
    mkdir -p "$PKG_DIR/example"
    mkdir -p "$PKG_DIR/resources"
    
    # Copy binary
    cp "$DIST_DIR/obdiag" "$PKG_DIR/bin/"
    chmod +x "$PKG_DIR/bin/obdiag"
    
    # Copy plugins
    cp -r "$PROJECT_DIR/plugins/"* "$PKG_DIR/plugins/"
    
    # Copy config
    cp -r "$PROJECT_DIR/conf/"* "$PKG_DIR/conf/"
    
    # Copy examples
    cp -r "$PROJECT_DIR/example/"* "$PKG_DIR/example/"
    
    # Copy resources
    cp -r "$PROJECT_DIR/resources/"* "$PKG_DIR/resources/" 2>/dev/null || true
    
    # Copy install/uninstall scripts
    cp "$SCRIPT_DIR/install.sh" "$PKG_DIR/"
    cp "$SCRIPT_DIR/uninstall.sh" "$PKG_DIR/"
    chmod +x "$PKG_DIR/"*.sh
    
    # Create README
    cat > "$PKG_DIR/README.txt" << EOF
OceanBase Diagnostic Tool - macOS Package
==========================================

Version: $VERSION
Build:   $RELEASE
Arch:    $ARCH

Installation
------------
1. Extract this package to a directory
2. Run: ./install.sh

Or manually:
1. Copy bin/obdiag to /usr/local/bin/
2. Copy plugins/ to ~/.obdiag/
3. Copy example/ to ~/.obdiag/

Usage
-----
obdiag --help
obdiag config
obdiag check run

Uninstallation
--------------
Run: ./uninstall.sh

More Information
----------------
https://github.com/oceanbase/obdiag
https://www.oceanbase.com/docs/obdiag-cn
EOF

    echo -e "${GREEN}Package structure created${NC}"
}

# Create archive
create_archive() {
    echo ""
    echo -e "${YELLOW}Creating archive...${NC}"
    
    cd "$DIST_DIR"
    
    # Create tar.gz
    tar -czf "${PKG_NAME}.tar.gz" "$PKG_NAME"
    
    # Create zip
    zip -r "${PKG_NAME}.zip" "$PKG_NAME"
    
    echo ""
    echo -e "${GREEN}Archives created:${NC}"
    ls -lh "$DIST_DIR/${PKG_NAME}".{tar.gz,zip}
    
    # Calculate checksums
    echo ""
    echo "SHA256 Checksums:"
    shasum -a 256 "${PKG_NAME}.tar.gz"
    shasum -a 256 "${PKG_NAME}.zip"
}

# Print summary
print_summary() {
    echo ""
    echo -e "${BLUE}========================================"
    echo "Build Complete!"
    echo -e "========================================${NC}"
    echo ""
    echo "Package: $PKG_NAME"
    echo ""
    echo "Files:"
    echo "  $DIST_DIR/${PKG_NAME}.tar.gz"
    echo "  $DIST_DIR/${PKG_NAME}.zip"
    echo ""
    echo "To test the package:"
    echo "  cd $DIST_DIR"
    echo "  tar -xzf ${PKG_NAME}.tar.gz"
    echo "  cd $PKG_NAME"
    echo "  ./install.sh"
    echo ""
}

# Main
main() {
    check_python
    check_dependencies
    clean_build
    update_version
    
    # Trap to restore version on error
    trap restore_version EXIT
    
    build_binary
    create_package
    create_archive
    
    # Restore version
    restore_version
    trap - EXIT
    
    print_summary
}

# Run
main "$@"
