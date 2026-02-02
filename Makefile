# OceanBase Diagnostic Tool - Development Makefile
# Converted from dev_helper.sh

SHELL := /bin/bash
PROJECT_PATH := $(shell pwd)
WORK_DIR := $(shell pwd)
OBDIAG_HOME ?= $(HOME)/.obdiag
RELEASE := $(shell date +%Y%m%d%H%M)
OBDIAG_VERSION ?= 4.0.0

# URLs for obstack downloads
OBUTILS_AARCH64_URL := https://obbusiness-private.oss-cn-shanghai.aliyuncs.com/download-center/opensource/observer/v4.3.5_CE/oceanbase-ce-utils-4.3.5.0-100000202024123117.el7.aarch64.rpm
OBUTILS_X64_URL := https://obbusiness-private.oss-cn-shanghai.aliyuncs.com/download-center/opensource/observer/v4.3.5_CE/oceanbase-ce-utils-4.3.5.0-100000202024123117.el7.x86_64.rpm

# Python version requirements
PYTHON_MIN_MAJOR := 3
PYTHON_MIN_MINOR := 11

.PHONY: all help pack clean init format download_obstack clean_rpm check_python install_requirements copy_files backup_obdiag build_update_package

# Default target
all: help

# Show help information
help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@echo "  pack                 - Build RPM package"
	@echo "  build_update_package - Build plugins update package (data.tar + version.yaml)"
	@echo "  clean                - Clean result files (gather/analyze packs)"
	@echo "  clean_rpm            - Clean old RPM build data"
	@echo "  init                 - Initialize development environment"
	@echo "  format               - Format code with black"
	@echo "  download_obstack     - Download obstack tools"
	@echo "  check_python         - Check Python version (>= 3.11)"
	@echo "  help                 - Show this help message"

# Build RPM package
# Note: requires rpm-build package, install with: sudo yum install rpm-build -y
pack: clean_rpm download_obstack
	@echo "Building RPM package (version: $(OBDIAG_VERSION))..."
	@command -v rpmbuild >/dev/null 2>&1 || (echo "Error: rpmbuild not found. Please install: sudo yum install rpm-build -y" && exit 1)
	@export RELEASE=$(RELEASE) && \
	export OBDIAG_VERSION=$(OBDIAG_VERSION) && \
	rpmbuild -bb ./rpm/oceanbase-diagnostic-tool.spec && \
	rpm_path=$$(find ~/rpmbuild -name "oceanbase-diagnostic-tool-*$(RELEASE)*.rpm") && \
	echo "rpm_path: $${rpm_path}" && \
	cp -rf $${rpm_path} ./ && \
	echo "rpm cp to: $$(pwd)/$$(basename $${rpm_path})"

	@echo "Build RPM package success"

# Build plugins update package
build_update_package:
	@echo "Building plugins update package..."
	@cd plugins && tar -cvf data.tar *
	@sha=$$(cd plugins && sha256sum data.tar | awk '{print $$1}') && \
	echo "obdiag_version: \"$(OBDIAG_VERSION)\"" > version.yaml && \
	echo "remote_tar_sha: \"$$sha\"" >> version.yaml
	@mv plugins/data.tar ./
	@echo ""
	@echo "Build completed:"
	@echo "  - data.tar      (plugins archive)"
	@echo "  - version.yaml  (version and SHA256)"
	@cat version.yaml

# Download obstack tools
download_obstack:
	@echo "Checking obstack..."
	@mkdir -p ./dependencies/bin
	@if [ -f ./dependencies/bin/obstack_aarch64 ]; then \
		echo "obstack_aarch64 exists, skip download"; \
	else \
		echo "Downloading aarch64 obstack..."; \
		wget --quiet $(OBUTILS_AARCH64_URL) -O ./obutils.rpm && \
		rpm2cpio obutils.rpm | cpio -idv && \
		cp -f ./usr/bin/obstack ./dependencies/bin/obstack_aarch64 && \
		rm -rf ./usr ./obutils.rpm; \
	fi
	@if [ -f ./dependencies/bin/obstack_x86_64 ]; then \
		echo "obstack_x86_64 exists, skip download"; \
	else \
		echo "Downloading x64 obstack..."; \
		wget --quiet $(OBUTILS_X64_URL) -O ./obutils.rpm && \
		rpm2cpio obutils.rpm | cpio -idv && \
		cp -f ./usr/bin/obstack ./dependencies/bin/obstack_x86_64 && \
		rm -rf ./usr ./obutils.rpm; \
	fi

# Clean old RPM build data
clean_rpm:
	@echo "Cleaning old RPM data..."
	@rm -rf ./rpmbuild ./build ./dist ./src/obdiag.py ./BUILDROOT ./get-pip.py ./obdiag.spec
	@echo "Clean old RPM data success"

# Clean result files
clean:
	@echo "Cleaning result files..."
	@rm -rf ./obdiag_gather_pack_* ./obdiag_analyze_pack_* ./obdiag_analyze_flt_result* ./obdiag_check_report
	@echo "Clean result files success"

# Check Python version
check_python:
	@echo "Checking Python version..."
	@python3 -c "import sys; exit(0 if sys.version_info >= ($(PYTHON_MIN_MAJOR), $(PYTHON_MIN_MINOR)) else 1)" || \
		(echo "Error: Python version must be >= $(PYTHON_MIN_MAJOR).$(PYTHON_MIN_MINOR)" && exit 1)
	@echo "Python version check passed"

# Install requirements (from pyproject.toml)
install_requirements:
	@echo "Installing dependencies from pyproject.toml..."
	@pip3 install -e "$(PROJECT_PATH)"

# Backup obdiag folders
backup_obdiag:
	@echo "Backing up obdiag folders..."
	@mkdir -p $(OBDIAG_HOME)/dev_backup
	@datestamp=$$(date +%Y%m%d_%H%M%S) && \
	tar -czf $(OBDIAG_HOME)/dev_backup/obdiag_backup_$${datestamp}.tar.gz \
		-C $(OBDIAG_HOME) check display gather rca 2>/dev/null || \
		echo "No folders found to back up or backup failed"

# Copy plugin files to OBDIAG_HOME
copy_files:
	@echo "Copying files to $(OBDIAG_HOME)..."
	@mkdir -p $(OBDIAG_HOME)/check $(OBDIAG_HOME)/gather $(OBDIAG_HOME)/display
	@cp -rf $(WORK_DIR)/plugins/* $(OBDIAG_HOME)/
	@if [ -d "$(WORK_DIR)/example" ]; then \
		cp -rf $(WORK_DIR)/example $(OBDIAG_HOME)/; \
	fi
	@echo "Files copied successfully"

# Remove existing obdiag folders
remove_folders:
	@echo "Removing existing folders..."
	@for folder in check display gather rca; do \
		if [ -d "$(OBDIAG_HOME)/$$folder" ]; then \
			echo "Removing $(OBDIAG_HOME)/$$folder"; \
			rm -rf "$(OBDIAG_HOME)/$$folder"; \
		fi; \
	done

# Initialize development environment
init: check_python backup_obdiag remove_folders copy_files install_requirements download_obstack
	@echo "Setting up environment..."
	@export PYTHONPATH=$$PYTHONPATH:$(PROJECT_PATH)
	@source $(WORK_DIR)/rpm/init_obdiag_cmd.sh 2>/dev/null || true
	@echo ""
	@echo "=============================================="
	@echo "Initialization completed successfully!"
	@echo ""
	@echo "Please run the following commands manually:"
	@echo "  export PYTHONPATH=\$$PYTHONPATH:$(PROJECT_PATH)"
	@echo "  alias obdiag='python3 $(PROJECT_PATH)/src/main.py'"
	@echo ""
	@echo "Or add them to your ~/.bashrc:"
	@echo "  echo 'export PYTHONPATH=\$$PYTHONPATH:$(PROJECT_PATH)' >> ~/.bashrc"
	@echo "  echo \"alias obdiag='python3 $(PROJECT_PATH)/src/main.py'\" >> ~/.bashrc"
	@echo "  source ~/.bashrc"
	@echo "=============================================="

# Format code with black
format:
	@echo "Formatting code with black..."
	@command -v black >/dev/null 2>&1 || pip3 install --user black
	@black -S -l 256 .
	@echo "Code formatting completed"

# Additional useful targets

# Run obdiag directly (for development)
run:
	@PYTHONPATH=$(PROJECT_PATH):$$PYTHONPATH python3 $(PROJECT_PATH)/src/main.py $(ARGS)

# Run tests (placeholder)
test:
	@echo "Running tests..."
	@PYTHONPATH=$(PROJECT_PATH):$$PYTHONPATH python3 -m pytest tests/ -v 2>/dev/null || echo "No tests found or pytest not installed"

# Show current configuration
info:
	@echo "Project Configuration:"
	@echo "  PROJECT_PATH:   $(PROJECT_PATH)"
	@echo "  WORK_DIR:       $(WORK_DIR)"
	@echo "  OBDIAG_HOME:    $(OBDIAG_HOME)"
	@echo "  OBDIAG_VERSION: $(OBDIAG_VERSION)"
	@echo "  RELEASE:        $(RELEASE)"
	@echo "  Python:         $$(python3 --version 2>&1)"

# ==================== macOS Targets ====================

.PHONY: pack_macos install_macos uninstall_macos

# Install obdiag locally on macOS (Homebrew-style)
install_macos:
	@echo "Installing obdiag on macOS..."
	@chmod +x $(PROJECT_PATH)/macos/test_local_install.sh
	@$(PROJECT_PATH)/macos/test_local_install.sh install

# Uninstall obdiag from macOS
uninstall_macos:
	@echo "Uninstalling obdiag from macOS..."
	@chmod +x $(PROJECT_PATH)/macos/uninstall.sh
	@$(PROJECT_PATH)/macos/uninstall.sh

# Build macOS package (requires Python 3.11+)
pack_macos:
	@echo "Building macOS package (version: $(OBDIAG_VERSION))..."
	@command -v pyinstaller >/dev/null 2>&1 || pip3 install pyinstaller
	@mkdir -p $(PROJECT_PATH)/dist_macos
	@cp -f src/main.py src/obdiag.py
	@sed -i '' "s/<B_TIME>/$$(date)/" ./src/common/version.py 2>/dev/null || sed -i "s/<B_TIME>/$$(date)/" ./src/common/version.py
	@sed -i '' "s/<VERSION>/$(OBDIAG_VERSION)/" ./src/common/version.py 2>/dev/null || sed -i "s/<VERSION>/$(OBDIAG_VERSION)/" ./src/common/version.py
	@pyinstaller --hidden-import=decimal --hidden-import=pyzipper -p $(PROJECT_PATH)/src -F src/obdiag.py --distpath $(PROJECT_PATH)/dist_macos
	@rm -f src/obdiag.py
	@echo "macOS binary built: $(PROJECT_PATH)/dist_macos/obdiag"
	@echo ""
	@echo "To create a distributable package:"
	@echo "  1. Copy dist_macos/obdiag, plugins/, conf/, example/ to a directory"
	@echo "  2. Create a DMG or ZIP file for distribution"
