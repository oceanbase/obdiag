#!/bin/bash

CURRENT_USER_ID=$(id -u)
CURRENT_USER_NAME=$(logname 2>/dev/null || echo "$SUDO_USER" | awk -F'[^a-zA-Z0-9_]' '{print $1}')

if [ "$CURRENT_USER_ID" -eq 0 ]; then
    if [ -n "$SUDO_USER" ]; then
        USER_HOME=$(getent passwd "$SUDO_USER" | cut -d: -f6)
    else
        USER_HOME=/root
    fi
else
    USER_HOME="$HOME"
fi

# Define source directory and target backup directory
SOURCE_DIR="$USER_HOME/.obdiag/"
BACKUP_DIR="$USER_HOME/.obdiag/backup/"

# Ensure the backup directory exists, create it if it does not
mkdir -p "$BACKUP_DIR"

# List of directories to be backed up
DIRS=("display" "check" "gather" "rca")

# Check if any of the directories contain files
should_backup=false
for dir in "${DIRS[@]}"; do
    if [ -d "$SOURCE_DIR$dir" ] && [ "$(ls -A "$SOURCE_DIR$dir")" ]; then
        should_backup=true
        break
    fi
done

if ! $should_backup; then
    echo "None of the specified directories contain files. Skipping backup."
    exit 0
fi

# Retrieve version information; if file does not exist or read fails, VERSION remains empty
VERSION=""
if [ -f "$SOURCE_DIR/version.yaml" ]; then
    VERSION=$(grep 'obdiag_version:' "$SOURCE_DIR/version.yaml" | awk '{print $2}' | tr -d '"')
fi

# Define the name of the backup archive (use timestamp for uniqueness, and optionally add version)
TIMESTAMP=$(date +"%Y%m%d%H%M%S")
BASE_NAME="obdiag_backup${VERSION:+_v$VERSION}"
TARFILE="$BACKUP_DIR/${BASE_NAME}_$TIMESTAMP.tar.gz"

# Check if a file with the same base name already exists in the BACKUP_DIR
if find "$BACKUP_DIR" -maxdepth 1 -name "${BASE_NAME}_*.tar.gz" -print -quit | grep -q .; then
    echo "A backup file with the same base name already exists. Skipping backup creation."
    exit 0
fi

# Temporary directory for staging backup files, including top-level directory
TEMP_BACKUP_DIR="$BACKUP_DIR/tmp_obdiag_backup_$TIMESTAMP"
TOP_LEVEL_DIR="$TEMP_BACKUP_DIR/obdiag_backup${VERSION:+_v$VERSION}_$TIMESTAMP"  # Top-level directory inside the tarball
mkdir -p "$TOP_LEVEL_DIR"

# Iterate over each directory to be backed up
for dir in "${DIRS[@]}"; do
    # Check if the source directory exists
    if [ -d "$SOURCE_DIR$dir" ]; then
        # Copy the directory into the top-level directory within the temporary backup directory
        cp -rp "$SOURCE_DIR$dir" "$TOP_LEVEL_DIR/"
        echo "Copied $dir to temporary backup directory under ${BASE_NAME}_$TIMESTAMP."
    else
        echo "Source directory $SOURCE_DIR$dir does not exist. Skipping."
    fi
done

# Create a tar.gz archive with the top-level directory included
if tar -czf "$TARFILE" -C "$TEMP_BACKUP_DIR" "obdiag_backup${VERSION:+_v$VERSION}_$TIMESTAMP"; then
    echo "Backup archive created successfully at $TARFILE"
else
    echo "Failed to create backup archive."
    exit 1
fi

# Clean up the temporary backup directory
rm -rf "$TEMP_BACKUP_DIR"
echo "Temporary files removed."

# Cleanup phase: Remove backups older than one year or delete the oldest backups if more than 12 exist
ONE_YEAR_AGO="+365"  # find command uses days, so +365 means older than one year

# Function to remove a single oldest backup file and print the action
remove_oldest_backup() {
    BACKUP_FILE=$(find "$BACKUP_DIR" -maxdepth 1 -name "obdiag_backup_*.tar.gz" -type f -printf '%T+ %p\n' | sort | head -n 1 | cut -d ' ' -f2-)
    if [ -n "$BACKUP_FILE" ]; then
        echo "Attempting to remove oldest backup file: $BACKUP_FILE"
        if rm -f "$BACKUP_FILE"; then
            echo "Successfully removed oldest backup file: $BACKUP_FILE"
            return 0
        else
            echo "Failed to remove oldest backup file: $BACKUP_FILE"
            return 1
        fi
    else
        echo "No backup files found."
        return 1
    fi
}

# Function to check if there are backups older than one year
has_old_backups() {
    if find "$BACKUP_DIR" -maxdepth 1 -name "obdiag_backup_*.tar.gz" -type f -mtime $ONE_YEAR_AGO | grep -q .; then
        echo "Found old backup files."
        return 0
    else
        echo "No old backup files found."
        return 1
    fi
}

# Function to check if there are more than 12 backups
has_too_many_backups() {
    COUNT=$(find "$BACKUP_DIR" -maxdepth 1 -name "obdiag_backup_*.tar.gz" -type f | wc -l)
    if [ $COUNT -gt 12 ]; then
        echo "More than 12 backup files found: $COUNT"
        return 0
    else
        echo "Backup count within limit: $COUNT"
        return 1
    fi
}

# Cleanup loop: Remove only one file at a time until neither condition is met
echo "Starting cleanup process..."
while has_old_backups || has_too_many_backups; do
    if ! remove_oldest_backup; then
        echo "Cleanup process stopped due to failure in removing oldest backup."
        break  # Stop if no more files to remove or removal failed
    fi
done
echo "Cleanup process completed."