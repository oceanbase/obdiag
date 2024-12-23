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

# Check if a file with the same name already exists in the BACKUP_DIR
if find "$BACKUP_DIR" -maxdepth 1 -name "${BASE_NAME}_*.tar.gz" -print -quit | grep -q .; then
	echo "A backup file with the same name already exists. Skipping backup creation."
	exit 0
fi

# Temporary directory for staging backup files
TEMP_BACKUP_DIR="$BACKUP_DIR/tmp_obdiag_backup_$TIMESTAMP"
mkdir -p "$TEMP_BACKUP_DIR"

# Iterate over each directory to be backed up
for dir in "${DIRS[@]}"; do
    # Check if the source directory exists
    if [ -d "$SOURCE_DIR$dir" ]; then
        # Copy the directory into the temporary backup directory
        cp -rp "$SOURCE_DIR$dir" "$TEMP_BACKUP_DIR/"
        echo "Copied $dir to temporary backup directory."
    else
        echo "Source directory $SOURCE_DIR$dir does not exist. Skipping."
    fi
done

# Create a tar.gz archive
if tar -czf "$TARFILE" -C "$TEMP_BACKUP_DIR" .; then
    echo "Backup archive created successfully at $TARFILE"
else
    echo "Failed to create backup archive."
    exit 1
fi

# Clean up the temporary backup directory
rm -rf "$TEMP_BACKUP_DIR"
echo "Temporary files removed."

# Clean rca old *scene.py files
find ${SOURCE_DIR}/rca -maxdepth 1 -name "*_scene.py" -type f -exec rm -f {} + 2>/dev/null

# Cleanup phase: Remove backups older than one year or delete the oldest backups if more than 12 exist
ONE_YEAR_AGO="+365"  # find command uses days, so +365 means older than one year

# Remove backups older than one year
find "$BACKUP_DIR" -maxdepth 1 -name "obdiag_backup_*.tar.gz" -type f -mtime $ONE_YEAR_AGO -exec rm -f {} \;
echo "Removed old backup files older than one year."

# If there are more than 12 backups, remove the excess oldest ones
BACKUP_FILES=($(find "$BACKUP_DIR" -maxdepth 1 -name "obdiag_backup_*.tar.gz" -type f -printf '%T@ %p\n' | sort -n))
NUM_BACKUPS=${#BACKUP_FILES[@]}

if [ $NUM_BACKUPS -gt 12 ]; then
    COUNT_TO_DELETE=$((NUM_BACKUPS - 12))
    for ((i = 0; i < COUNT_TO_DELETE; i++)); do
        FILE_PATH=${BACKUP_FILES[i]#* }
        rm -f "$FILE_PATH"
        echo "Removed excess backup file: $FILE_PATH"
    done
fi