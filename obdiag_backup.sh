#!/bin/bash

# Define source directory and target backup directory
SOURCE_DIR=~/.obdiag/
BACKUP_DIR=~/.obdiag/backup/

# Ensure the backup directory exists, create it if it does not
mkdir -p "$BACKUP_DIR"

# List of directories to be backed up
DIRS=("display" "check" "gather" "rca")

# Retrieve version information; if file does not exist or read fails, VERSION remains empty
VERSION=""
if [ -f "$SOURCE_DIR/version.yaml" ]; then
    VERSION=$(grep 'obdiag_version:' "$SOURCE_DIR/version.yaml" | awk '{print $2}' | tr -d '"')
fi

# Define the name of the backup archive (use timestamp for uniqueness, and optionally add version)
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
TARFILE="$BACKUP_DIR/obdiag_backup_$TIMESTAMP"
TARFILE+="${VERSION:+_v$VERSION}.tar.gz"

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