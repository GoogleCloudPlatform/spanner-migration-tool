#!/bin/bash

# Define the path to the dist directory and index.html file
DIST_DIR="ui/dist"
INDEX_FILE="${DIST_DIR}/index.html"

# Check if index.html exists
if [ ! -f "$INDEX_FILE" ]; then
  echo "Error: index.html not found in $DIST_DIR"
  exit 1
fi

# Define the patterns to search for
declare -a patterns=("styles.*.css" "runtime.*.js" "polyfills.*.js" "main.*.js")

# Flag to check if any file is missing
MISSING_FILES=false

# Check each pattern
for pattern in "${patterns[@]}"; do
  # Use grep to find the file reference in index.html
  file=$(grep -oP '(?<=href="|src=")'$pattern'' "$INDEX_FILE" | head -n 1)
  if [ -z "$file" ]; then
    echo "Error: No file matching pattern $pattern found in $INDEX_FILE"
    MISSING_FILES=true
  else
    # Check if the file exists in the dist directory
    FILE_PATH="${DIST_DIR}/${file}"
    if [ ! -f "$FILE_PATH" ]; then
      echo "Error: File not found - $FILE_PATH"
      MISSING_FILES=true
    else
      echo "File exists: $FILE_PATH"
    fi
  fi
done

# Exit with an error if any file was missing
if [ "$MISSING_FILES" = true ]; then
  echo "Some files are missing. Verification failed."
  exit 1
else
  echo "All files are present. Verification passed."
fi
