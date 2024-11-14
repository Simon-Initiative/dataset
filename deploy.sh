#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "Starting deployment..."

# Define variables
BUCKET_NAME="analyticsjobs"
DATASET_DIR="dataset"
MAIN_FILE="job.py"
ZIP_FILE="dataset.zip"

# Check if the dataset directory exists
if [ ! -d "$DATASET_DIR" ]; then
  echo "Error: Directory '$DATASET_DIR' does not exist."
  exit 1
fi

# Check if the main file exists
if [ ! -f "$MAIN_FILE" ]; then
  echo "Error: Main file '$MAIN_FILE' does not exist."
  exit 1
fi

# Upload the main file to S3
echo "Uploading $MAIN_FILE to S3..."
aws s3 cp "$MAIN_FILE" "s3://$BUCKET_NAME/$MAIN_FILE"

# Package all files in the dataset directory into a ZIP file
echo "Creating ZIP file..."
zip -r "$ZIP_FILE" "$DATASET_DIR"

# Upload the ZIP file to S3
echo "Uploading $ZIP_FILE to S3..."
aws s3 cp "$ZIP_FILE" "s3://$BUCKET_NAME/$ZIP_FILE"

# Cleanup: Remove the ZIP file after upload
echo "Cleaning up..."
rm -f "$ZIP_FILE"

echo "Deployment completed successfully!"
