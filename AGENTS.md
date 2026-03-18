# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a PySpark-based custom dataset creation system for Torus that processes xAPI event data from AWS S3 and generates datasets in CSV or Datashop XML format. It runs on AWS EMR Serverless and processes educational analytics data.

## Key Commands

### Testing
```bash
# Run specific test file
python -m unittest tests.test_utils
python -m unittest tests.test_datashop
python -m unittest tests.test_hints

# Run all tests in tests directory
python -m unittest discover tests
```

### Docker Image Management
```bash
# Update custom EMR Docker image with dependencies
cd config
./update_image.sh
```

## Architecture

### Core Components

1. **Entry Point**: `job.py` - Main PySpark job that parses arguments and routes to appropriate processing function
   - Handles two main actions: 'datashop' (XML format) or other event types (CSV format)
   - Configures context with bucket names, section IDs, page IDs, and processing parameters

2. **Processing Pipeline**:
   - **dataset/dataset.py**: Contains `generate_dataset()` and `generate_datashop()` functions
     - Initializes Spark context and S3 client
     - Retrieves keys from S3 inventory based on section IDs
     - Processes data in chunks for memory efficiency
     - For Datashop: Groups and sorts part attempts by user/session before XML conversion
   
   - **dataset/keys.py**: Handles S3 inventory operations to list relevant data files
   
   - **dataset/datashop.py**: Datashop XML format processing
     - `process_jsonl_file()`: Processes individual JSONL files
     - `process_part_attempts()`: Converts part attempts to XML format
     - Handles hint processing and transaction generation
   
   - **dataset/lookup.py**: Retrieves context/lookup data from S3 for Datashop processing

3. **Event Processing**:
   - **dataset/event_registry.py**: Registry of event types and their processing configurations
   - **dataset/attempts.py**: Processes attempt-related events
   - **dataset/page_viewed.py**: Processes page view events
   - **dataset/video.py**: Processes video interaction events

4. **Utilities**:
   - **dataset/utils.py**: Common utility functions (encoding, parallel processing, field pruning)
   - **dataset/manifest.py**: Generates HTML/JSON manifests for datasets

### Data Flow

1. Job receives parameters (section IDs, event types, etc.)
2. Retrieves file keys from S3 inventory based on section IDs
3. Processes files in parallel chunks using Spark
4. For Datashop: Groups results by user/session, sorts by page/activity/part hierarchy
5. Outputs results to S3 as CSV or XML chunks

### Key Processing Features

- **Chunked Processing**: Handles large datasets by processing in configurable chunk sizes
- **Parallel Processing**: Uses Spark's parallel_map for distributed processing
- **Field Exclusion**: Supports excluding specific fields from output
- **Anonymization**: Can anonymize student IDs if configured
- **Multi-format Output**: Supports CSV and Datashop XML formats

## AWS Infrastructure Requirements

- **S3 Buckets**: 
  - Source bucket for xAPI data (e.g., `torus-xapi-prod`)
  - Inventory bucket for S3 inventory data
  - Results bucket for datasets (e.g., `torus-datasets-prod`)
  - Analytics jobs bucket for scripts and logs

- **EMR Serverless**: Application configured with custom Docker image containing Python dependencies

- **IAM Role**: EMR execution role with S3 access permissions

## Dependencies

Main Python packages (from requirements.txt):
- pyspark==3.5.3
- boto3==1.35.73
- pandas==2.2.3
- numpy==2.1.3