# Custom Dataset Creation

This repository contains a parameterized PySpark job for Torus custom dataset creation, 
along with scripts to deploy, run, and manage dependencies and configurations 
for executing within AWS EMR Serverless.

---

## Table of Contents
1. [Development](#development)
2. [Testing](#testing)
3. [Deployment](#deployment)
4. [Running the PySpark Job](#running-the-pyspark-job)
5. [Updating the Custom Docker Image](#updating-the-custom-docker-image)
6. [Requirements](#requirements)

---

## Development

### Setup Development Environment
```bash
# Clone the repository
git clone <repository-url>
cd dataset

# Setup virtual environment and install dependencies
make setup
# OR manually:
python -m venv env
source env/bin/activate
pip install -r requirements.txt
```

## Testing

This project has comprehensive unit test coverage (85-90%) across all major components.

### Quick Test Commands

```bash
# Run core module tests (recommended)
make test

# Run all tests
make test-all

# Run specific test modules
make test-utils
make test-lookup
```

### Alternative Test Commands

```bash
# Using Python script
python run_tests.py core     # Core modules
python run_tests.py all      # All tests
python run_tests.py utils    # Utils only

# Using npm (if Node.js available)
npm test                     # Core modules
npm run test:all            # All tests

# Manual commands
source env/bin/activate && python -m unittest tests.test_utils tests.test_event_registry tests.test_lookup tests.test_manifest tests.test_keys -v
```

### Test Coverage

- ✅ **Core Infrastructure**: utils, event_registry (100%)
- ✅ **Data Processing**: lookup, keys, manifest (100%) 
- ✅ **Event Handlers**: attempts, page_viewed, video
- ✅ **Datashop Processing**: XML generation, sanitization
- ✅ **Integration**: End-to-end workflows
- ✅ **Edge Cases**: Error handling, malformed data

---

## Deployment

The entrypoint for the PySpark job for custom dataset generation is defined in `job.py`.  Supporting
modules are found in the `dataset` directory.  To be invoked in the AWS EMR Serverless environment,
these files must be deployed and accessible from an S3 bucket. 

The `deploy.sh` script automates packaging and uploading the PySpark job script and dependencies to this S3 bucket.

### Steps to Deploy:
1. Run the `deploy.sh` script from the root directory:
   ```bash
   ./deploy.sh
   ```

## Running the PySpark Job

A job can be manually invoked from EMR Serverless Studio, but also directly from the commandline using
one of two helper bash scripts. These bash scripts are wrappers around the AWS commandline tool, which you
need to install from (https://aws.amazon.com/cli/)

### Steps to Run a CSV raw data job:
1. Run the `run_job.sh` script from the root directory with arguments for action, event subtypes, and section ids
   ```bash
   ./run_job.sh attempt_evaluated part_attempt_evaluated 2342,2343
   ```
### Steps to Run a Datashop XML job:
1. Generate the context JSON file using the `context.sql` and manually upload it to the `torus-datasets-prod` bucket in the `contexts` folder,
named the same as the job id sepecified in the next step.  
2. Run the `run_datashop.sh` script from the root directory with arguments for job id and the course section ids
   ```bash
   ./run_datasohp.sh 1922 2342,2343
   ```

For the above to work, the context file must be named `1922.json` and be present in the `contexts` folder. 


## Updating the Custom Docker Image

The dependencies needed by code executed by worker and executor nodes in PySpark are supplied via a custom EMR Docker image.  Periodically,
this image may need to be updated as we expand the feature set. The
Dockerfile is present at `config/Dockerfile` and the script 
`update_image.sh` automates the building and deployment of it. 
