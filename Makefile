# Dataset Processing Test Commands

.PHONY: test test-core test-all test-coverage help

# Activate virtual environment and run core tests
test-core:
	@echo "Running core module tests..."
	source env/bin/activate && python -m unittest tests.test_utils tests.test_event_registry tests.test_lookup tests.test_manifest tests.test_keys -v

# Run all tests
test-all:
	@echo "Running all tests..."
	source env/bin/activate && python -m unittest discover tests -v

# Run specific test module
test-utils:
	source env/bin/activate && python -m unittest tests.test_utils -v

test-lookup:
	source env/bin/activate && python -m unittest tests.test_lookup -v

test-datashop:
	source env/bin/activate && python -m unittest tests.test_datashop -v

# Default test command
test: test-core

# Setup development environment
setup:
	python -m venv env
	source env/bin/activate && pip install -r requirements.txt

# Clean up
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

help:
	@echo "Available commands:"
	@echo "  make test       - Run core module tests (default)"
	@echo "  make test-core  - Run core module tests"
	@echo "  make test-all   - Run all tests"
	@echo "  make test-utils - Run utils tests only"
	@echo "  make setup      - Setup development environment"
	@echo "  make clean      - Clean Python cache files"
	@echo "  make help       - Show this help message"