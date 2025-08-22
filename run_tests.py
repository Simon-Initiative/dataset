#!/usr/bin/env python3
"""
Test runner script for dataset processing project.
Usage: python run_tests.py [test_type]
"""

import sys
import subprocess
import os

def run_command(command, description):
    """Run a shell command and print the description."""
    print(f"\n🧪 {description}")
    print("=" * 60)
    result = subprocess.run(command, shell=True, cwd=os.path.dirname(os.path.abspath(__file__)))
    return result.returncode == 0

def main():
    test_type = sys.argv[1] if len(sys.argv) > 1 else "core"
    
    # Ensure we're in the project directory
    project_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(project_dir)
    
    commands = {
        "core": {
            "cmd": "source env/bin/activate && python -m unittest tests.test_utils tests.test_event_registry tests.test_lookup tests.test_manifest tests.test_keys -v",
            "desc": "Running core module tests"
        },
        "all": {
            "cmd": "source env/bin/activate && python -m unittest discover tests -v", 
            "desc": "Running all tests"
        },
        "utils": {
            "cmd": "source env/bin/activate && python -m unittest tests.test_utils -v",
            "desc": "Running utils tests"
        },
        "lookup": {
            "cmd": "source env/bin/activate && python -m unittest tests.test_lookup -v",
            "desc": "Running lookup tests"
        },
        "datashop": {
            "cmd": "source env/bin/activate && python -m unittest tests.test_datashop -v",
            "desc": "Running datashop tests"
        }
    }
    
    if test_type not in commands:
        print(f"❌ Unknown test type: {test_type}")
        print("\nAvailable test types:")
        for cmd_name, cmd_info in commands.items():
            print(f"  {cmd_name:10} - {cmd_info['desc']}")
        sys.exit(1)
    
    cmd_info = commands[test_type]
    success = run_command(cmd_info["cmd"], cmd_info["desc"])
    
    if success:
        print(f"\n✅ {cmd_info['desc']} completed successfully!")
    else:
        print(f"\n❌ {cmd_info['desc']} failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()