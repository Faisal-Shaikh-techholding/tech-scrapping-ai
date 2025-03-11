#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Startup script for the AI-Powered CSV Processor application.

This script creates the logs directory and starts the Streamlit application.
"""

import os
import subprocess
import sys
from pathlib import Path
import site
import platform
import shutil

# Create logs directory if it doesn't exist
logs_dir = Path('logs')
logs_dir.mkdir(exist_ok=True)

# Find the absolute path to the app
app_path = Path(__file__).parent / 'app' / 'main.py'

if not app_path.exists():
    print(f"Error: Application file not found: {app_path}")
    sys.exit(1)

# Try to find streamlit executable
streamlit_path = None

# Check if streamlit is in PATH
streamlit_in_path = shutil.which('streamlit')
if streamlit_in_path:
    streamlit_path = Path(streamlit_in_path)

# Check common installation locations if not found in PATH
if not streamlit_path:
    possible_locations = [
        Path(site.getuserbase()) / "bin" / "streamlit",
        Path(os.path.expanduser("~")) / "Library" / "Python" / "3.11" / "bin" / "streamlit",  # macOS specific
        Path(os.path.expanduser("~")) / "Library" / "Python" / "3.10" / "bin" / "streamlit",  # macOS specific
        Path(os.path.expanduser("~")) / "Library" / "Python" / "3.9" / "bin" / "streamlit",   # macOS specific
        Path(os.path.expanduser("~")) / ".local" / "bin" / "streamlit",  # Linux/Unix
        Path(sys.prefix) / "bin" / "streamlit",  # Virtual environment
    ]
    
    if platform.system() == "Windows":
        possible_locations.extend([
            Path(site.getuserbase()) / "Scripts" / "streamlit.exe",
            Path(sys.prefix) / "Scripts" / "streamlit.exe",
        ])
    
    for loc in possible_locations:
        if loc.exists():
            streamlit_path = loc
            break

if not streamlit_path:
    print("Error: Streamlit executable not found.")
    print("Please make sure streamlit is installed using: pip install -r requirements.txt")
    print("You may need to add the streamlit installation directory to your PATH.")
    print("Based on the pip output, try: /Users/faisalshaikh/Library/Python/3.11/bin/streamlit")
    sys.exit(1)

# Print startup message
print("Starting AI-Powered CSV Processor application...")
print(f"Application path: {app_path}")
print(f"Streamlit path: {streamlit_path}")
print("Logs directory: ./logs")
print("Press Ctrl+C to stop the application")
print("-" * 50)

# Start Streamlit server
os.environ['PYTHONPATH'] = str(Path(__file__).parent)

try:
    subprocess.run([str(streamlit_path), "run", str(app_path)], check=True)
except subprocess.CalledProcessError as e:
    print(f"Error: Failed to run Streamlit application: {e}")
    sys.exit(1)
except KeyboardInterrupt:
    print("\nApplication stopped by user.")
    sys.exit(0)
except Exception as e:
    print(f"Unexpected error: {e}")
    sys.exit(1)
