import sys
import os

# 1. Get the directory where this conftest.py file lives (/tests)
current_dir = os.path.dirname(__file__)

# 2. Go up one level to the project root (/streamshop-data-platform)
project_root = os.path.abspath(os.path.join(current_dir, '..'))

# 3. Forcefully inject the project root into Python's system path
sys.path.insert(0, project_root)