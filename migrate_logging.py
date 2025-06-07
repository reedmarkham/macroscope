#!/usr/bin/env python3
"""
Script to migrate print() statements to logging.logger calls.
"""

import re
import sys
import os

def migrate_file_logging(file_path):
    """Migrate print statements to logging in a single file."""
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Add logging import if not present
    if 'import logging' not in content:
        # Find the imports section and add logging
        import_pattern = r'(import [^\n]+\n)+'
        match = re.search(import_pattern, content)
        if match:
            imports_end = match.end()
            content = (content[:imports_end] + 
                      'import logging\n' + 
                      content[imports_end:])
    
    # Add logger configuration if not present
    if 'logger = logging.getLogger' not in content:
        # Find config_manager import and add logger setup after it
        config_manager_pattern = r'from config_manager import get_config_manager\n'
        match = re.search(config_manager_pattern, content)
        if match:
            insert_pos = match.end()
            logger_setup = """
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
"""
            content = content[:insert_pos] + logger_setup + content[insert_pos:]
    
    # Migrate print statements to logger calls
    # Pattern: print(f"text with {variable}")
    f_string_pattern = r'print\(f"([^"]*?)"\)'
    def replace_f_string(match):
        message = match.group(1)
        # Determine log level based on keywords
        if any(keyword in message.lower() for keyword in ['error', 'failed', 'exception']):
            return f'logger.error("{message}")'
        elif any(keyword in message.lower() for keyword in ['warning', 'warn']):
            return f'logger.warning("{message}")'
        else:
            return f'logger.info("{message}")'
    
    content = re.sub(f_string_pattern, replace_f_string, content)
    
    # Pattern: print("simple string")
    simple_string_pattern = r'print\("([^"]*?)"\)'
    def replace_simple_string(match):
        message = match.group(1)
        if any(keyword in message.lower() for keyword in ['error', 'failed', 'exception']):
            return f'logger.error("{message}")'
        elif any(keyword in message.lower() for keyword in ['warning', 'warn']):
            return f'logger.warning("{message}")'
        else:
            return f'logger.info("{message}")'
    
    content = re.sub(simple_string_pattern, replace_simple_string, content)
    
    # Write back the modified content
    with open(file_path, 'w') as f:
        f.write(content)
    
    print(f"Migrated logging in: {file_path}")

def main():
    """Main function to migrate logging in specified directories."""
    base_dir = "/Users/reedmarkham/github/electron-microscopy-ingest"
    
    # Directories to process
    app_dirs = [
        "app/ebi",
        "app/epfl", 
        "app/flyem",
        "app/idr",
        "app/openorganelle",
        "app/consolidate"
    ]
    
    for app_dir in app_dirs:
        full_path = os.path.join(base_dir, app_dir)
        if os.path.exists(full_path):
            # Process main.py files
            main_py = os.path.join(full_path, "main.py")
            if os.path.exists(main_py):
                migrate_file_logging(main_py)
            
            # Process helper files if they exist
            helpers_py = os.path.join(full_path, "helpers.py")
            if os.path.exists(helpers_py):
                migrate_file_logging(helpers_py)
            
            # Process processing.py if it exists
            processing_py = os.path.join(full_path, "processing.py")
            if os.path.exists(processing_py):
                migrate_file_logging(processing_py)

if __name__ == "__main__":
    main()