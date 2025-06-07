#!/usr/bin/env python3
"""
Script to migrate OpenOrganelle print() statements to logging.
"""

import re
import os

def migrate_openorganelle_logging():
    """Migrate OpenOrganelle logging systematically."""
    
    base_dir = "/Users/reedmarkham/github/electron-microscopy-ingest/app/openorganelle"
    files_to_migrate = ["main.py", "helpers.py", "processing.py"]
    
    for filename in files_to_migrate:
        file_path = os.path.join(base_dir, filename)
        if not os.path.exists(file_path):
            continue
            
        print(f"Migrating {filename}...")
        
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Add logging import if not present
        if 'import logging' not in content:
            # Add after existing imports
            import_pattern = r'(from tqdm import tqdm\n)'
            if re.search(import_pattern, content):
                content = re.sub(import_pattern, r'\1import logging\n', content)
            else:
                # Fallback - add after numpy import
                content = re.sub(r'(import numpy as np\n)', r'\1import logging\n', content)
        
        # Add logger if not present  
        if 'logger = logging.getLogger' not in content:
            # Add after config_manager import or at start of functions
            config_pattern = r'(from config_manager import get_config_manager\n)'
            logger_setup = """\n# Configure logging
logger = logging.getLogger(__name__)
"""
            if re.search(config_pattern, content):
                content = re.sub(config_pattern, r'\1' + logger_setup, content)
            else:
                # Add at the beginning of the file after imports
                first_function = re.search(r'\n\ndef ', content)
                if first_function:
                    pos = first_function.start()
                    content = content[:pos] + logger_setup + content[pos:]
        
        # Convert print statements based on content
        replacements = [
            # Error messages
            (r'print\(f"❌([^"]*)"([^)]*)\)', r'logger.error("\1"\2)'),
            (r'print\(f"⚠️([^"]*)"([^)]*)\)', r'logger.warning("\1"\2)'),
            (r'print\("❌([^"]*)"([^)]*)\)', r'logger.error("\1"\2)'),
            (r'print\("⚠️([^"]*)"([^)]*)\)', r'logger.warning("\1"\2)'),
            
            # Remove emojis and convert to appropriate log levels
            (r'print\(f"🚨([^"]*)"([^)]*)\)', r'logger.error("\1"\2)'),
            (r'print\(f"💥([^"]*)"([^)]*)\)', r'logger.error("\1"\2)'),
            (r'print\(f"🔧([^"]*)"([^)]*)\)', r'logger.info("\1"\2)'),
            (r'print\(f"📊([^"]*)"([^)]*)\)', r'logger.info("\1"\2)'),
            (r'print\(f"🔍([^"]*)"([^)]*)\)', r'logger.debug("\1"\2)'),
            (r'print\(f"✅([^"]*)"([^)]*)\)', r'logger.info("\1"\2)'),
            (r'print\(f"🔗([^"]*)"([^)]*)\)', r'logger.info("\1"\2)'),
            (r'print\(f"📡([^"]*)"([^)]*)\)', r'logger.info("\1"\2)'),
            (r'print\(f"📋([^"]*)"([^)]*)\)', r'logger.info("\1"\2)'),
            (r'print\(f"🧩([^"]*)"([^)]*)\)', r'logger.info("\1"\2)'),
            (r'print\(f"🔄([^"]*)"([^)]*)\)', r'logger.info("\1"\2)'),
            (r'print\(f"⚡([^"]*)"([^)]*)\)', r'logger.info("\1"\2)'),
            (r'print\(f"🛡️([^"]*)"([^)]*)\)', r'logger.info("\1"\2)'),
            (r'print\(f"🟢([^"]*)"([^)]*)\)', r'logger.info("\1"\2)'),
            (r'print\(f"🟡([^"]*)"([^)]*)\)', r'logger.info("\1"\2)'),
            (r'print\(f"🔴([^"]*)"([^)]*)\)', r'logger.info("\1"\2)'),
            
            # Simple print statements with emojis
            (r'print\("🚨([^"]*)"([^)]*)\)', r'logger.error("\1"\2)'),
            (r'print\("💥([^"]*)"([^)]*)\)', r'logger.error("\1"\2)'),
            (r'print\("🔧([^"]*)"([^)]*)\)', r'logger.info("\1"\2)'),
            (r'print\("📊([^"]*)"([^)]*)\)', r'logger.info("\1"\2)'),
            (r'print\("✅([^"]*)"([^)]*)\)', r'logger.info("\1"\2)'),
            
            # General f-string prints (catch-all)
            (r'print\(f"([^"]*)"([^)]*)\)', r'logger.info("\1"\2)'),
            
            # Simple string prints
            (r'print\("([^"]*)"([^)]*)\)', r'logger.info("\1"\2)'),
        ]
        
        # Apply replacements
        for pattern, replacement in replacements:
            content = re.sub(pattern, replacement, content)
        
        # Write back
        with open(file_path, 'w') as f:
            f.write(content)
        
        print(f"Migrated {filename} successfully")

if __name__ == "__main__":
    migrate_openorganelle_logging()