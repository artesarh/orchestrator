#!/usr/bin/env python3
"""
Debug script to show exactly what config files are being loaded
"""

import os
import sys
from pathlib import Path

def debug_config_loading():
    print("=== CONFIG DEBUG ===")
    print(f"APP_ENV environment variable: {os.getenv('APP_ENV', 'NOT SET')}")
    print()
    
    # Show which files exist
    root_dir = Path(".")
    env_files = [".env", ".env.dev", ".env.test", ".env.prod"]
    
    print("Environment files:")
    for env_file in env_files:
        file_path = root_dir / env_file
        if file_path.exists():
            print(f"✅ {env_file}: EXISTS")
        else:
            print(f"❌ {env_file}: NOT FOUND")
    print()
    
    # Before importing config
    print("BEFORE importing orchestrator.utils.config:")
    important_vars = ["DJANGO_API_URL", "DJANGO_JWT_TOKEN", "EXTERNAL_API_URL", "EXTERNAL_API_KEY"]
    for var in important_vars:
        value = os.getenv(var)
        if value:
            display_value = f"{value[:20]}..." if len(value) > 20 else value
            print(f"  {var}: {display_value}")
        else:
            print(f"  {var}: NOT SET")
    print()
    
    # Import config system
    print("Importing config system...")
    try:
        from orchestrator.utils.config import get_environment, DJANGO_API_URL
        print("✅ Config imported successfully")
        print(f"Detected environment: {get_environment().value}")
        print(f"Django API URL: {DJANGO_API_URL}")
        
        # Try to import other variables
        try:
            from orchestrator.utils.config import DJANGO_JWT_TOKEN
            print(f"Django JWT Token: {DJANGO_JWT_TOKEN[:20]}...")
        except ValueError as e:
            print(f"❌ Django JWT Token: {e}")
            
        try:
            from orchestrator.utils.config import EXTERNAL_API_URL
            print(f"External API URL: {EXTERNAL_API_URL}")
        except ValueError as e:
            print(f"❌ External API URL: {e}")
            
        try:
            from orchestrator.utils.config import EXTERNAL_API_KEY
            print(f"External API Key: {EXTERNAL_API_KEY[:20]}...")
        except ValueError as e:
            print(f"❌ External API Key: {e}")
            
    except Exception as e:
        print(f"❌ Error importing config: {e}")
    
    print()
    print("AFTER importing orchestrator.utils.config:")
    for var in important_vars:
        value = os.getenv(var)
        if value:
            display_value = f"{value[:20]}..." if len(value) > 20 else value
            print(f"  {var}: {display_value}")
        else:
            print(f"  {var}: NOT SET")

if __name__ == "__main__":
    debug_config_loading() 