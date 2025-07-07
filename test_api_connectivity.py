#!/usr/bin/env python3
"""
Quick connectivity test for Django API
Tests the /api endpoint to verify server is running and accessible
"""

import requests
import sys
import os
from pathlib import Path
from orchestrator.utils.config import DJANGO_API_URL, get_environment


def test_api_connectivity() -> bool:
    """Test connectivity to Django API /api endpoint"""
    try:
        # Test the basic /api endpoint for 200 OK
        url = f"{DJANGO_API_URL}/api"
        print(f"Testing connectivity to: {url}")
        
        # Don't use JWT token for basic health check
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            print(f"✅ SUCCESS: {url} returned 200 OK")
            print(f"   Response: {response.text[:100]}...")
            return True
        elif response.status_code == 401:
            print(f"✅ API IS RUNNING: {url} returned 401 (requires auth)")
            print(f"   This means the Django server is running correctly!")
            print(f"   Response: {response.text[:200]}")
            return True  # Server is up, just needs auth
        else:
            print(f"❌ FAILED: {url} returned {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            return False
            
    except requests.exceptions.ConnectionError:
        print(f"❌ CONNECTION ERROR: Cannot connect to {DJANGO_API_URL}")
        print("   Make sure the Django server is running at localhost:8000")
        return False
    except requests.exceptions.Timeout:
        print(f"❌ TIMEOUT: Request to {DJANGO_API_URL} timed out")
        return False
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: {e}")
        return False


def test_api_with_auth() -> bool:
    """Test an authenticated endpoint to verify JWT token works"""
    try:
        # Import JWT token from config system
        try:
            from orchestrator.utils.config import DJANGO_JWT_TOKEN
            jwt_token = DJANGO_JWT_TOKEN
        except ValueError as e:
            print(f"⚠️  NO JWT TOKEN: {e}")
            print(f"   To test authenticated endpoints, set DJANGO_JWT_TOKEN in your environment config")
            return False
            
        url = f"{DJANGO_API_URL}/api/reports/"
        print(f"\nTesting authenticated endpoint: {url}")
        
        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "Content-Type": "application/json"
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            print(f"✅ SUCCESS: Authenticated request returned 200 OK")
            data = response.json()
            if isinstance(data, dict) and "data" in data:
                print(f"   Found {len(data.get('data', []))} reports")
            return True
        elif response.status_code == 401:
            print(f"❌ AUTHENTICATION FAILED: Check DJANGO_JWT_TOKEN")
            print(f"   The token may be invalid or expired")
            return False
        else:
            print(f"❌ FAILED: Authenticated request returned {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            return False
            
    except Exception as e:
        print(f"❌ AUTH TEST ERROR: {e}")
        return False


def check_environment_setup():
    """Check if environment variables are properly configured"""
    env = get_environment()
    print("Environment Configuration Check:")
    print("-" * 40)
    print(f"Current environment: {env.value}")
    
    # Check which config files exist
    root_dir = Path(__file__).parent
    config_files = {
        ".env": root_dir / ".env",
        f".env.{env.value}": root_dir / f".env.{env.value}",
    }
    
    print("\nConfiguration files:")
    for file_name, file_path in config_files.items():
        if file_path.exists():
            print(f"✅ {file_name}: EXISTS")
        else:
            print(f"❌ {file_name}: NOT FOUND")
    
    # Check required environment variables using the config system approach
    config_checks = [
        ("DJANGO_API_URL", DJANGO_API_URL, False),  # Not sensitive
    ]
    
    # Try to import sensitive config values and handle missing ones gracefully
    sensitive_checks = []
    for var_name in ["DJANGO_JWT_TOKEN", "EXTERNAL_API_URL", "EXTERNAL_API_KEY"]:
        try:
            if var_name == "DJANGO_JWT_TOKEN":
                from orchestrator.utils.config import DJANGO_JWT_TOKEN
                sensitive_checks.append((var_name, DJANGO_JWT_TOKEN, True))
            elif var_name == "EXTERNAL_API_URL":
                from orchestrator.utils.config import EXTERNAL_API_URL
                sensitive_checks.append((var_name, EXTERNAL_API_URL, False))
            elif var_name == "EXTERNAL_API_KEY":
                from orchestrator.utils.config import EXTERNAL_API_KEY
                sensitive_checks.append((var_name, EXTERNAL_API_KEY, True))
        except ValueError:
            sensitive_checks.append((var_name, None, True))
    
    all_checks = config_checks + sensitive_checks
    all_good = True
    
    print("\nEnvironment variables:")
    for var_name, var_value, is_sensitive in all_checks:
        if var_value:
            # Mask sensitive values
            if is_sensitive:
                display_value = f"{var_value[:8]}..." if len(var_value) > 8 else "***"
            else:
                display_value = var_value
            print(f"✅ {var_name}: {display_value}")
        else:
            print(f"❌ {var_name}: NOT SET")
            all_good = False
    
    if not all_good:
        print(f"\n💡 SETUP GUIDE for {env.value.upper()} environment:")
        print(f"   1. Copy env.{env.value}.example to .env.{env.value}")
        print(f"   2. Edit .env.{env.value} with your actual credentials")
        print(f"   3. Set APP_ENV={env.value} in your shell")
        print(f"   4. Run this test again")
        print(f"\n   Example:")
        print(f"   cp env.{env.value}.example .env.{env.value}")
        print(f"   export APP_ENV={env.value}")
    
    return all_good


if __name__ == "__main__":
    print("Django API Connectivity Test")
    print("=" * 40)
    print(f"Target URL: {DJANGO_API_URL}")
    print()
    
    # Check environment setup first
    env_ok = check_environment_setup()
    print()
    
    # Test basic connectivity
    basic_success = test_api_connectivity()
    
    # Test authenticated endpoint if basic test passes
    auth_success = False
    if basic_success:
        auth_success = test_api_with_auth()
    
    print("\n" + "=" * 40)
    if basic_success and auth_success:
        print("✅ ALL TESTS PASSED - Django API is accessible and working")
        sys.exit(0)
    elif basic_success:
        print("⚠️  PARTIAL SUCCESS - Django server is running")
        if not env_ok:
            print("   Setup your environment-specific config file to test authenticated endpoints")
        sys.exit(0)  # Server is running, which is what we mainly wanted to test
    else:
        print("❌ TESTS FAILED - Django API is not accessible")
        sys.exit(1) 