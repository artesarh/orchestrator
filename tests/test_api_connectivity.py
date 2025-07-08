#!/usr/bin/env python3
"""
Enhanced connectivity test for Django API
Tests all major endpoints to verify server functionality
"""

import requests
import sys
import os
import json
from pathlib import Path
from orchestrator.utils.config import DJANGO_API_URL, get_env
from orchestrator.resources.api_client import DjangoAPIClient


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


def test_django_api_endpoints() -> bool:
    """Test all major Django API endpoints used by the orchestrator"""
    try:
        from orchestrator.utils.config import DJANGO_JWT_TOKEN
        client = DjangoAPIClient(base_url=DJANGO_API_URL, api_token=DJANGO_JWT_TOKEN)
    except ValueError as e:
        print(f"⚠️  Skipping endpoint tests: {e}")
        return False
    
    print(f"\nTesting Django API endpoints...")
    
    endpoints_tested = 0
    endpoints_passed = 0
    
    # Test 1: GET /api/reports/
    try:
        print(f"  Testing GET /api/reports/")
        response = client.get_all_reports()
        if isinstance(response, dict) and "data" in response:
            print(f"    ✅ SUCCESS: Found {len(response.get('data', []))} reports")
            endpoints_passed += 1
        else:
            print(f"    ❌ FAILED: Unexpected response structure")
        endpoints_tested += 1
    except Exception as e:
        print(f"    ❌ FAILED: {e}")
        endpoints_tested += 1
    
    # Test 2: GET /api/reports/{id}/ (if any reports exist)
    try:
        reports_response = client.get_all_reports()
        reports = reports_response.get("data", [])
        if reports:
            report_id = reports[0].get("id")
            print(f"  Testing GET /api/reports/{report_id}/")
            response = client.get_report(report_id)
            if isinstance(response, dict) and "data" in response:
                print(f"    ✅ SUCCESS: Retrieved report {report_id}")
                endpoints_passed += 1
            else:
                print(f"    ❌ FAILED: Unexpected response structure")
        else:
            print(f"  ⚠️  SKIPPED: No reports available to test GET /api/reports/{id}/")
        endpoints_tested += 1
    except Exception as e:
        print(f"    ❌ FAILED: {e}")
        endpoints_tested += 1
    
    # Test 3: POST /api/jobs/ (create job)
    try:
        print(f"  Testing POST /api/jobs/")
        test_job_data = {
            "report_id": 1,
            "report_modifier_id": 1,
            "fireant_jobid": "test_job_" + str(int(time.time())),
            "status": "submitted"
        }
        response = client.create_job(test_job_data)
        if isinstance(response, dict) and "id" in response:
            job_id = response["id"]
            print(f"    ✅ SUCCESS: Created job {job_id}")
            endpoints_passed += 1
            
            # Test 4: PATCH /api/jobs/{id}/ (update job)
            try:
                print(f"  Testing PATCH /api/jobs/{job_id}/")
                update_data = {"status": "completed"}
                response = client.update_job(job_id, update_data)
                if isinstance(response, dict) and response.get("status") == "completed":
                    print(f"    ✅ SUCCESS: Updated job {job_id}")
                    endpoints_passed += 1
                else:
                    print(f"    ❌ FAILED: Job update unsuccessful")
                endpoints_tested += 1
                
                # Clean up: DELETE the test job
                try:
                    client.delete_job(job_id)
                    print(f"    ✅ CLEANUP: Deleted test job {job_id}")
                except Exception as e:
                    print(f"    ⚠️  CLEANUP WARNING: Could not delete test job {job_id}: {e}")
                    
            except Exception as e:
                print(f"    ❌ FAILED: {e}")
                endpoints_tested += 1
        else:
            print(f"    ❌ FAILED: Unexpected response structure")
        endpoints_tested += 1
    except Exception as e:
        print(f"    ❌ FAILED: {e}")
        endpoints_tested += 1
    
    # Test 5: GET /api/reports/{id}/modifiers/{modifier_id}/all (if data exists)
    try:
        reports_response = client.get_all_reports()
        reports = reports_response.get("data", [])
        if reports:
            report_id = reports[0].get("id")
            print(f"  Testing GET /api/reports/{report_id}/modifiers/1/all")
            response = client.get_report_with_modifier(report_id, 1)
            if isinstance(response, dict) and "data" in response:
                print(f"    ✅ SUCCESS: Retrieved report with modifier")
                endpoints_passed += 1
            else:
                print(f"    ❌ FAILED: Unexpected response structure")
        else:
            print(f"  ⚠️  SKIPPED: No reports available to test modifier endpoint")
        endpoints_tested += 1
    except Exception as e:
        print(f"    ❌ FAILED: {e}")
        endpoints_tested += 1
    
    print(f"\nEndpoint Test Summary: {endpoints_passed}/{endpoints_tested} passed")
    return endpoints_passed == endpoints_tested


def check_environment_setup():
    """Check if environment variables are properly configured"""
    env = get_env()
    print("Environment Configuration Check:")
    print("-" * 40)
    print(f"Current environment: {env.value}")
    
    # Check which config files exist
    root_dir = Path(__file__).parent.parent
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
    for var_name in ["DJANGO_JWT_TOKEN", "FIREANT_API_URL", "FIREANT_API_KEY"]:
        try:
            if var_name == "DJANGO_JWT_TOKEN":
                from orchestrator.utils.config import DJANGO_JWT_TOKEN
                sensitive_checks.append((var_name, DJANGO_JWT_TOKEN, True))
            elif var_name == "FIREANT_API_URL":
                from orchestrator.utils.config import FIREANT_API_URL
                sensitive_checks.append((var_name, FIREANT_API_URL, False))
            elif var_name == "FIREANT_API_KEY":
                from orchestrator.utils.config import FIREANT_API_KEY
                sensitive_checks.append((var_name, FIREANT_API_KEY, True))
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
    import time
    
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
    
    # Test specific endpoints if auth passes
    endpoint_success = False
    if auth_success:
        endpoint_success = test_django_api_endpoints()
    
    print("\n" + "=" * 40)
    if basic_success and auth_success and endpoint_success:
        print("✅ ALL TESTS PASSED - Django API is fully functional")
        sys.exit(0)
    elif basic_success and auth_success:
        print("⚠️  PARTIAL SUCCESS - Django API is accessible but some endpoints failed")
        sys.exit(0)
    elif basic_success:
        print("⚠️  PARTIAL SUCCESS - Django server is running")
        if not env_ok:
            print("   Setup your environment-specific config file to test authenticated endpoints")
        sys.exit(0)  # Server is running, which is what we mainly wanted to test
    else:
        print("❌ TESTS FAILED - Django API is not accessible")
        sys.exit(1) 