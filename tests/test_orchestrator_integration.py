#!/usr/bin/env python3
"""
Integration test for orchestrator components with live Django API
Tests the fixes we made for:
1. API response parsing (data field with custom format)
2. Modifier management and linking
3. Schema transformation
"""

import json
from datetime import date, datetime
from orchestrator.resources.api_client import DjangoAPIClient
from orchestrator.utils.schema_transformer import transform_report_schema
from orchestrator.utils.config import DJANGO_API_URL, DJANGO_JWT_TOKEN


def test_reports_discovery():
    """Test getting reports from Django API"""
    print("=== Testing Reports Discovery ===")
    
    client = DjangoAPIClient(
        base_url=DJANGO_API_URL,
        api_token=DJANGO_JWT_TOKEN
    )
    
    try:
        # Test the API endpoint
        reports_response = client.get_all_reports()
        print(f"✓ API call successful: {DJANGO_API_URL}/api/reports/")
        print(f"✓ Response keys: {list(reports_response.keys())}")
        
        # Test the correct parsing (should use 'data' for your custom format)
        reports = reports_response.get("data", [])
        print(f"✓ Found {len(reports)} reports in 'data' field")
        
        if reports:
            first_report = reports[0]
            # Extract ID from URL
            report_url = first_report.get("url", "")
            if "/reports/" in report_url:
                report_id = int(report_url.strip("/").split("/")[-1])
            else:
                report_id = None
                
            print(f"✓ Sample report URL: {report_url}")
            print(f"✓ Sample report ID: {report_id}")
            print(f"✓ Sample report name: {first_report.get('name')}")
            print(f"✓ Sample cron schedule: {first_report.get('cron')}")
            
            # Return report with extracted ID
            return {**first_report, "id": report_id}
        else:
            print("⚠️  No reports found - create some reports in Django admin")
            return None
            
    except Exception as e:
        print(f"✗ Error: {e}")
        return None


def test_modifier_management(report_id: int):
    """Test modifier finding and creation"""
    print(f"\n=== Testing Modifier Management for Report {report_id} ===")
    
    client = DjangoAPIClient(
        base_url=DJANGO_API_URL,
        api_token=DJANGO_JWT_TOKEN
    )
    
    test_date = date.today()
    
    try:
        # Test the complete modifier workflow
        print(f"Looking for modifier with fx_date={test_date} and as_at_date={test_date}")
        
        modifier = client.get_or_create_modifier_for_date(report_id, test_date)
        
        print(f"✓ Got modifier ID: {modifier['id']}")
        print(f"✓ fx_date: {modifier.get('fx_date')}")
        print(f"✓ as_at_date: {modifier.get('as_at_date')}")
        
        return modifier
        
    except Exception as e:
        print(f"✗ Modifier management error: {e}")
        return None


def test_complete_report_data(report_id: int, modifier_id: int):
    """Test getting complete report data and transformation"""
    print(f"\n=== Testing Complete Report Data (Report {report_id}, Modifier {modifier_id}) ===")
    
    client = DjangoAPIClient(
        base_url=DJANGO_API_URL,
        api_token=DJANGO_JWT_TOKEN
    )
    
    try:
        # Test the complete data endpoint
        raw_data = client.get_report_with_modifier(report_id, modifier_id)
        
        print(f"✓ Got complete report data")
        print(f"✓ Top-level keys: {list(raw_data.keys())}")
        
        if "data" in raw_data:
            data_keys = list(raw_data["data"].keys())
            print(f"✓ Data section keys: {data_keys}")
        
        # Test schema transformation
        print("\n--- Testing Schema Transformation ---")
        transformed_data = transform_report_schema(raw_data)
        
        print(f"✓ Schema transformation successful")
        print(f"✓ Transformed keys: {list(transformed_data.keys())}")
        
        # Check for expected transformations
        events_keys = [k for k in transformed_data.keys() if k in ['geos', 'rings', 'boxes', 'events']]
        if events_keys:
            print(f"✓ Events renamed to: {events_keys[0]}")
        
        if 'fx_date' in transformed_data and 'as_at_date' in transformed_data:
            print(f"✓ Has fx_date: {transformed_data['fx_date']}")
            print(f"✓ Has as_at_date: {transformed_data['as_at_date']}")
        
        if 'id' not in transformed_data and 'cron' not in transformed_data:
            print("✓ Correctly excluded 'id' and 'cron' fields")
        
        # Test JSON serialization for API
        json_payload = json.dumps(transformed_data)
        print(f"✓ JSON serialization successful ({len(json_payload)} chars)")
        
        return transformed_data
        
    except Exception as e:
        print(f"✗ Complete data test error: {e}")
        return None


def test_modifier_filtering():
    """Test the modifier filtering endpoint directly"""
    print(f"\n=== Testing Modifier Filtering Endpoint ===")
    
    import requests
    
    test_date = date.today()
    url = f"{DJANGO_API_URL}/api/report-modifiers/"
    params = {
        "fx_date": test_date.isoformat(),
        "as_at_date": test_date.isoformat()
    }
    headers = {
        "Authorization": f"Bearer {DJANGO_JWT_TOKEN}",
        "Content-Type": "application/json",
    }
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Check what format this endpoint uses
        if "results" in data:
            modifiers = data.get("results", [])
            print(f"✓ Modifier endpoint uses standard DRF format with 'results'")
        elif "data" in data:
            modifiers = data.get("data", [])
            print(f"✓ Modifier endpoint uses custom format with 'data'")
        else:
            print(f"✗ Unknown response format: {list(data.keys())}")
            return None
        
        print(f"✓ Filtering endpoint works: found {len(modifiers)} modifiers for {test_date}")
        print(f"✓ Request URL: {response.url}")
        
        if modifiers:
            sample = modifiers[0]
            print(f"✓ Sample modifier: ID={sample.get('id')}, fx_date={sample.get('fx_date')}")
        
        return modifiers
        
    except Exception as e:
        print(f"✗ Filtering test error: {e}")
        return None


def main():
    """Run all integration tests"""
    print("🚀 Starting Orchestrator Integration Tests")
    print(f"Django API URL: {DJANGO_API_URL}")
    print(f"Using token: {DJANGO_JWT_TOKEN[:20]}...")
    
    # Test 1: Reports discovery
    sample_report = test_reports_discovery()
    
    if not sample_report or not sample_report.get("id"):
        print("\n❌ Cannot continue without valid reports")
        return
    
    report_id = sample_report["id"]
    
    # Test 2: Modifier filtering
    test_modifier_filtering()
    
    # Test 3: Modifier management
    modifier = test_modifier_management(report_id)
    
    if not modifier:
        print("\n❌ Cannot continue without modifier")
        return
    
    # Test 4: Complete workflow
    test_complete_report_data(report_id, modifier["id"])
    
    print("\n🎉 All integration tests completed!")
    print("\nNext steps:")
    print("1. Test the sensor by running: dagster dev")
    print("2. Check the Dagster UI at http://localhost:3000")
    print("3. Manually trigger the report_cron_schedules asset")
    print("4. Check if the sensor picks up and schedules reports")


if __name__ == "__main__":
    main() 