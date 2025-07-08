from dagster import (
    job,
    Config,
    get_dagster_logger,
    StaticPartitionsDefinition,
    DynamicPartitionsDefinition,
    MultiPartitionsDefinition,
    MultiPartitionKey,
)
from ..assets.report_processing import (
    report_data,
    external_job_submission,
    local_job_record,
    job_completion_status,
    results_storage,
)
from ..utils.config import CONFIG
from typing import Dict, Any, List
import os
import json


class IndividualReportConfig(Config):
    """Config for individual report jobs"""
    run_date: str
    fireant_api_url: str = CONFIG["FIREANT_API_URL"]
    fireant_api_key: str = CONFIG["FIREANT_API_KEY"]


def create_report_job(report_id: int, report_name: str):
    """Create a job for a specific report"""
    
    # Create date-based partitions for this report
    partitions_def = DynamicPartitionsDefinition(name=f"report_{report_id}_dates")
    
    @job(
        name=f"process_report_{report_id}",
        description=f"Process {report_name} (ID: {report_id})",
        partitions_def=partitions_def,
        config=IndividualReportConfig,
        tags={
            "report_id": str(report_id),
            "report_name": report_name,
            "job_type": "individual_report"
        }
    )
    def individual_report_job():
        """Process a single report through the complete pipeline"""
        # Configure the first asset with the report ID
        configured_report_data = report_data.configured(
            {
                "report_id": report_id,
                "run_date": "{{ run_config.run_date }}",
                "fireant_api_url": "{{ run_config.fireant_api_url }}",
                "fireant_api_key": "{{ run_config.fireant_api_key }}",
            },
            name=f"report_{report_id}_data"
        )
        
        # Sequential pipeline execution
        # Step 1: Fetch and transform report data
        data = configured_report_data
        
        # Step 2: Submit job to external API
        submission = external_job_submission(data)
        
        # Step 3: Create local job record
        job_record = local_job_record(submission)
        
        # Step 4: Poll for job completion
        completion_status = job_completion_status(job_record)
        
        # Step 5: Store results
        results_storage(completion_status)
    
    return individual_report_job


def get_reports_from_cache() -> List[Dict[str, Any]]:
    """Get reports from cached discovery data"""
    cache_file = os.path.join(os.getcwd(), ".dagster_reports_cache.json")
    
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r') as f:
                cache_data = json.load(f)
                return cache_data.get("reports", [])
        except Exception as e:
            print(f"Error reading reports cache: {e}")
    
    return []


def generate_individual_report_jobs() -> Dict[str, Any]:
    """Generate individual jobs for each report"""
    reports = get_reports_from_cache()
    jobs = {}
    
    for report in reports:
        report_id = report.get("id")
        report_name = report.get("name", f"Report {report_id}")
        
        if report_id and report.get("cron"):  # Only create jobs for reports with cron schedules
            job_func = create_report_job(report_id, report_name)
            jobs[f"process_report_{report_id}"] = job_func
    
    return jobs


# Cache update utility
def update_reports_cache(reports_data: Dict[str, Any]):
    """Update the reports cache file"""
    cache_file = os.path.join(os.getcwd(), ".dagster_reports_cache.json")
    
    try:
        with open(cache_file, 'w') as f:
            json.dump(reports_data, f, indent=2)
    except Exception as e:
        print(f"Error writing reports cache: {e}")


# Generate jobs based on current cache
INDIVIDUAL_REPORT_JOBS = generate_individual_report_jobs() 