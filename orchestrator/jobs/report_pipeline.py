from dagster import (
    job,
    DynamicPartitionsDefinition,
    Config,
    DefaultSensorStatus,
    sensor,
    RunRequest,
    SkipReason,
)
from ..assets.report_processing import (
    report_data,
    external_job_submission,
    local_job_record,
    job_completion_status,
    results_storage,
)
from ..assets.report_discovery import report_cron_schedules


# Dynamic partitions for reports
report_partitions = DynamicPartitionsDefinition(name="reports")


@job(
    partitions_def=report_partitions,
)
def process_report_job():
    """Process a single report through the complete pipeline"""
    # Step 1: Fetch and transform report data
    data = report_data()
    
    # Step 2: Submit job to external API
    submission = external_job_submission(data)
    
    # Step 3: Create local job record
    job_record = local_job_record(submission)
    
    # Step 4: Poll for job completion
    completion_status = job_completion_status(job_record)
    
    # Step 5: Store results
    results_storage(completion_status)
