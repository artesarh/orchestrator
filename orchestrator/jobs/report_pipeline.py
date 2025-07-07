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
    config={
        "ops": {
            "report_data": {
                "config": {
                    "report_id": {"env": "DAGSTER_REPORT_ID"},
                    "modifier_id": {"env": "DAGSTER_MODIFIER_ID"},
                    "external_api_url": {"env": "EXTERNAL_API_URL"},
                    "external_api_key": {"env": "EXTERNAL_API_KEY"},
                }
            }
        }
    },
)
def process_report_job():
    """Process a single report through the complete pipeline"""
    results_storage(
        job_completion_status(local_job_record(
            external_job_submission(report_data())))
    )
