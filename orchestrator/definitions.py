from dagster import Definitions
from .assets.report_discovery import report_cron_schedules
from .assets.report_processing import (
    report_data,
    external_job_submission,
    local_job_record,
    job_completion_status,
    results_storage,
)
from .jobs.report_pipeline import process_report_job
from .sensors.report_sensor import report_cron_sensor, daily_report_discovery_sensor
from .resources.api_client import DjangoAPIClient
from .resources.azure_storage import AzureStorageResource
from .utils.config import (
    DJANGO_API_URL,
    DJANGO_JWT_TOKEN,
    AZURE_CONNECTION_STRING,
    AZURE_CONTAINER_NAME
)

defs = Definitions(
    assets=[
        report_cron_schedules,
        report_data,
        external_job_submission,
        local_job_record,
        job_completion_status,
        results_storage,
    ],
    jobs=[process_report_job],
    sensors=[report_cron_sensor, daily_report_discovery_sensor],
    resources={
        "api_client": DjangoAPIClient(
            base_url=DJANGO_API_URL,
            api_token=DJANGO_JWT_TOKEN
        ),
        "azure_storage": AzureStorageResource(
            connection_string=AZURE_CONNECTION_STRING,
            container_name=AZURE_CONTAINER_NAME,
        ),
    },
)
