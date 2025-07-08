from dagster import Definitions, DefaultSensorStatus, DefaultScheduleStatus
from .assets.report_discovery import report_cron_schedules
from .assets.report_processing import (
    report_data,
    external_job_submission,
    local_job_record,
    job_completion_status,
    results_storage,
)
from .jobs.report_pipeline import process_report_job
from .jobs.individual_report_jobs import INDIVIDUAL_REPORT_JOBS
from .sensors.report_sensor import daily_report_discovery_sensor, report_cron_sensor
from .schedules.dynamic_schedules import unified_report_schedule
from .resources.api_client import DjangoAPIClient
from .resources.fabric_storage import FabricStorageResource
from .resources.external_api import FireantAPIClient
from .utils.config import (
    DJANGO_API_URL,
    DJANGO_JWT_TOKEN,
    AZURE_TENANT_ID,
    AZURE_CLIENT_ID,
    AZURE_CLIENT_SECRET,
    AZURE_ACCOUNT_NAME,
    FABRIC_WORKSPACE_ID,
    FIREANT_API_URL,
    FIREANT_API_KEY,
    SchedulingApproach,
    get_scheduling_approach,
)

# Determine which scheduling approach to use
scheduling_approach = get_scheduling_approach()

# Configure sensors and schedules based on approach
if scheduling_approach == SchedulingApproach.SENSOR:
    active_sensors = [
        daily_report_discovery_sensor,
        report_cron_sensor,
    ]
    active_schedules = [
        unified_report_schedule
    ]
    # Use unified job for sensor approach
    all_jobs = [process_report_job]
    
elif scheduling_approach == SchedulingApproach.PIPELINE:
    active_sensors = [
        daily_report_discovery_sensor,
        report_cron_sensor,
    ]
    active_schedules = [
        unified_report_schedule
    ]
    # Use individual jobs for pipeline approach
    all_jobs = [process_report_job] + list(INDIVIDUAL_REPORT_JOBS.values())
    
else:  # SchedulingApproach.SCHEDULE
    active_sensors = [
        daily_report_discovery_sensor,
        report_cron_sensor,
    ]
    active_schedules = [
        unified_report_schedule
    ]
    # Use unified job for schedule approach
    all_jobs = [process_report_job]

defs = Definitions(
    assets=[
        report_cron_schedules,
        report_data,
        external_job_submission,
        local_job_record,
        job_completion_status,
        results_storage,
    ],
    jobs=all_jobs,
    sensors=active_sensors,
    schedules=active_schedules,
    resources={
        "api_client": DjangoAPIClient(
            base_url=DJANGO_API_URL, api_token=DJANGO_JWT_TOKEN
        ),
        "fabric_storage": FabricStorageResource(
            tenant_id=AZURE_TENANT_ID,
            client_id=AZURE_CLIENT_ID,
            client_secret=AZURE_CLIENT_SECRET,
            account_name=AZURE_ACCOUNT_NAME,
            workspace_id=FABRIC_WORKSPACE_ID,
        ),
        "fireant_api": FireantAPIClient(
            base_url=FIREANT_API_URL,
            api_key=FIREANT_API_KEY,
        ),
    },
)
