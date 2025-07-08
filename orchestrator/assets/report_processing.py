from dagster import asset, AssetIn, Config, get_dagster_logger, OpExecutionContext
from ..resources.api_client import DjangoAPIClient
from ..resources.external_api import FireantAPIClient
from ..utils.schema_transformer import transform_report_schema
from ..utils.config import MAX_POLL_ATTEMPTS, POLL_INTERVAL_SECONDS
from typing import Dict, Any
from datetime import date, datetime
import time
import json
import requests
from ..resources.fabric_storage import FabricStorageResource


class ReportProcessingConfig(Config):
    report_id: int
    run_date: str  # Changed from modifier_id to run_date
    fireant_api_url: str
    fireant_api_key: str


@asset(group_name="processing", compute_kind="api_call")
def report_data(
    context: OpExecutionContext,
    config: ReportProcessingConfig,
    api_client: DjangoAPIClient,
) -> Dict[str, Any]:
    """Fetch and transform report data from Django API"""
    logger = get_dagster_logger()

    # 1. Get report and validate it has event group
    try:
        report_response = api_client.get_report(config.report_id)
        report = report_response.get("data", {})
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            raise ValueError(f"Report {config.report_id} not found")
        raise

    if not report.get("event_group"):
        error_msg = (
            f"Report {config.report_id} ('{report.get('name', 'Unknown')}') has no Event Group associated. "
            "Cannot process report without an event group. Please assign an event group in the Django admin."
        )
        context.add_output_metadata({
            "error": error_msg,
            "report_id": config.report_id,
            "report_name": report.get("name", "Unknown"),
            "event_group_status": "MISSING - USER ACTION REQUIRED",
        })
        raise ValueError(error_msg)

    # 2. Get or create modifier for the run date
    run_date = date.fromisoformat(config.run_date)
    modifier = api_client.get_or_create_modifier_for_date(config.report_id, run_date)
    modifier_id = modifier["id"]

    # 3. Get and transform report data
    raw_data = api_client.get_report_with_modifier(config.report_id, modifier_id)
    transformed_data, event_type = transform_report_schema(raw_data)

    # Add metadata for downstream assets
    context.add_output_metadata({
        "event_type": event_type,
        "report_id": config.report_id,
        "modifier_id": modifier_id,
        "report_name": report.get("name", "Unknown"),
    })

    logger.info(
        f"Processed report {config.report_id} ({report.get('name', 'Unknown')}) "
        f"with modifier {modifier_id} for {run_date}, event type: {event_type}"
    )

    return transformed_data


@asset(group_name="processing", compute_kind="external_api", deps=[report_data])
def external_job_submission(
    context: OpExecutionContext,
    config: ReportProcessingConfig,
    report_data: Dict[str, Any],
) -> Dict[str, Any]:
    """Submit job to Fireant API and get job ID"""
    logger = get_dagster_logger()

    # Get metadata from upstream asset
    upstream_output = context.asset_output_for_input("report_data")
    if not upstream_output or not upstream_output.metadata:
        raise ValueError("Missing metadata from report_data asset")

    event_type = upstream_output.metadata.get("event_type")
    report_name = upstream_output.metadata.get("report_name", "Unknown")
    
    if not event_type:
        raise ValueError("Could not determine event type from report data")

    logger.info(f"Submitting {event_type} job for report '{report_name}'")

    fireant_client = FireantAPIClient(
        config.fireant_api_url, config.fireant_api_key
    )

    # Submit to Fireant API with job type
    fireant_job_id = fireant_client.submit_job(report_data, event_type)

    logger.info(f"Submitted {event_type} job to Fireant API, got job_id: {fireant_job_id}")

    # Add metadata for downstream assets
    context.add_output_metadata({
        "event_type": event_type,
        "report_id": config.report_id,
        "report_name": report_name,
        "fireant_job_id": fireant_job_id,
        "submission_time": datetime.now().isoformat(),
    })

    return {
        "fireant_job_id": fireant_job_id,
        "status": "submitted",
        "submitted_at": time.time(),
        "job_type": event_type,
    }


@asset(group_name="processing", compute_kind="database", deps=[external_job_submission])
def local_job_record(
    context: OpExecutionContext,
    config: ReportProcessingConfig,
    api_client: DjangoAPIClient,
    external_job_submission: Dict[str, Any],
) -> Dict[str, Any]:
    """Create job record in local Django database"""
    logger = get_dagster_logger()

    # Get metadata from upstream assets
    report_data_output = context.asset_output_for_input("report_data")
    if not report_data_output or not report_data_output.metadata:
        raise ValueError("Missing metadata from report_data asset")

    job_output = context.asset_output_for_input("external_job_submission")
    if not job_output or not job_output.metadata:
        raise ValueError("Missing metadata from external_job_submission asset")

    # Get required values from metadata
    modifier_id = report_data_output.metadata.get("modifier_id")
    report_name = report_data_output.metadata.get("report_name", "Unknown")
    fireant_job_id = job_output.metadata.get("fireant_job_id")
    event_type = job_output.metadata.get("event_type")

    if not modifier_id:
        raise ValueError("Missing modifier_id in metadata")
    if not fireant_job_id:
        raise ValueError("Missing fireant_job_id in metadata")

    job_data = {
        "report_id": config.report_id,
        "report_modifier_id": modifier_id,
        "fireant_jobid": fireant_job_id,
        "status": "submitted",
    }

    local_job = api_client.create_job(job_data)
    logger.info(
        f"Created local job record {local_job['id']} for report '{report_name}' "
        f"({event_type} job, modifier: {modifier_id})"
    )

    # Add metadata for downstream assets
    context.add_output_metadata({
        "local_job_id": local_job["id"],
        "report_id": config.report_id,
        "report_name": report_name,
        "modifier_id": modifier_id,
        "fireant_job_id": fireant_job_id,
        "event_type": event_type,
        "creation_time": datetime.now().isoformat(),
    })

    return {
        **local_job,
        "fireant_job_id": fireant_job_id,
        "report_name": report_name,
        "event_type": event_type,
    }


@asset(group_name="processing", compute_kind="polling", deps=[local_job_record])
def job_completion_status(
    context: OpExecutionContext,
    config: ReportProcessingConfig,
    api_client: DjangoAPIClient,
    local_job_record: Dict[str, Any],
) -> Dict[str, Any]:
    """Poll Fireant API until job is complete"""
    logger = get_dagster_logger()

    fireant_client = FireantAPIClient(
        config.fireant_api_url, config.fireant_api_key
    )

    fireant_job_id = local_job_record["fireant_job_id"]
    local_job_id = local_job_record["id"]
    report_name = local_job_record.get("report_name", "Unknown")

    # Map Fireant statuses to our local statuses
    status_mapping = {
        "Running": "running",
        "Failed": "failed",
        "Finished": "completed"
    }

    for attempt in range(MAX_POLL_ATTEMPTS):
        status_response = fireant_client.check_job_status(fireant_job_id)
        
        # Get status from data/status in response
        fireant_status = status_response.get("data", {}).get("status")
        if not fireant_status:
            logger.warning(
                f"No status found in Fireant response for job {fireant_job_id}: {status_response}"
            )
            # Use raw response as fallback
            fireant_status = status_response.get("status", "Unknown")

        # Map Fireant status to our status
        local_status = status_mapping.get(fireant_status, "unknown")
        
        # Always update local job to track activity
        api_client.update_job(local_job_id, {"status": local_status})
        
        logger.info(
            f"Job {fireant_job_id} for report '{report_name}' status: {fireant_status} "
            f"(attempt {attempt + 1}/{MAX_POLL_ATTEMPTS})"
        )

        if fireant_status == "Finished":
            logger.info(f"Job {fireant_job_id} completed successfully")
            return {
                "fireant_job_id": fireant_job_id,
                "local_job_id": local_job_id,
                "final_status": "completed",
                "status_response": status_response,
            }
        elif fireant_status == "Failed":
            error_msg = f"Job {fireant_job_id} failed: {status_response.get('data', {}).get('error', 'Unknown error')}"
            logger.error(error_msg)
            return {
                "fireant_job_id": fireant_job_id,
                "local_job_id": local_job_id,
                "final_status": "failed",
                "status_response": status_response,
                "error": error_msg,
            }

        time.sleep(POLL_INTERVAL_SECONDS)

    # Timeout after max attempts
    timeout_msg = f"Job {fireant_job_id} timed out after {MAX_POLL_ATTEMPTS} attempts"
    logger.error(timeout_msg)
    api_client.update_job(local_job_id, {"status": "timeout"})
    raise Exception(timeout_msg)


@asset(group_name="processing", compute_kind="storage", deps=[job_completion_status])
def results_storage(
    context: OpExecutionContext,
    config: ReportProcessingConfig,
    api_client: DjangoAPIClient,
    fabric_storage: FabricStorageResource,
    job_completion_status: Dict[str, Any],
) -> Dict[str, Any]:
    """Download results and save to Fabric lakehouse"""
    logger = get_dagster_logger()

    if job_completion_status["final_status"] != "completed":
        raise Exception(
            f"Job failed with status: {job_completion_status['final_status']}"
        )

    # Get metadata from upstream assets
    job_output = context.asset_output_for_input("job_completion_status")
    if not job_output or not job_output.metadata:
        raise ValueError("Missing metadata from job_completion_status asset")

    report_name = job_output.metadata.get("report_name", "Unknown")
    fireant_job_id = job_completion_status["fireant_job_id"]
    local_job_id = job_completion_status["local_job_id"]

    # Initialize Fireant client
    fireant_client = FireantAPIClient(
        config.fireant_api_url, config.fireant_api_key
    )

    # Download results as JSON
    logger.info(f"Downloading results for job {fireant_job_id}")
    results_response = fireant_client.download_results(fireant_job_id)
    results_data = results_response.json()

    # Generate file name with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"report_{config.report_id}_{fireant_job_id}_{timestamp}.json"

    # Upload to Fabric lakehouse
    logger.info(f"Uploading results to Fabric lakehouse: {file_name}")
    file_path = fabric_storage.upload_json(results_data, file_name)

    # Update local job with completion status and results location
    api_client.update_job(
        local_job_id,
        {
            "status": "completed",
            "results_url": file_path,
        },
    )

    logger.info(
        f"Results for report '{report_name}' (job {fireant_job_id}) "
        f"saved to Fabric lakehouse: {file_path}"
    )

    # Add metadata for downstream assets
    context.add_output_metadata({
        "report_id": config.report_id,
        "report_name": report_name,
        "fireant_job_id": fireant_job_id,
        "local_job_id": local_job_id,
        "results_file": file_name,
        "results_path": file_path,
        "storage_time": datetime.now().isoformat(),
    })

    return {
        "file_path": file_path,
        "file_name": file_name,
        "job_id": local_job_id,
    }
