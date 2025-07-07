from dagster import asset, AssetIn, Config, get_dagster_logger, OpExecutionContext
from ..resources.api_client import DjangoAPIClient
from ..resources.azure_storage import AzureStorageResource
from ..utils.schema_transformer import transform_report_schema
from ..utils.external_api import FireantAPIClient as ExternalAPIClient
from typing import Dict, Any
import time
import json


class ReportProcessingConfig(Config):
    report_id: int
    modifier_id: int
    external_api_url: str
    external_api_key: str
    max_poll_attempts: int = 120  # 2 hours with 1-minute intervals
    poll_interval_seconds: int = 60


@asset(group_name="processing", compute_kind="api_call")
def report_data(
    context: OpExecutionContext,
    config: ReportProcessingConfig,
    api_client: DjangoAPIClient,
) -> Dict[str, Any]:
    """Fetch and transform report data from Django API"""
    logger = get_dagster_logger()

    # Get raw data from API
    raw_data = api_client.get_report_with_modifier(
        config.report_id, config.modifier_id)

    # Transform schema
    transformed_data = transform_report_schema(raw_data)

    logger.info(f"Retrieved and transformed data for report {
                config.report_id}")
    return transformed_data


@asset(group_name="processing", compute_kind="external_api", deps=[report_data])
def external_job_submission(
    context: OpExecutionContext,
    config: ReportProcessingConfig,
    report_data: Dict[str, Any],
) -> Dict[str, Any]:
    """Submit job to external API and get job ID"""
    logger = get_dagster_logger()

    external_client = ExternalAPIClient(
        config.external_api_url, config.external_api_key
    )

    # Submit to external API
    external_job_id = external_client.submit_job(report_data)

    logger.info(f"Submitted job to external API, got job_id: {
                external_job_id}")

    return {
        "external_job_id": external_job_id,
        "status": "submitted",
        "submitted_at": time.time(),
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

    job_data = {
        "report": config.report_id,
        "fireant_jobid": external_job_submission["external_job_id"],
        "status": "submitted",  # Add this field to your Job model
    }

    local_job = api_client.create_job(job_data)
    logger.info(f"Created local job record: {local_job['id']}")

    return {**local_job, "external_job_id": external_job_submission["external_job_id"]}


@asset(group_name="processing", compute_kind="polling", deps=[local_job_record])
def job_completion_status(
    context: OpExecutionContext,
    config: ReportProcessingConfig,
    api_client: DjangoAPIClient,
    local_job_record: Dict[str, Any],
) -> Dict[str, Any]:
    """Poll external API until job is complete"""
    logger = get_dagster_logger()

    external_client = ExternalAPIClient(
        config.external_api_url, config.external_api_key
    )

    external_job_id = local_job_record["external_job_id"]
    local_job_id = local_job_record["id"]

    for attempt in range(config.max_poll_attempts):
        status_response = external_client.check_job_status(external_job_id)
        status = status_response.get("status")

        # Update local job status
        api_client.update_job(local_job_id, {"status": status})

        if status in ["completed", "failed", "error"]:
            logger.info(
                f"Job {external_job_id} finished with status: {status}")
            return {
                "external_job_id": external_job_id,
                "local_job_id": local_job_id,
                "final_status": status,
                "status_response": status_response,
            }

        logger.info(
            f"Job {external_job_id} still running (attempt {attempt + 1})")
        time.sleep(config.poll_interval_seconds)

    # Timeout
    api_client.update_job(local_job_id, {"status": "timeout"})
    raise Exception(
        f"Job {external_job_id} timed out after {
            config.max_poll_attempts} attempts"
    )


@asset(group_name="processing", compute_kind="storage", deps=[job_completion_status])
def results_storage(
    context: OpExecutionContext,
    config: ReportProcessingConfig,
    api_client: DjangoAPIClient,
    azure_storage: AzureStorageResource,
    job_completion_status: Dict[str, Any],
) -> Dict[str, Any]:
    """Download results and save to Azure lakehouse"""
    logger = get_dagster_logger()

    if job_completion_status["final_status"] != "completed":
        raise Exception(
            f"Job failed with status: {job_completion_status['final_status']}"
        )

    external_client = ExternalAPIClient(
        config.external_api_url, config.external_api_key
    )

    # Download results
    results_data = external_client.download_results(
        job_completion_status["external_job_id"]
    )

    # Generate blob name
    blob_name = f"reports/{config.report_id}/results_{
        job_completion_status['external_job_id']}.json"

    # Upload to Azure
    blob_url = azure_storage.upload_file(results_data, blob_name)

    # Update local job with completion status
    api_client.update_job(
        job_completion_status["local_job_id"],
        {
            "status": "completed",
            "results_url": blob_url,  # Add this field to your Job model
        },
    )

    logger.info(f"Results saved to: {blob_url}")

    return {
        "blob_url": blob_url,
        "blob_name": blob_name,
        "job_id": job_completion_status["local_job_id"],
    }
