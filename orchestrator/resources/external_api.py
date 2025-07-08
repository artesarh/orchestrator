from dagster import ConfigurableResource, get_dagster_logger
import requests
from typing import Dict, Any, Optional
from enum import Enum, auto


class JobType(Enum):
    """Valid job types for the external API"""
    GEO = "geo"
    RING = "ring"
    BOX = "box"


class FireantAPIClient(ConfigurableResource):
    base_url: str
    api_key: str
    timeout: int = 30

    def submit_job(self, job_data: Dict[str, Any], job_type: str) -> str:
        """Submit job to external service and return job_id"""
        headers = {"Authorization": f"Bearer {self.api_key}"}

        try:
            job_type_enum = JobType(job_type.lower())
        except ValueError:
            raise ValueError(f"Invalid job type '{job_type}'. Must be one of: {[t.value for t in JobType]}")

        # Map job type to endpoint
        job_url_map = {
            JobType.GEO: "job_exposure_geo_agg",
            JobType.RING: "job_exposure_ring_agg",
            JobType.BOX: "job_exposure_box_agg"
        }

        job_url_submit = job_url_map[job_type_enum]

        response = requests.post(
            f"{self.base_url}/{job_url_submit}", 
            json=job_data, 
            headers=headers,
            timeout=self.timeout
        )
        response.raise_for_status()
        return response.json()["job_id"]

    def check_job_status(self, job_id: str) -> Dict[str, Any]:
        """Check job status on external service"""
        headers = {"Authorization": f"Bearer {self.api_key}"}
        response = requests.get(
            f"{self.base_url}/get_job_status/{job_id}?is_get_results=false&is_newline=true",
            headers=headers,
            timeout=self.timeout
        )
        response.raise_for_status()
        return response.json()

    def download_results(self, job_id: str) -> bytes:
        """Download job results"""
        headers = {"Authorization": f"Bearer {self.api_key}"}
        response = requests.get(
            f"{self.base_url}/jobs/results/{job_id}", 
            headers=headers,
            timeout=self.timeout
        )
        response.raise_for_status()
        return response.content 