from dagster import get_dagster_logger
import requests
from typing import Dict, Any, Optional
import time


class ExternalAPIClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.api_key = api_key
        self.logger = get_dagster_logger()

    def submit_job(self, job_data: Dict[str, Any]) -> str:
        """Submit job to external service and return job_id"""
        headers = {"Authorization": f"Bearer {self.api_key}"}
        response = requests.post(
            f"{self.base_url}/submit", json=job_data, headers=headers
        )
        response.raise_for_status()
        return response.json()["job_id"]

    def check_job_status(self, job_id: str) -> Dict[str, Any]:
        """Check job status on external service"""
        headers = {"Authorization": f"Bearer {self.api_key}"}
        response = requests.get(
            f"{self.base_url}/jobs/{job_id}/status", headers=headers
        )
        response.raise_for_status()
        return response.json()

    def download_results(self, job_id: str) -> bytes:
        """Download job results"""
        headers = {"Authorization": f"Bearer {self.api_key}"}
        response = requests.get(
            f"{self.base_url}/jobs/{job_id}/results", headers=headers
        )
        response.raise_for_status()
        return response.content
