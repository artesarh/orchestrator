from dagster import ConfigurableResource, get_dagster_logger
import requests
from typing import Dict, Any, Optional
import time


class DjangoAPIClient(ConfigurableResource):
    base_url: str
    api_token: str
    timeout: int = 30

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }

    def get_report_with_modifier(
        self, report_id: int, modifier_id: int
    ) -> Dict[str, Any]:
        """Call /reports/{id}/modifiers/{modifier_id}/all endpoint"""
        url = f"{self.base_url}/api/reports/{report_id}/modifiers/{modifier_id}/all"
        response = requests.get(
            url, headers=self._get_headers(), timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def get_all_reports(self) -> Dict[str, Any]:
        """Get all reports to check cron schedules"""
        url = f"{self.base_url}/api/reports/"
        response = requests.get(
            url, headers=self._get_headers(), timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def create_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """POST to /jobs/ endpoint"""
        url = f"{self.base_url}/api/jobs/"
        response = requests.post(
            url, json=job_data, headers=self._get_headers(), timeout=self.timeout
        )
        response.raise_for_status()
        return response.json()

    def update_job(self, job_id: int, update_data: Dict[str, Any]) -> Dict[str, Any]:
        """PATCH /jobs/{id}/ endpoint"""
        url = f"{self.base_url}/api/jobs/{job_id}/"
        response = requests.patch(
            url, json=update_data, headers=self._get_headers(), timeout=self.timeout
        )
        response.raise_for_status()
        return response.json()

    def delete_job(self, job_id: int) -> None:
        """DELETE /jobs/{id}/ endpoint - primarily for test cleanup"""
        url = f"{self.base_url}/api/jobs/{job_id}/"
        response = requests.delete(
            url, headers=self._get_headers(), timeout=self.timeout
        )
        response.raise_for_status()
        # DELETE typically returns 204 No Content, so no JSON to parse
