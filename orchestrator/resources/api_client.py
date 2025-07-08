from dagster import ConfigurableResource, get_dagster_logger
import requests
from typing import Dict, Any, Optional
from datetime import date
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

    def find_modifier_for_date(
        self, report_id: int, target_date: date
    ) -> Optional[Dict[str, Any]]:
        """Find modifier for report where fx_date and as_at_date match target_date"""

        # The DRF API contains queryable endpoints in a GET request
        url = f"{self.base_url}/api/reports/{report_id}/modifiers/"
        params = {
            "fx_date": target_date.isoformat(),
            "as_at_date": target_date.isoformat(),
        }
        response = requests.get(
            url, headers=self._get_headers(), params=params, timeout=self.timeout
        )
        response.raise_for_status()

        data = response.json()
        # Get modifiers from the data section
        modifiers = data.get("data", {}).get("modifiers", [])

        # Return the first matching modifier (there should typically be only one)
        return modifiers[0] if modifiers else None

    def create_modifier(self, modifier_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new modifier"""
        url = f"{self.base_url}/api/report-modifiers/"
        response = requests.post(
            url, json=modifier_data, headers=self._get_headers(), timeout=self.timeout
        )
        response.raise_for_status()
        return response.json()

    def link_modifier_to_report(
        self, report_id: int, modifier_id: int
    ) -> Dict[str, Any]:
        """Link a modifier to a report"""
        url = f"{self.base_url}/api/link-modifier/single/"
        link_data = {"report_id": report_id, "modifier_id": modifier_id}
        logger = get_dagster_logger()
        logger.info(f"Linking modifier {modifier_id} to report {
                    report_id} via {url}")

        response = requests.post(
            url, json=link_data, headers=self._get_headers(), timeout=self.timeout
        )

        # Log the response for debugging
        logger.info(f"Link response status: {response.status_code}")
        if response.status_code >= 400:
            logger.error(f"Link error response: {response.text}")

        response.raise_for_status()
        return response.json()

    def get_or_create_modifier_for_date(
        self, report_id: int, target_date: date
    ) -> Dict[str, Any]:
        """Get existing modifier or create new one for the target date"""
        logger = get_dagster_logger()

        # First try to find existing modifier linked to this report
        existing_modifier = self.find_modifier_for_date(report_id, target_date)

        if existing_modifier:
            logger.info(
                f"Found existing modifier {existing_modifier['id']} for report {
                    report_id
                } on {target_date}"
            )
            return existing_modifier

        # Create new modifier (without report field - it's linked separately)
        logger.info(f"Creating new modifier for report {
                    report_id} on {target_date}")
        modifier_data = {
            "fx_date": target_date.isoformat(),
            "as_at_date": target_date.isoformat(),
        }

        new_modifier = self.create_modifier(modifier_data)
        modifier_id = new_modifier["id"]

        # Link the modifier to the report
        logger.info(f"Linking modifier {modifier_id} to report {report_id}")
        try:
            link_result = self.link_modifier_to_report(report_id, modifier_id)
            logger.info(
                f"Successfully linked modifier {
                    modifier_id} to report {report_id}"
            )
        except Exception as e:
            logger.error(
                f"Failed to link modifier {
                    modifier_id} to report {report_id}: {e}"
            )
            # Don't raise here - the modifier was created successfully

        return new_modifier

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

    def get_report(self, report_id: int) -> Dict[str, Any]:
        """Get a single report by ID"""
        url = f"{self.base_url}/api/reports/{report_id}/"
        response = requests.get(
            url, headers=self._get_headers(), timeout=self.timeout
        )
        response.raise_for_status()
        return response.json()
