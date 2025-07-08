from dagster import ConfigurableResource, get_dagster_logger
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
from typing import Any
import json


class FabricStorageResource(ConfigurableResource):
    tenant_id: str
    client_id: str
    client_secret: str
    account_name: str
    workspace_id: str
    data_path: str = "report_results"  # Default path in the lakehouse

    def _get_service_client(self) -> DataLakeServiceClient:
        """Get authenticated Data Lake service client"""
        account_url = f"https://{self.account_name}.dfs.fabric.microsoft.com"
        token_credential = ClientSecretCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret
        )
        return DataLakeServiceClient(account_url, credential=token_credential)

    def upload_json(self, data: Any, file_name: str) -> str:
        """Upload JSON data to Fabric lakehouse"""
        logger = get_dagster_logger()
        
        try:
            service_client = self._get_service_client()
            
            # Get file system client for the workspace
            file_system_client = service_client.get_file_system_client(self.workspace_id)

            # Ensure directory exists
            dir_client = file_system_client.get_directory_client(self.data_path)
            try:
                dir_client.create_directory()
            except Exception as e:
                logger.debug(f"Directory exists or error: {e}")

            # Create file client and upload
            file_client = dir_client.get_file_client(file_name)
            
            # Convert data to JSON bytes
            json_bytes = json.dumps(data, indent=2).encode('utf-8')
            
            # Upload the data
            file_client.upload_data(json_bytes, overwrite=True)
            
            logger.info(f'File {file_name} uploaded to {self.data_path} successfully')
            
            # Return the full path
            return f"{self.data_path}/{file_name}"
            
        except Exception as e:
            logger.error(f"Failed to upload {file_name}: {e}")
            raise 