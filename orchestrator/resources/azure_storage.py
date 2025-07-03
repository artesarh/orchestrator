from dagster import ConfigurableResource
from azure.storage.blob import BlobServiceClient
from typing import Any


class AzureStorageResource(ConfigurableResource):
    connection_string: str
    container_name: str

    def upload_file(self, file_content: bytes, blob_name: str) -> str:
        """Upload file to Azure Blob Storage"""
        blob_service_client = BlobServiceClient.from_connection_string(
            self.connection_string
        )
        blob_client = blob_service_client.get_blob_client(
            container=self.container_name, blob=blob_name
        )

        blob_client.upload_blob(file_content, overwrite=True)
        return blob_client.url
