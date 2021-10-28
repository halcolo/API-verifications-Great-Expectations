import os
from src.utils.utils import get_project_path

def upload_blob(bucket_name, source_file_name, destination_blob_name, gcp_project) -> str:
    """upload blob to GCS

    Args:
        bucket_name (str): Name of bucket destination, verify credential have access to the bucket
        source_file_name (str): Path to local file 
        destination_blob_name (str): name of blob in GCS
        gcp_project (str): Name of GCP project
    """
    from google.oauth2 import service_account
    from google.cloud import storage

    credentials = service_account.Credentials.from_service_account_file(f'{get_project_path()}/credentials/credentials.json')
    project_id = gcp_project
    storage_client = storage.Client(credentials= credentials,project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    # Removing local file
    os.remove(source_file_name)

    return("File {} uploaded to {}.".format(source_file_name, destination_blob_name))