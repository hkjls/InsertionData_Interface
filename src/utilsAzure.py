import io
import os
from azure.storage.blob import ContainerClient
from io import StringIO, BytesIO
import pickle
from dotenv import load_dotenv

load_dotenv()
container_url_with_sas = os.getenv("url")

# Create ContainerClient using SAS URL
container_client = ContainerClient.from_container_url(container_url_with_sas)


def get_Azure_file_csv(blob_name):
    downloaded_blob = container_client.download_blob(blob_name)

    return StringIO(downloaded_blob.content_as_text())


def get_Azure_file_bytes(blob_name):
    try:
        downloaded_blob = container_client.download_blob(blob_name)

        return BytesIO(downloaded_blob.content_as_bytes())
    except:
        return None


def upload_Azure_file_bytes(blob_name, data, overwrite):
    return container_client.upload_blob(name=blob_name, data=data, overwrite=overwrite)


def upload_Azure_file(data, file_path: str, overwrite=True):
    if file_path.endswith(".pkl"):
        buffer = io.BytesIO()
        pickle.dump(data, buffer)
        buffer.seek(0)  # Reset buffer position to the beginning
        upload_Azure_file_bytes(blob_name=file_path, data=buffer, overwrite=overwrite)
    else:
        buffer = io.BytesIO()
        data.to_excel(buffer, index=False, engine="openpyxl")
        buffer.seek(0)
        upload_Azure_file_bytes(blob_name=file_path, data=buffer, overwrite=overwrite)


def upload_Azure_blob(file_content, file_path: str, overwrite=True):

    container_client.upload_blob(name=file_path, data=file_content, overwrite=overwrite)


def rename_file_Azure(old_file_name, new_file_name):
    file = get_Azure_file_bytes(old_file_name)
    upload_Azure_file_bytes(new_file_name, file, overwrite=True)
