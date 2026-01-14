import os
import logging
import zipfile
import tempfile
from typing import List, Optional
from google.cloud import storage
from google.api_core.exceptions import NotFound, GoogleAPICallError, PermissionDenied

class GCSHelper:
    def __init__(self, gcs_client: storage.Client):
        self.gcs_client = gcs_client

    def _list_blobs(self, bucket_name: str, prefix: str = "", extensions: Optional[List[str]] = None) -> List[storage.Blob]:
        """List all blobs in a bucket with optional prefix and optional extensions."""
        try:
            bucket = self.gcs_client.bucket(bucket_name)
            blobs = list(bucket.list_blobs(prefix=prefix))
            
            results = []
            for blob in blobs:
                if blob.name.endswith("/"):  # Skip directories
                    continue
                if extensions:
                    if any(blob.name.lower().endswith(ext.lower()) for ext in extensions):
                        results.append(blob)
                else:
                    results.append(blob)
            return results

        except NotFound:
            logging.error(f"Error: The bucket '{bucket_name}' was not found.")
            return []
        except PermissionDenied:
            logging.error(f"Error: Permission denied when accessing the bucket '{bucket_name}'.")
            return []
        except GoogleAPICallError as e:
            logging.error(f"API call error: {e}")
            return []
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            return []

    def _download_blob(self, blob_name: str, destination_file: str, bucket_name: str):
        """Download a blob from bucket to local file."""
        bucket = self.gcs_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.download_to_filename(destination_file)
        logging.info(f"Downloaded {blob_name} to {destination_file}")

    def _upload_blob(self, source_file: str, destination_blob: str, bucket_name: str):
        """Upload a local file to a GCS bucket."""
        bucket = self.gcs_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob)
        blob.upload_from_filename(source_file)
        logging.info(f"Uploaded {source_file} to {destination_blob}")

    def _delete_blob(self, blob_name: str, bucket_name: str):
        """Delete a blob from the bucket."""
        bucket = self.gcs_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
        logging.info(f"Deleted blob {blob_name}")

    def _move_blob(self, blob_name: str, new_blob_name: str, bucket_name: str):
        """Move blob within the same bucket."""
        bucket = self.gcs_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        new_blob = bucket.rename_blob(blob, new_blob_name)
        logging.info(f"Moved blob {blob_name} to {new_blob.name}")

    def extract_zip(self, file_path: str, out_dir_path: str) -> List[str]:
        """Extracts a local zip file to a directory."""
        if not file_path.endswith(".zip"):
            return [file_path]

        extracted_files = []
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            for member in zip_ref.namelist():
                # Correctly handle folder structures inside the zip
                filename = os.path.basename(member)
                if not filename:
                    continue
                    
                target_path = os.path.join(out_dir_path, filename)
                with zip_ref.open(member) as source, open(target_path, "wb") as target:
                    target.write(source.read())
                extracted_files.append(target_path)
                
        logging.info(f"Extracted {len(extracted_files)} files to {out_dir_path}")
        return extracted_files

    def _extract_files_from_blob(self, blob: storage.Blob, extensions: List[str] = None, output_dir: str = None) -> List[str]:
        """Downloads a blob and extracts it if it is a ZIP file."""
        if extensions:
            extensions = [ext.lower() for ext in extensions]
            
        if not output_dir:
            output_dir = tempfile.gettempdir()
            
        try:
            os.makedirs(output_dir, exist_ok=True)
            local_path = os.path.join(output_dir, os.path.basename(blob.name))
            blob.download_to_filename(local_path)

            files = []
            if local_path.lower().endswith(".zip"):
                files = self.extract_zip(local_path, output_dir)
            else:
                files.append(local_path)

            if extensions:
                return [f for f in files if any(f.lower().endswith(ext) for ext in extensions)]
            return files

        except zipfile.BadZipFile:
            raise ValueError(f"The file {blob.name} is not a valid zip file or is corrupted")
        except Exception as e:
            raise ValueError(f"Error while extracting {blob.name}: {e}")

    def _get_xlsx_from_blob(self, blob: storage.Blob) -> List[str]:
        """Specialized helper to get only XLSX files from a blob/zip."""
        return self._extract_files_from_blob(blob=blob, extensions=[".xlsx"])