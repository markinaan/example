import os
from google.cloud import storage
from utils.utils import get_local_path


class Storage:
    def __init__(self) -> None:
        self._storage = storage.Client()

    def download_blob(self, bucket_name: str, source_blob_name: str, target_path: str = None) -> str:
        bucket = self._storage.bucket(bucket_name)
        target = target_path if target_path else os.path.join(get_local_path(), source_blob_name)
        bucket.blob(source_blob_name).download_to_filename(target)
        print(f'blob {source_blob_name} downloaded to path {target}')
        return target

    def upload_blob(self, bucket_name: str, source_blob_name: str, target_blob_name: str) -> None:
        bucket = self._storage.bucket(bucket_name)
        blob = bucket.blob(target_blob_name)
        blob.upload_from_filename(source_blob_name)
        print(f'blob {target_blob_name} uploaded to bucket {bucket_name} successfully')
