import os
import re
from datetime import datetime, timedelta
from utils import RequestMock, get_config, get_default_credentials, load_csv_to_dataframe, load_excel_to_dataframe, get_local_path
from firestore import Firestore
from sftp import SFTPHandler
from google.cloud import storage

credentials, project = get_default_credentials()

FS_COLLECTION_CONFIGS = 'configs_jobs'
FS_DOCUMENT_CONFIG_ID = 'job-data-fetcher-procare'
FS_FIELD_HOST = "sftp_host"
FS_FIELD_USERNAME = "sftp_username"
FS_FIELD_REMOTE_PATH = "sftp_remote_path"  # dir where files are stored on SFTP
FS_FIELD_BUCKET = "bucket"


def procare_file_filter(filename: str, file_date: datetime.date, bucket_name: str) -> bool:
    if file_exists_in_bucket(bucket_name, os.path.basename(filename)):
        print(f"Skip: File already exists in the bucket")
        return False

    filename_upper = filename.upper()

    match_procare = re.search(r'PROCARE_THERANICA_ITD_DATAFEED_(\d{4})-(\d{2})-(\d{2})', filename_upper)
    if match_procare:
        year, month, day = map(int, match_procare.groups())
        try:
            datetime(year, month, day)
            print(f"Match: Valid PROCARE file → {filename}")
            return True
        except ValueError:
            print(f"Skip: Invalid date in PROCARE filename → {filename}")
            return False

    if 'BI SUMMARY' in filename_upper:
        match_bi = re.match(r'(\d{8})- BI SUMMARY', filename_upper)
        if match_bi:
            try:
                datetime.strptime(match_bi.group(1), "%Y%m%d")
                print(f"Match: Valid BI SUMMARY file → {filename}")
                return True
            except ValueError:
                print(f"Skip: Invalid date in BI SUMMARY filename → {filename}")
                return False
        else:
            print(f"Skip: BI SUMMARY filename doesn't match expected format → {filename}")
            return False

    # file is neither PROCARE nor BI SUMMARY
    print(f"Skip: Not a PROCARE or BI SUMMARY file → {filename}")
    return False


def file_exists_in_bucket(bucket_name: str, file_name: str) -> bool:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    return bucket.blob(file_name).exists()


def run(event=None, context=None):
    fs = Firestore(project)
    config = get_config(project, FS_COLLECTION_CONFIGS, FS_DOCUMENT_CONFIG_ID)

    # extract multiple fs fields
    sftp_host = config.get(FS_FIELD_HOST, "")
    sftp_username = config.get(FS_FIELD_USERNAME, "")
    sftp_password = os.environ.get("PROCARE_SFTP_PASSWD")
    sftp_remote_path = config.get(FS_FIELD_REMOTE_PATH, "")
    gcs_bucket = config.get(FS_FIELD_BUCKET, "")

    handler = SFTPHandler(
        host=sftp_host,
        username=sftp_username,
        password=sftp_password,
        remote_path=sftp_remote_path,
        bucket=gcs_bucket
    )
    handler.connect()

    # only for testing
    # sftp = handler.sftp
    # print("files in remote SFTP folder:")
    # for file_attr in sftp.listdir_attr(handler.remote_path):
    #     print(file_attr.filename)

    def wrapped_filter(filename, file_date):
        return procare_file_filter(filename, file_date, gcs_bucket)

    all_matching_files = handler.get_new_files(filter_func=wrapped_filter)
    if not all_matching_files:
        print("No matching files found")
        handler.close()
        return "No files processed"

    # filter out files that already exist in GCS
    new_files = [f for f in all_matching_files if not file_exists_in_bucket(gcs_bucket, os.path.basename(f))]
    if not new_files:
        print("All matching files already exist in the bucket")
        handler.close()
        return "No new files to upload"

    # download and upload to GCS
    downloaded_files = handler.download_files(new_files)
    handler.upload_to_gcs(downloaded_files)

    handler.close()
    print("Processing completed")
    return "OK"


# FOR LOCAL TESTING
if __name__ == '__main__':
    run()
