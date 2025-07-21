import os
import paramiko
from datetime import datetime, timedelta
from utils.utils import get_local_path
from core.storage import Storage
import io


BUFFER_SIZE = 65536
TIMEOUT = 60
KEEPALIVE_INTERVAL = 10
SFTP_PORT = 22


class SFTPHandler:
    def __init__(self, host: str, username: str, remote_path: str, bucket: str, password: str = None, private_key_path: str = None):
        self.host = host
        self.username = username
        self.password = password
        self.remote_path = remote_path
        self.bucket = bucket
        self.private_key_path = private_key_path
        self.sftp = None
        self.transport = None

    def connect(self):
        print("Connecting to SFTP server...")
        self.transport = paramiko.Transport((self.host, SFTP_PORT))
        self.transport.banner_timeout = TIMEOUT  # increase timeout
        self.transport.set_keepalive(KEEPALIVE_INTERVAL)  # keep connection alive

        if self.password:  # password authentication
            self.transport.connect(username=self.username, password=self.password)

        elif self.private_key_path: # SSH key authentication
            print("Trying to use private key at:", self.private_key_path)
            with open(self.private_key_path, "r") as f:
                key_data = f.read()
            # print("Key starts with:", key_data[:30])
            key_stream = io.StringIO(key_data)

            try:
                private_key = paramiko.RSAKey.from_private_key(key_stream)
            except paramiko.SSHException:
                key_stream.seek(0)
                private_key = paramiko.Ed25519Key.from_private_key(key_stream)

            self.transport.connect(username=self.username, pkey=private_key)

        else:
            # print("Either password or private_key_path must be provided")
            raise ValueError("Either password or private_key_path must be provided")

        self.sftp = paramiko.SFTPClient.from_transport(self.transport)
        print("Connected to SFTP successfully!")

    def close(self):
        if self.sftp:
            self.sftp.close()
        if self.transport:
            self.transport.close()
        print("SFTP connection closed")

    #
    def get_new_files(self, filter_func=None) -> list:
        """Fetches files matching the filter_func condition"""
        print("Fetching file list from SFTP...")
        files = []

        for f in self.sftp.listdir_attr(self.remote_path):
            filename = f.filename
            file_date = datetime.fromtimestamp(f.st_mtime).date()
            if filter_func is None or filter_func(filename, file_date):
                files.append(filename)

        print(f"Files found: {files}" if files else "No new files found")
        return files


    def download_large_file(self, remote_file: str, local_file: str):
        print(f"Downloading: {remote_file} → {local_file}")
        with self.sftp.open(remote_file, 'rb') as r_file, open(local_file, 'wb') as l_file:
            while True:
                data = r_file.read(BUFFER_SIZE)
                if not data:
                    break
                l_file.write(data)
        print(f"Download complete: {local_file}")

    def download_files(self, file_list: list) -> list:
        if not file_list:
            print("No files to download")
            return []

        downloaded_files = []
        temp_dir = get_local_path()

        for file in file_list:
            # remote = os.path.join(self.remote_path, file)
            remote = f"{self.remote_path.rstrip('/')}/{file}"
            local = os.path.join(temp_dir, file)
            self.download_large_file(remote, local)
            downloaded_files.append(local)

        print("All new files downloaded to tmp")
        return downloaded_files

    def upload_to_gcs(self, local_files: list) -> list:
        if not local_files:
            print("No files to upload")
            return []

        for path in local_files:
            filename = os.path.basename(path)
            print(f"Uploading {filename} to bucket {self.bucket}")
            Storage().upload_blob(self.bucket, path, filename)

        print("Upload completed")
        return local_files
