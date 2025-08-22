import time
import os
import glob
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from config import DRIVE_SCOPES
from secret_manager import access_secret
import json

drive_json = json.loads(access_secret("kitrum-cloud", "google_drive_artem"))


class GoogleDrive:
    def __init__(self):
        self.creds = None
        self.service = None
        self.drive_files = []
        self.local_directory = 'files'
        self.files_for_bq = []

    def get_service(self):
        if not self.creds or not self.creds.valid:
            self.creds = Credentials.from_authorized_user_info(drive_json, DRIVE_SCOPES)
            self.creds.refresh(Request())
            self.service = build("drive", "v3", credentials=self.creds)

    def list_files(self):
        self.get_service()
        all_files, next_page_token = {}, None
        while True:
            page_files_response = self.service.files().list(
                corpora='drive',
                driveId="0AK2mosQ1T1NVUk9PVA",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                pageSize=1000,
                pageToken=next_page_token

            ).execute()
            page_files = page_files_response['files']
            for page_file in page_files:
                all_files[page_file['name']] = page_file['id']
            try:
                next_page_token = page_files_response["nextPageToken"]
            except KeyError:
                next_page_token = None
            if not next_page_token:
                break
        self.drive_files = all_files

    def upload_file(self, file_name, file_path):
        retries = 1
        while retries <= 5:
            print(f"\t\tUploading File {file_name} to Google Drive, attempt: {retries}")
            try:
                self.get_service()
                file_metadata = {
                    "name": file_name,
                    "parents": ["0AK2mosQ1T1NVUk9PVA"]
                }
                media = MediaFileUpload(file_path)
                create_file = self.service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields="id",
                    supportsAllDrives=True
                ).execute()
                file_url = f"https://drive.google.com/file/d/{create_file['id']}"
                return file_url
            except:
                retries += 1

    def save_file_locally(self, file):
        try:
            with open(f"{self.local_directory}/{file['file_name']}", 'wb') as f:
                f.write(file['file_data'])
            return True
        except FileNotFoundError:
            return False

    def uploader(self, files_list):
        file_counter = 0
        for file in files_list:
            file_counter += 1
            print("\t\t-------------------------------------------------")
            print(f"\t\t{file_counter} of {len(files_list)}")
            if f"{file['file_name']}" in self.drive_files:
                file_url = f"https://drive.google.com/file/d/{self.drive_files[file['file_name']]}"
            else:
                saved = self.save_file_locally(file)
                if not saved:
                    continue
                time.sleep(1)
                try:
                    file_url = self.upload_file(file['file_name'], f"{self.local_directory}/{file['file_name']}")
                except Exception as e:
                    file_url = ""
                    print("ERROR")
                if file_url:
                    self.drive_files[file['file_name']] = file_url

            self.files_for_bq.append({
                "name": file['attachment_name'],
                "url": file_url,
                "messageId": file['message_id']
            })

    def delete_local_files(self):
        files = glob.glob(f'{self.local_directory}/*')
        for f in files:
            os.remove(f)