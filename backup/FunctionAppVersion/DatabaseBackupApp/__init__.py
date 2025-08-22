import os
import datetime
import pyodbc
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import subprocess

# Set Google Drive folder ID
GOOGLE_DRIVE_FOLDER_ID = "<YOUR_FOLDER_ID>"

# Path to temporary backup
TEMP_DIR = os.environ.get("TMP", "/tmp")
os.makedirs(TEMP_DIR, exist_ok=True)

# Google Service Account JSON
SERVICE_ACCOUNT_FILE = "service_account.json"

# Authenticate with Google Drive
credentials = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=["https://www.googleapis.com/auth/drive.file"]
)
drive_service = build('drive', 'v3', credentials=credentials)


def upload_to_drive(file_path, file_name):
    file_metadata = {
        'name': file_name,
        'parents': [GOOGLE_DRIVE_FOLDER_ID]
    }
    media = MediaFileUpload(file_path, mimetype='application/octet-stream')
    uploaded_file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()
    print(f"Uploaded {file_name} to Google Drive with ID {uploaded_file.get('id')}")


def export_bacpac(server, database, username, password):
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    bacpac_name = f"{database}_{timestamp}.bacpac"
    output_path = os.path.join(TEMP_DIR, bacpac_name)

    connection_string = (
        f"Data Source={server};"
        f"Initial Catalog={database};"
        f"User ID={username};"
        f"Password={password};"
        "TrustServerCertificate=True"
    )

    # Use sqlpackage if you want to stick with it
    command = [
        "D:\\home\\site\\wwwroot\\ExportDbFunction\\sqlpackage\\sqlpackage.exe",  # bundle exe
        "/Action:Export",
        "/SourceConnectionString:" + connection_string,
        f"/tf:{output_path}"
    ]

    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode == 0:
        print(f"[✔] Export successful: {output_path}")
        upload_to_drive(output_path, bacpac_name)
    else:
        print("[✖] Export failed")
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)


# Example usage
export_bacpac(
    server="4.221.230.0",
    database="stlmprodb",
    username="stlmprodbuser",
    password="Stlm@epms#2025"
)

export_bacpac(
    server="4.221.229.40",
    database="ndmprodb",
    username="ndmvmuserpro",
    password="ndm@virual@987#"
)
