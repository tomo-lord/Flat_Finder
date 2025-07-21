import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# If modifying these scopes, delete the file token.json.
# We need 'drive' scope to list and manage all files, not just those created by the app.
SCOPES = ["https://www.googleapis.com/auth/drive"]

def authenticate_google_drive():
    """Authenticates with Google Drive API and returns the service object."""
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    return build("drive", "v3", credentials=creds)

def find_or_create_folder(service, folder_name, source_folder_name="root"):
    """
    Finds the 'Archive' folder in the user's Drive. If not found, creates it.

    Args:
        service: The authenticated Google Drive service object.
        folder_name (str): The desired name for the folder.
        source_folder_name (str): The desired name for the source folder.

    Returns:
        str: The ID of the 'Archive' folder.
    """
    # Search for the folder
    query = (
        f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' "
        f"and '{source_folder_name}' in parents and trashed=false"
    )
    results = (
        service.files()
        .list(q=query, spaces="drive", fields="files(id, name)")
        .execute()
    )
    items = results.get("files", [])

    if items:
        print(f"Found existing '{folder_name}' folder: {items[0]['id']}")
        return items[0]["id"]
    else:
        # Create the folder if it doesn't exist
        print(f"'{folder_name}' folder not found. Creating it...")
        file_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [source_folder_name],
        }
        folder = (
            service.files()
            .create(body=file_metadata, fields="id, name")
            .execute()
        )
        print(f"Created '{folder_name}' folder: {folder.get('id')}")
        return folder.get("id")

def move_files_to_folder(
    service, source_folder_id, destination_folder_id
):
    """
    Moves all non-folder files from a source folder (or root) to a destination folder.

    Args:
        service: The authenticated Google Drive service object.
        source_folder_id (str): The ID of the folder to check for files. Use 'root' for My Drive.
        destination_folder_id (str): The ID of the folder to move files into.
    """
    print(f"\nChecking for files to move from '{source_folder_id}' to '{destination_folder_id}'...")

    # Query for files that are not folders within the source_folder_id
    query = (
        f"'{source_folder_id}' in parents and "
        "mimeType!='application/vnd.google-apps.folder' and "
        "trashed=false"
    )
    # Get all files, paginating if necessary
    files_to_move = []
    page_token = None
    while True:
        results = (
            service.files()
            .list(
                q=query,
                spaces="drive",
                fields="nextPageToken, files(id, name, parents)",
                pageToken=page_token,
            )
            .execute()
        )
        files_to_move.extend(results.get("files", []))
        page_token = results.get("nextPageToken", None)
        if not page_token:
            break

    if not files_to_move:
        print("No files found to move.")
        return

    print(f"Found {len(files_to_move)} files to move.")

    for file_item in files_to_move:
        file_id = file_item["id"]
        file_name = file_item["name"]
        current_parents = file_item.get("parents", [])

        # Ensure the destination folder is not already a parent to avoid errors
        if destination_folder_id in current_parents:
            print(f"Skipping '{file_name}' (ID: {file_id}) - already in archive.")
            continue

        try:
            # Remove the current parent and add the new parent
            # If a file has multiple parents, we remove the specified source_folder_id
            # and add the destination_folder_id.
            # 'fields': Specifies what fields to return after the update
            (
                service.files()
                .update(
                    fileId=file_id,
                    addParents=destination_folder_id,
                    removeParents=source_folder_id,
                    fields="id, parents",
                )
                .execute()
            )
            print(f"Moved '{file_name}' (ID: {file_id}) to archive.")
        except Exception as e:
            print(f"Error moving '{file_name}' (ID: {file_id}): {e}")

def upload_file_to_folder(
    service, file_path, folder_id, new_file_name=None
):
    """
    Uploads a file to a specific Google Drive folder.

    Args:
        service: The authenticated Google Drive service object.
        file_path (str): The path to the file you want to upload.
        folder_id (str): The ID of the target Google Drive folder.
        new_file_name (str, optional): The name to give the file on Google Drive.
                                       If None, the original file name is used.

    Returns:
        dict: The uploaded file's metadata.
    """
    if not os.path.exists(file_path):
        print(f"Error: Local file not found at {file_path}")
        return None

    if new_file_name is None:
        new_file_name = os.path.basename(file_path)

    file_metadata = {
        "name": new_file_name,
        "parents": [folder_id],
    }
    media = MediaFileUpload(file_path, resumable=True)

    try:
        file = (
            service.files()
            .create(body=file_metadata, media_body=media, fields="id, name")
            .execute()
        )
        print(f"File ID: {file.get('id')}")
        print(f"File Name: {file.get('name')}")
        print(f"File '{new_file_name}' uploaded successfully to folder ID '{folder_id}'!")
        return file
    except Exception as e:
        print(f"An error occurred during upload: {e}")
        return None

def upload_file(file_path: str):
    TARGET_UPLOAD_FOLDER_NAME = "OtoDom"

    ARCHIVE_FOLDER_NAME = "Archive"

    # Authenticate and get the Drive service
    drive_service = authenticate_google_drive()

    if drive_service:
        otodom_folder_id = find_or_create_folder(
            drive_service, TARGET_UPLOAD_FOLDER_NAME
        )

        # 1. Find or create the 'Archive' folder
        archive_folder_id = find_or_create_folder(
            drive_service, ARCHIVE_FOLDER_NAME, otodom_folder_id
        )

        if archive_folder_id:
            # 2. Move existing non-folder files to the 'Archive' folder
            move_files_to_folder(
                drive_service, otodom_folder_id, archive_folder_id
            )

            # 3. Upload the new file to the specified target folder
            print(f"\nAttempting to upload '{file_path}' to '{otodom_folder_id}'...")
            uploaded_file_info = upload_file_to_folder(
                drive_service,
                file_path,
                otodom_folder_id,
            )
            if uploaded_file_info:
                print("\nUpload complete.")
            else:
                print("\nFile upload failed.")
        else:
            print("Could not get or create the Archive folder. Aborting upload.")
    else:
        print("Failed to authenticate with Google Drive.")