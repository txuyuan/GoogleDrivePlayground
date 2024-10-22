import os.path
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]

units = {
    "KB": 10**3,
    "MB": 10**6,
    "GB": 10**9,
    "TB": 10**12
}
def bytes2str(byteCount):
    for (unit, size) in reversed(units.items()):
        if byteCount // size > 1:
            return f"{(byteCount / size):.1f}{unit}"
    return "0"

def fetch_parent_wrapper(process):
    file, parent, creds = process
    service = build("drive", "v3", credentials=creds)
    return (file, fetch_file(parent, service))

def fetch_file(fileId, service):
    file = (
        service.files().get(fileId=fileId)
        .execute()
    )
    return file

def init_service():
  creds = None
  # The file token.json stores the user's access and refresh tokens, and is
  # created automatically when the authorization flow completes for the first
  # time.
  if os.path.exists("token.json"):
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)
  # If there are no (valid) credentials available, let the user log in.
  if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
      creds.refresh(Request())
    else:
      flow = InstalledAppFlow.from_client_secrets_file(
          "credentials.json", SCOPES
      )
      creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open("token.json", "w") as token:
      token.write(creds.to_json())

  service = build("drive", "v3", credentials=creds)
  return service, creds


def main():
    try:
        service, creds = init_service()

        # Primary - Fetch files
        results = (service.files()
            .list(
                pageSize=int(sys.argv[1]), 
                orderBy="quotaBytesUsed desc,recency", 
                #and '191519R@student.hci.edu.sg' in owners
                q="""
                    name contains 'rioHC'
                    and mimeType != 'application/vnd.google-apps.folder'
                    and mimeType != 'vnd.google-apps.shortcut'
                    and not 'choir@student.hci.edu.sg' in owners
                """,
                fields="files(id, name, quotaBytesUsed, mimeType, parents, owners)"
            )
            .execute()
        )
        files = results["files"]
        print("Finished fetching primary")
        print(f"Primary returned {len(files)} results")
    
    except HttpError as error:
        print(f"An error occurred in primary fetch: {error}")

    try:
        # Secondary - Fetch Parents
        processes = []
        for file in files:
            file["parentNames"] = []
            if "parents" in file:
                for parent in file["parents"]:
                    processes += [(file, parent, creds)]

        with ThreadPoolExecutor(max_workers=3) as executor:
            results = list(executor.map(fetch_parent_wrapper, processes))

        for (file, parentFile) in results:
            file["parentNames"] += [parentFile["name"]]

        print("Finished fetching secondary")


    except HttpError as error:
        print(f"An error occured in secondary fetch: {fetch}")

   # Finally - displayed
    print()
    print(f"     {'Bytes':<6}\t{'Name':<40}\t{'Parent Names': <25}\t{'Owner Email': <30}\t{'Owner Name': <25}\t{'MimeType':<8}")
    for (i, file) in enumerate(files):
        size = bytes2str(int(file['quotaBytesUsed']))
        name = file['name']
        parentNames = ",".join(file['parentNames']) if len(file['parentNames']) > 0 else ""
        ownerEmail = file['owners'][0]['emailAddress']
        ownerName = file['owners'][0]['displayName']
        mimeType = file['mimeType']
        print(f"{str(i) :<4} {size: <6}\t{name: <40}\t{parentNames: <25}\t{ownerEmail: <30}\t{ownerName: <25}\t{mimeType: <8}")
    print()

    types = {}
    totalBytes = 0
    for file in files:
        totalBytes += int(file['quotaBytesUsed'])
        mimetype = file["mimeType"]
        if mimetype not in types:
            types[mimetype] = 1
        else:
            types[mimetype] += 1
      
    types = dict(sorted(types.items(), key=lambda x: x[1], reverse=True))
    print("Aggregated file types: ", json.dumps(types, indent=2))
    print()
    print(f"Total bytes displayed: {bytes2str(totalBytes)}")


if __name__ == "__main__":
    main()
