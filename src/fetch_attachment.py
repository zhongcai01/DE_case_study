import os
import msal
import requests
from urllib.parse import urlencode, urlparse, parse_qs
import zipfile
import base64
import json

with open('constants.json', 'r') as f:
    constants = json.load(f)

# Azure AD App Registration Details
CLIENT_ID = constants["CLIENT_ID"]
CLIENT_SECRET = constants["CLIENT_SECRET"]
TENANT_ID = constants["TENANT_ID"]
REDIRECTED_URI = constants["REDIRECTED_URI"]
SCOPES = constants["SCOPES"]

# MSAL and OAuth Endpoints
AUTHORITY = f'https://login.microsoftonline.com/{TENANT_ID}'
AUTH_URL = f'{AUTHORITY}/oauth2/v2.0/authorize'
TOKEN_URL = f'{AUTHORITY}/oauth2/v2.0/token'

# microsoft graph api
GRAPH_API_URL = constants["GRAPH_API_URL"]

# folder names
FOLDER_NAME = constants["FOLDER_NAME"]
ATTACHMENT_SAVE_PATH = constants["ATTACHMENT_SAVE_PATH"]
EXTRACT_TO_FOLDER = constants["EXTRACT_TO_FOLDER"]

# MSAL PublicClientApplication initialization
app = msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY)

# Step 1: Get Authorization Code by Redirecting the User to the Login Page
def get_authorization_url():
    auth_params = {
        'client_id': CLIENT_ID,
        'response_type': 'code',
        'redirected_uri': REDIRECTED_URI,
        'scope': ' '.join(SCOPES),
    }
    url = f"{AUTH_URL}?{urlencode(auth_params)}"
    return url

# Step 2: Handle the Redirect and Extract the Authorization Code
def extract_authorization_code(redirected_url):
    parsed_url = urlparse(redirected_url)
    code = parse_qs(parsed_url.query).get('code', [None])[0]
    return code

# Step 3: Exchange Authorization Code for Access Token
def get_access_token_from_code(auth_code):
    token_data = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'code': auth_code,
        'redirected_uri': REDIRECTED_URI,
        'grant_type': 'authorization_code'
    }
    
    response = requests.post(TOKEN_URL, data=token_data)
    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        raise Exception(f"Error getting access token: {response.status_code} {response.text}")

# Step 4: Get User Info (or make any Graph API requests)
def get_folder_id(access_token):
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    response = requests.get(f'{GRAPH_API_URL}/MailFolders', headers=headers)
    response.raise_for_status()
    folders = response.json().get("value", [])
    for folder in folders:
        if folder["displayName"].lower() == FOLDER_NAME.lower():
            return folder["id"]
    raise Exception(f"Folder '{FOLDER_NAME}' not found.")

def download_attachments(access_token, folder_id):
    # Download attachments from emails in the specified folder.
    # filter for email with attachments in specific folder
    url = f"{GRAPH_API_URL}/mailFolders/{folder_id}/messages?$expand=attachments" 
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    messages = response.json().get("value", [])
    for message in messages:
        attachments_url = f"{GRAPH_API_URL}/messages/{message['id']}/attachments"
        attachments_response = requests.get(attachments_url, headers=headers)
        attachments_response.raise_for_status()
        attachments = attachments_response.json().get("value", [])
        for attachment in attachments:
            if "@odata.type" in attachment and "fileAttachment" in attachment["@odata.type"]:
                file_name = attachment["name"]
                file_content = attachment["contentBytes"]
                file_path = os.path.join(ATTACHMENT_SAVE_PATH, file_name)
                # file_content is encoded in base64
                with open(file_path, "wb") as f:
                    f.write(base64.b64decode(file_content))
                print(f"Saved attachment: {file_path}")
    
def extract_zip_from_attachment_save_path(attachment_save_path, extract_to_folder):
    # loop through contents of attachment_save_path folder
    for filename in os.listdir(attachment_save_path):
        file_path = os.path.join(attachment_save_path, filename)
        # check if file is a zip file
        if filename.endswith('.zip') and os.path.isfile(file_path):
            extract_to = os.path.join(extract_to_folder, os.path.splitext(filename)[0])
            os.makedirs(extract_to, exist_ok=True)

            try:
                # Open and extract the zip file
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_to)
                    print(f"Extracted '{filename}' to '{extract_to}'")
            except zipfile.BadZipFile:
                print(f"Error: '{filename} is not a valid zip file")
        else:
            print(f"Skipping '{filename}', not a zip file.")

def access_email_and_download_attachments_and_extract():

    with open('constants.json', 'r') as f:
        constants = json.load(f)

    # Create folder to save attachments
    if not os.path.exists(ATTACHMENT_SAVE_PATH):
        os.makedirs(ATTACHMENT_SAVE_PATH)

    # Step 1: Get the authorization URL
    authorization_url = get_authorization_url()
    print(f"Please visit the following URL and log in: {authorization_url}")
    
    # Let the user log in and get the authorization code
    redirected_url = input("Paste the redirected URL here: ")
    
    # Step 2: Extract the authorization code from the redirected URL
    auth_code = extract_authorization_code(redirected_url)
    
    # Step 3: Exchange the authorization code for an access token
    access_token = get_access_token_from_code(auth_code)
    
    # Step 4: Use the access token to access the Microsoft Graph API (e.g., get user info)
    folder_id = get_folder_id(access_token)
    download_attachments(access_token, folder_id)

    # Step 5: extract zipfiles from attachment_save_path
    extract_zip_from_attachment_save_path(ATTACHMENT_SAVE_PATH, EXTRACT_TO_FOLDER)


# Main Program
if __name__ == '__main__':
    access_email_and_download_attachments_and_extract()
    
