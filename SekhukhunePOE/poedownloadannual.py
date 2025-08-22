import os
import requests
import xml.etree.ElementTree as ET
from urllib.parse import urljoin, quote
from tqdm import tqdm
import re

# Credentials for API (used only for XML feed, not blob download)
USERNAME = "elvis@igssolutions.co.za"
PASSWORD = "epmspass@@998"

# URLs
BASE_URL = "https://epmpro.azurewebsites.net/ApplicationData.svc/"
POEFILES_URL = urljoin(BASE_URL, "POEFiles")
USERINPUTS_URL = urljoin(BASE_URL, "IPMSUserInputs")
IPMSSET_URL = urljoin(BASE_URL, "IPMSSet")
DEPARTMENTS_URL = urljoin(BASE_URL, "Departments")

# XML namespaces
NS = {
    'atom': 'http://www.w3.org/2005/Atom',
    'd': 'http://schemas.microsoft.com/ado/2007/08/dataservices',
    'm': 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata'
}

def fetch_xml(url):
    response = requests.get(url, auth=(USERNAME, PASSWORD))
    response.raise_for_status()
    return ET.fromstring(response.content)

def extract_entries(xml_root):
    return xml_root.findall('atom:entry', NS)

def parse_poe_files(entries):
    poe_files = []
    for entry in entries:
        try:
            props = entry.find('atom:content/m:properties', NS)
            poe_files.append({
                'id': int(props.find('d:Id', NS).text),
                'url': props.find('d:FileURL', NS).text,
                'ipms_user_input_id': int(props.find('d:POEFile_IPMSUserInput', NS).text)
            })
        except Exception as e:
            print(f"Skipping POE entry due to parsing error: {e}")
    return poe_files

def parse_user_inputs(entries):
    user_inputs = {}
    for entry in entries:
        try:
            props = entry.find('atom:content/m:properties', NS)
            user_inputs[int(props.find('d:Id', NS).text)] = {
                'ipms_id': int(props.find('d:IPMSUserInput_IPMS', NS).text),
                'quarter': props.find('d:Quarter', NS).text
            }
        except Exception as e:
            print(f"Skipping User Input entry due to parsing error: {e}")
    return user_inputs

def parse_ipms_set(entries):
    ipms_set = {}
    for entry in entries:
        try:
            props = entry.find('atom:content/m:properties', NS)
            ipms_set[int(props.find('d:Id', NS).text)] = {
                'indicator_no': props.find('d:IDPIndicatorNo', NS).text,
                'from_year': int(props.find('d:FromYear', NS).text),
                'department_id': int(props.find('d:IPMS_Department', NS).text)
            }
        except Exception as e:
            print(f"Skipping IPMS Set entry due to parsing error: {e}")
    return ipms_set

def parse_departments(entries):
    departments = {}
    for entry in entries:
        try:
            props = entry.find('atom:content/m:properties', NS)
            departments[int(props.find('d:Id', NS).text)] = props.find('d:Description', NS).text
        except Exception as e:
            print(f"Skipping Department entry due to parsing error: {e}")
    return departments

def sanitize_name(name):
    """Sanitize names for file/folder usage on Windows."""
    # Remove invalid characters
    name = re.sub(r'[<>:"/\\|?*]', '', name)
    # Replace multiple spaces
    name = re.sub(r'\s+', ' ', name)
    return name.strip()

def ensure_folder(path):
    try:
        if not os.path.exists(path):
            os.makedirs(path)
    except OSError as e:
        print(f"Skipping folder creation for path '{path}' due to error: {e}")

def download_file(url, dest_path):
    headers = {'User-Agent': 'Mozilla/5.0'}
    encoded_url = quote(url, safe=':/?=&')
    try:
        response = requests.get(encoded_url, headers=headers, stream=True, timeout=(10, 60))
        response.raise_for_status()
        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(1024):
                f.write(chunk)
        return True
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return False

# --- MAIN SCRIPT ---

print("Fetching XML feeds...")
poe_entries = extract_entries(fetch_xml(POEFILES_URL))
user_entries = extract_entries(fetch_xml(USERINPUTS_URL))
ipms_entries = extract_entries(fetch_xml(IPMSSET_URL))
dept_entries = extract_entries(fetch_xml(DEPARTMENTS_URL))

poe_files = parse_poe_files(poe_entries)
user_inputs = parse_user_inputs(user_entries)
ipms_set = parse_ipms_set(ipms_entries)
departments = parse_departments(dept_entries)

print("Downloading filtered files...")
count = 0

for file in tqdm(poe_files):
    user_input = user_inputs.get(file['ipms_user_input_id'])
    if not user_input or user_input['quarter'] != "Annual":
        continue

    ipms = ipms_set.get(user_input['ipms_id'])
    if not ipms or ipms['from_year'] != 2024:
        continue

    department_name_raw = departments.get(ipms['department_id'], "UnknownDepartment")
    department_name = sanitize_name(department_name_raw)

    quarter_folder = sanitize_name(user_input['quarter'])
    indicator_folder = sanitize_name(ipms['indicator_no'])
    file_name = sanitize_name(os.path.basename(file['url']))

    folder_path = os.path.join(quarter_folder, department_name, indicator_folder)

    ensure_folder(folder_path)

    dest_file_path = os.path.join(folder_path, file_name)

    if not os.path.exists(dest_file_path):
        if download_file(file['url'], dest_file_path):
            count += 1

print(f"Download complete. {count} files downloaded.")
