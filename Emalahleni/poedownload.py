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
BASE_URL = "https://epmswebpro.igsswebapps.net/ApplicationData.svc/"
POEFILES_URL = urljoin(BASE_URL, "POEFiles")
USERINPUTS_URL = urljoin(BASE_URL, "IPMSUserInputs")
IPMSSET_URL = urljoin(BASE_URL, "IPMSSet")
DEPARTMENTS_URL = urljoin(BASE_URL, "Departments")
MUNICIPALITIES_URL = urljoin(BASE_URL, "Municipalities")

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
            if props is None:
                print("Skipping POE entry due to missing properties")
                continue

            id_elem = props.find('d:Id', NS)
            file_url_elem = props.find('d:FileURL', NS)
            ipms_user_input_elem = props.find('d:POEFile_IPMSUserInput', NS)

            if (id_elem is None or not id_elem.text or
                file_url_elem is None or not file_url_elem.text or
                ipms_user_input_elem is None or not ipms_user_input_elem.text):
                print("Skipping POE entry due to missing element values")
                continue

            poe_files.append({
                'id': int(id_elem.text),
                'url': file_url_elem.text,
                'ipms_user_input_id': int(ipms_user_input_elem.text)
            })
        except Exception as e:
            print(f"Skipping POE entry due to parsing error: {e}")
    return poe_files

def parse_user_inputs(entries):
    user_inputs = {}
    for entry in entries:
        try:
            props = entry.find('atom:content/m:properties', NS)
            if props is None:
                print("Skipping User Input entry due to missing properties")
                continue

            id_elem = props.find('d:Id', NS)
            ipms_user_input_elem = props.find('d:IPMSUserInput_IPMS', NS)
            quarter_elem = props.find('d:Quarter', NS)

            if (id_elem is None or not id_elem.text or
                ipms_user_input_elem is None or not ipms_user_input_elem.text or
                quarter_elem is None or not quarter_elem.text):
                print("Skipping User Input entry due to missing element values")
                continue

            user_inputs[int(id_elem.text)] = {
                'ipms_id': int(ipms_user_input_elem.text),
                'quarter': quarter_elem.text
            }
        except Exception as e:
            print(f"Skipping User Input entry due to parsing error: {e}")
    return user_inputs

def parse_ipms_set(entries):
    ipms_set = {}
    for entry in entries:
        try:
            props = entry.find('atom:content/m:properties', NS)
            if props is None:
                print("Skipping IPMS Set entry due to missing properties")
                continue

            id_elem = props.find('d:Id', NS)
            indicator_elem = props.find('d:IDPIndicatorNo', NS)
            from_year_elem = props.find('d:FromYear', NS)
            department_elem = props.find('d:IPMS_Department', NS)
            municipality_elem = props.find('d:IPMS_Municipality', NS)
            is_disabled_elem = props.find('d:isDisabled', NS)  # New: check for disabled status

            # If the isDisabled element exists and its text is "1", skip this entry.
            if is_disabled_elem is not None and is_disabled_elem.text and is_disabled_elem.text.strip() == "1":
                continue

            if (id_elem is None or not id_elem.text or
                indicator_elem is None or not indicator_elem.text or
                from_year_elem is None or not from_year_elem.text or
                department_elem is None or not department_elem.text or
                municipality_elem is None or not municipality_elem.text):
                continue

            ipms_set[int(id_elem.text)] = {
                'indicator_no': indicator_elem.text,
                'from_year': int(from_year_elem.text),
                'department_id': int(department_elem.text),
                'municipality_id': int(municipality_elem.text)
            }
        except Exception as e:
            print(f"Skipping IPMS Set entry due to parsing error: {e}")
    return ipms_set

def parse_departments(entries):
    departments = {}
    for entry in entries:
        try:
            props = entry.find('atom:content/m:properties', NS)
            if props is None:
                print("Skipping Department entry due to missing properties")
                continue

            id_elem = props.find('d:Id', NS)
            desc_elem = props.find('d:Name', NS)

            if id_elem is None or not id_elem.text:
                print("Skipping Department entry due to missing ID")
                continue
            if desc_elem is None or not desc_elem.text:
                print("Skipping Department entry due to missing Description")
                continue

            departments[int(id_elem.text)] = desc_elem.text
        except Exception as e:
            print(f"Skipping Department entry due to parsing error: {e}")
    return departments

def parse_municipalities(entries):
    municipalities = {}
    for entry in entries:
        try:
            props = entry.find('atom:content/m:properties', NS)
            if props is None:
                continue

            id_elem = props.find('d:Id', NS)
            if id_elem is None or not id_elem.text:
                continue

            m_id = int(id_elem.text)
            # Only process municipality with id 33
            if m_id != 33:
                continue

            desc_elem = props.find('d:Name', NS)
            if desc_elem is None or not desc_elem.text:
                print(f"Skipping Municipality entry due to missing Description for ID {m_id}")
                continue

            municipalities[m_id] = desc_elem.text
        except Exception as e:
            print(f"Skipping Municipality entry due to parsing error: {e}")
    return municipalities

def ensure_folder(path):
    if not os.path.exists(path):
        os.makedirs(path)

def sanitize_name(name):
    """Sanitize folder names by removing invalid characters and trimming spaces."""
    return re.sub(r'[<>:"/\\|?*]', '', name).strip()

def download_file(url, dest_path):
    headers = {'User-Agent': 'Mozilla/5.0'}
    encoded_url = quote(url, safe=':/?=&')
    try:
        response = requests.get(encoded_url, headers=headers, stream=True, timeout=10)
        response.raise_for_status()
        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(1024):
                f.write(chunk)
        return True
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return False

if __name__ == '__main__':
    print("Fetching XML feeds...")
    # Fetch XML feeds and extract entries
    poe_entries = extract_entries(fetch_xml(POEFILES_URL))
    user_entries = extract_entries(fetch_xml(USERINPUTS_URL))
    ipms_entries = extract_entries(fetch_xml(IPMSSET_URL))
    dept_entries = extract_entries(fetch_xml(DEPARTMENTS_URL))
    muni_entries = extract_entries(fetch_xml(MUNICIPALITIES_URL))

    # Parse XML feeds
    poe_files = parse_poe_files(poe_entries)
    user_inputs = parse_user_inputs(user_entries)
    ipms_set = parse_ipms_set(ipms_entries)
    departments = parse_departments(dept_entries)
    # This will now only capture municipality id 33
    municipalities = parse_municipalities(muni_entries)

    # Filter IPMS entries to only those for Municipality 33
    ipms_set = {k: v for k, v in ipms_set.items() if v.get('municipality_id') == 33}

    print("Downloading filtered files...")
    count = 0
    for file in tqdm(poe_files):
        user_input = user_inputs.get(file['ipms_user_input_id'])
        if not user_input or user_input['quarter'] != "4":
            continue

        ipms = ipms_set.get(user_input['ipms_id'])
        # Skip if no corresponding IPMS record exists or its year is not 2024.
        if not ipms or ipms['from_year'] != 2024:
            continue

        # Sanitize folder names for quarter, department, and indicator number
        department_name_raw = departments.get(ipms['department_id'], "UnknownDepartment")
        department_name = sanitize_name(department_name_raw)
        quarter_folder = sanitize_name(user_input['quarter'])
        indicator_folder = sanitize_name(ipms['indicator_no'])

        file_name = os.path.basename(file['url'])
        folder_path = os.path.join(quarter_folder, department_name, indicator_folder)
        ensure_folder(folder_path)

        dest_file_path = os.path.join(folder_path, file_name)
        if not os.path.exists(dest_file_path):
            if download_file(file['url'], dest_file_path):
                count += 1

    print(f"Download complete. {count} files downloaded.")
