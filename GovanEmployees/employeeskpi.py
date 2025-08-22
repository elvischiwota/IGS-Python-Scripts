import requests
import xml.etree.ElementTree as ET
import pandas as pd
from requests.auth import HTTPBasicAuth

# API credentials
username = 'elvis@igssolutions.co.za'
password = 'epmspass@@998'

# API endpoints
employees_url = 'https://lgsa2pro.igsswebapps.net/applicationdata.svc/Employees'
ipms_url = 'https://lgsa2pro.igsswebapps.net/applicationdata.svc/IPMSSet'

# Headers
headers = {
    'Accept': 'application/xml'
}

# Helper function to parse Atom XML feed
def parse_atom_entries(xml_content):
    ns = {
        'atom': 'http://www.w3.org/2005/Atom',
        'd': 'http://schemas.microsoft.com/ado/2007/08/dataservices',
        'm': 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata'
    }
    root = ET.fromstring(xml_content)
    entries = []
    for entry in root.findall('atom:entry', ns):
        props = entry.find('atom:content/m:properties', ns)
        if props is not None:
            entry_data = {}
            for child in props:
                tag = child.tag.split('}')[-1]
                entry_data[tag] = child.text
            entries.append(entry_data)
    return entries

# Fetch Employees data
response_employees = requests.get(employees_url, auth=HTTPBasicAuth(username, password), headers=headers)
employees_data = parse_atom_entries(response_employees.text)
df_employees = pd.DataFrame(employees_data)

# Fetch IPMS data
response_ipms = requests.get(ipms_url, auth=HTTPBasicAuth(username, password), headers=headers)
ipms_data = parse_atom_entries(response_ipms.text)
df_ipms = pd.DataFrame(ipms_data)

# Clean column names
df_employees.columns = pd.Index([str(col).strip() for col in df_employees.columns])
df_ipms.columns = pd.Index([str(col).strip() for col in df_ipms.columns])

# Debug actual columns
print("Employee Columns:", df_employees.columns.tolist())
print("IPMS Columns:", df_ipms.columns.tolist())

# Adjust according to actual column name
df_merged = pd.merge(
    df_ipms,
    df_employees,
    how='left',
    left_on='EmployeeId',
    right_on='id'  # <-- replace 'id' with actual key name
)


# Select relevant fields for output
df_output = df_merged[[
    'IDPIndicatorNo',
    'FirstName',
    'LastName',
    'Department',
    'IsDisabled'
]]

# Rename for clarity
df_output = df_output.rename(columns={
    'FirstName': 'EmployeeName',
    'LastName': 'EmployeeSurname',
    'IsDisabled': 'IsEmployeeDisabled'
})

# Save to Excel
output_file = 'joined_employee_ipms_data.xlsx'
df_output.to_excel(output_file, index=False)
print(f"✅ Data exported to {output_file}")
