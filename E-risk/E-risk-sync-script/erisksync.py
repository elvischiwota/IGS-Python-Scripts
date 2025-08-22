import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import urllib.parse

# Fetch data from API
def fetch_data(endpoint):
    url = f"https://eriskpro.igsswebapps.net/applicationdata.svc/{endpoint}"
    response = requests.get(url, auth=("admin2", "Admin@123"), headers={"Accept": "application/json"})
    if response.status_code == 200:
        return pd.DataFrame(response.json()['value'])
    else:
        raise Exception(f"Failed to fetch {endpoint}: {response.status_code}")

# Fetch all required tables
print("Fetching data...")
additional_actions = fetch_data("AddtionalActions").rename(columns={'Id': 'Action_ID'})
erms = fetch_data("ERMs").rename(columns={'Id': 'ERM_Id'})
departments = fetch_data("Departments").rename(columns={'Id': 'Department_Id'})
employees = fetch_data("Employees").rename(columns={'Id': 'Employee_Id'})
positions = fetch_data("Positions").rename(columns={'Id': 'Position_Id'})
likelihoods = fetch_data("Likelihoods").rename(columns={'Id': 'Likelihood_Id'})
potentialimpacts = fetch_data("Potentialimpacts").rename(columns={'Id': 'Impact_Id'})
ipms_strategic_goals = fetch_data("IpmsStrategicGoals").rename(columns={'Id': 'StrategicGoal_Id'})
municipalities = fetch_data("Municipalities").rename(columns={'Id': 'Municipality_Id'})
risk_register_names = fetch_data("RiskRegisterNames").rename(columns={'Id': 'RiskRegister_Id'})
action_responses = fetch_data("ActionResponses")

# Build the base view
print("Building ViewRiskReport...")
view = additional_actions.merge(
    erms, left_on='AddtionalAction_ERM', right_on='ERM_Id', how='left'
).merge(
    departments[['Department_Id', 'Name']],
    left_on='AddtionalAction_Department', right_on='Department_Id', how='left'
).rename(columns={'Name': 'Department'}
).merge(
    employees[['Employee_Id', 'FirstName', 'LastName', 'Employee_Position']],
    left_on='AddtionalAction_Employee', right_on='Employee_Id', how='left'
).merge(
    positions[['Position_Id', 'Title']],
    left_on='Employee_Position', right_on='Position_Id', how='left'
).rename(columns={'Title': 'Action Owner'}
).merge(
    potentialimpacts[['Impact_Id', 'ImpactFactor']],
    left_on='ERM_Potentialimpact', right_on='Impact_Id', how='left'
).rename(columns={'ImpactFactor': 'InherentImpactFactor'}
).merge(
    potentialimpacts[['Impact_Id', 'ImpactFactor']],
    left_on='ERM_Potentialimpact1', right_on='Impact_Id', how='left'
).rename(columns={'ImpactFactor': 'ResidualImpactFactor'}
).merge(
    likelihoods[['Likelihood_Id', 'LikelihoodFactor']],
    left_on='ERM_Likelihood', right_on='Likelihood_Id', how='left'
).rename(columns={'LikelihoodFactor': 'InherentLikelihoodFactor'}
).merge(
    likelihoods[['Likelihood_Id', 'LikelihoodFactor']],
    left_on='ERM_Likelihood2', right_on='Likelihood_Id', how='left'
).rename(columns={'LikelihoodFactor': 'ResidualLikelihoodFactor'}
).merge(
    ipms_strategic_goals[['StrategicGoal_Id', 'Title']],
    left_on='ERM_IpmsStrategicGoal', right_on='StrategicGoal_Id', how='left'
).rename(columns={'Title': 'StrategicGoal'}
).merge(
    municipalities[['Municipality_Id', 'Name']],
    left_on='ERM_Municipality', right_on='Municipality_Id', how='left'
).rename(columns={'Name': 'Municipality'}
).merge(
    risk_register_names[['RiskRegister_Id', 'Title']],
    left_on='ERM_RiskRegisterName', right_on='RiskRegister_Id', how='left'
).rename(columns={'Title': 'RiskRegister'})

# Get the latest responses
def get_latest(df, col_id, date_col, condition):
    latest = df[condition].copy()
    latest['rank'] = latest.groupby(col_id)[date_col].rank(method='first', ascending=False)
    return latest[latest['rank'] == 1].drop(columns='rank')

latest_submitted = get_latest(
    action_responses, 'ActionResponse_AddtionalAction', 'SubmittedDate',
    (action_responses['Comments'].notna()) & (action_responses['Submitted'] == 1)
)
latest_approval = get_latest(
    action_responses, 'ActionResponse_AddtionalAction', 'ApproveStatusDate',
    action_responses['ApproveComment'].notna()
)
latest_panel = get_latest(
    action_responses, 'ActionResponse_AddtionalAction', 'StatusDate',
    action_responses['PanelComment'].notna()
)

# Merge responses
response = pd.merge(latest_submitted, latest_approval, on='ActionResponse_AddtionalAction', how='outer', suffixes=('', '_approval'))
response = pd.merge(response, latest_panel, on='ActionResponse_AddtionalAction', how='outer', suffixes=('', '_panel'))
response['Progress to Date'] = response['ApproveComment'].combine_first(response['Comments'])

# Merge response into view
view = view.merge(response, left_on='Action_ID', right_on='ActionResponse_AddtionalAction', how='left')

# Format and calculate
view['EndDateOfActionPlan'] = view['EndDateOfActionPlan'].astype(str).str.split('.').str[0]
view['Due Date'] = pd.to_datetime(view['EndDateOfActionPlan'], errors='coerce').dt.strftime('%A, %B %d, %Y')
view['Inherent Risk'] = pd.to_numeric(view['InherentLikelihoodFactor'], errors='coerce') * pd.to_numeric(view['InherentImpactFactor'], errors='coerce')
view['Residual Risk'] = pd.to_numeric(view['ResidualLikelihoodFactor'], errors='coerce') * pd.to_numeric(view['ResidualImpactFactor'], errors='coerce')
view['Risk Owner'] = view['Owner']

# Rename
final = view.rename(columns={
    'Title_x': 'Risk No', 'Description': 'Risk Or Threat', 'RootCause': 'Root Cause',
    'ActionPlan_x': 'Action Plan', 'MeansOfVerification': 'Means Of Verification',
    'ActionStatus': 'ActionStatus', 'VarianceReason': 'VarianceReason',
    'RemedialActionSubmit': 'Remedial Action', 'Title_y': 'Action Owner',
    'PanelComment_panel': 'CRO Comments', 'ToYear': 'ToYear', 'FromYear': 'FromYear',
    'Municipality': 'Municipality'
})

columns = [
    'Risk No', 'Risk Or Threat', 'Root Cause', 'Inherent Risk', 'Residual Risk',
    'InherentLikelihoodFactor', 'ResidualLikelihoodFactor',
    'InherentImpactFactor', 'ResidualImpactFactor',
    'Action Plan', 'Due Date', 'Means Of Verification', 'Progress to Date',
    'ActionStatus', 'VarianceReason', 'Remedial Action', 'Risk Owner',
    'Action Owner', 'CRO Comments', 'ToYear', 'FromYear', 'Department',
    'RiskRegister', 'Municipality', 'ActionPlanType', 'ERM_Id', 'Action_ID'
]

final = final[columns]
# Filter only rows where ToYear == current year
from datetime import datetime
current_year = datetime.now().year
final = final[final['ToYear'] == current_year]

# Trim to first row only
first_row_df = final.head(1)

# Create connection string for Azure SQL Database
params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=dev1epms.database.windows.net;"
    "DATABASE=ERISK-Copy-IGS;"
    "UID=dev1user;"
    "PWD=dev1@emps@db;"
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

def upsert_using_merge_batch(df, table_name, engine, key_columns, chunk_size=100):
    with engine.begin() as connection:
        df = df.replace({np.nan: None})
        for start in range(0, len(df), chunk_size):
            chunk = df.iloc[start:start + chunk_size]
            for _, values in chunk.iterrows():
                values = values.to_dict()
                columns = list(values.keys())
                set_columns = [col for col in columns if col not in key_columns]
                merge_sql = f"""
                MERGE [{table_name}] AS target
                USING (
                    SELECT {', '.join([f':p{i} AS [{col}]' for i, col in enumerate(columns, 1)])}
                ) AS source
                ON ({' AND '.join([f'target.[{col}] = source.[{col}]' for col in key_columns])})
                WHEN MATCHED THEN
                    UPDATE SET
                        {', '.join([f'target.[{col}] = source.[{col}]' for col in set_columns])}
                WHEN NOT MATCHED THEN
                    INSERT ({', '.join([f'[{col}]' for col in columns])})
                    VALUES ({', '.join([f':p{i + len(columns)}' for i in range(1, len(columns) + 1)])});
                """
                params = {}
                for i, val in enumerate(values.values(), 1):
                    params[f'p{i}'] = val
                    params[f'p{i + len(columns)}'] = val
                connection.execute(text(merge_sql), params)

# Save all rows in chunks (default chunk_size=100)
print("Saving all rows to database in batches...")
key_columns = ['Action_ID']
upsert_using_merge_batch(final, "ViewRiskReport_Table", engine, key_columns)
print("✅ All rows saved with batch upsert.")