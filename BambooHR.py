import pandas as pd
import pyodbc
import urllib
from sqlalchemy import create_engine
import requests
import base64
import json
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials


# BambooHR subdomain and API key
subdomain = 'quantanite'
# api_key = '93bf2c145024d6f57c76c874833eb3af9ce98719'
api_key = '24de5912d60f80f44ec6d309b11f56186324bdd7'

# BambooHR API endpoint
url = f"https://api.bamboohr.com/api/gateway.php/{subdomain}/v1/reports/custom?format=json"
# url = f'https://api.bamboohr.com/api/gateway.php/{subdomain}/v1/reports/763'
# url = f'https://api.bamboohr.com/api/gateway.php/{subdomain}/v1/employees/directory'

# Set the API key for authorization
headers = { "accept": "application/json",
    'Authorization': 'Basic ' + base64.b64encode(f'{api_key}:x'.encode()).decode()
}

payload  = {
    "fields": [
        'employeeNumber',
        'fullName2',
        'workEmail',
        'homeEmail',
        'mobilePhone',
        '91',
        'jobTitle',
        'location',
        'department',
        'dateOfBirth',
        'gender',
        'customShiftTime',
        'hireDate',
        'status',
        'terminationDate',
        '4314',
        'employmentHistoryStatus',
        'customCampaignStatus',
        'location1',
        'count'
    ]
}

# response = requests.get(url, headers=headers, verify= False)
response = requests.post(url, json=payload, headers=headers, verify= False)
# print(json.loads(response.text))
# print(type(json.loads(response.text)))
# print(response.json)
dict_1 = json.loads(response.text)

# json to csv
df = pd.DataFrame(dict_1['employees'])
# df.to_excel(r'E:\Office work\DB\export_bambooHR.xlsx', index=False)
# print(dict_1['fields'])
df.rename(columns = {'employeeNumber': 'Employee #', 
                     'fullName2':'First name Last name',
                     'workEmail':'Work Email',
                     'homeEmail': 'Home Email',
                    'mobilePhone': 'Mobile Phone',
                    '91' : 'Reporting to',
                    'jobTitle': 'Job Title',
                    'location' : 'Location',
                    'department': 'Department',
                    'dateOfBirth' : 'Birth Date',
                    'gender' : 'Gender',
                    'customShiftTime': 'Shift Type',
                    'hireDate': 'Hire Date',
                    'status': 'Status',
                    'terminationDate': 'Termination Date',
                    '4314': 'Termination Reason',
                    'employmentHistoryStatus': 'Employment Status',
                    'customCampaignStatus': 'Campaign Status',
                    # 'location': 'Place',
                    # 'count': 'Count'
                    }, inplace = True)

# # Step 1: Read and Clean the CSV Data
# df = pd.read_csv('E:\Office work\DB\BambooHRManualDump.csv')

df['Mobile Phone'] = df['Mobile Phone'].str.extract('(\d+)').astype(float)

# 2.2. Clean country prefixes in Col S (assuming it's the 'Location' column)
country_prefixes = {
    'SA': 'South Africa',
    'UK': 'United Kingdom',
    'Bangladesh': 'Bangladesh',
    'Bulgaria': 'Bulgaria',
    'US': 'United States',
    'Dhaka': 'Bangladesh', # Add more mappings as needed
     }
df['Place'] = df['Location'].map(country_prefixes)
# df['Count'] = df.groupby('Work Email')
# print(df['Count'])
# 2.3. Check for duplicate entries based on Col T (assuming it's the 'Count' column)
# df.drop_duplicates(subset=['Count'], keep='first', inplace=True)
# print(df)
# Generate Excel with Data Filtering and Sorting
filtered_df = df[df['Place'].isin(['Bangladesh','South Africa',
                                      'United Kingdom','Bulgaria','United States','Bangladesh'])]
# Sort the filtered DataFrame by a specific column (e.g., 'Count')
df_11 = filtered_df.sort_values(by='Work Email')
# df_11.drop_duplicates(subset=['Work Email'], keep='first', inplace=True)
df_12 = pd.DataFrame(data=df_11['Work Email'].value_counts().to_frame(name='Count'))
df_21 = (df_11.merge(df_12, left_on='Work Email', right_on='Work Email'))
df_21.drop('id', inplace= True, axis = 1)
# # Step 2: Connect to Microsoft Access
conn_str = (
    r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
    r"DBQ=D:\Office work\DB\ACCESS_DATA.accdb"
)
connection_url = f"access+pyodbc:///?odbc_connect={urllib.parse.quote_plus(conn_str)}"
acc_engine = create_engine(connection_url)
print("Connection Establish")
print("Data insert in DataBase ..........")
df_21.to_sql('BambooHRManualDump', acc_engine, if_exists='replace', index=False)
print("Data complete....")

# Generate Excel
# df_21.to_excel(r'D:\Office work\DB\export_bambooHR.xlsx', index=False)
# print("Excel file is exported......")

print("Before google sheet")
# Authenticate with Google Sheets API using your credentials JSON file
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('./bamboohr-405206-512d9252123a.json', scope)
client = gspread.authorize(creds)

# Open the Google Sheet by title (replace with your actual title)
sheet = client.open('Employee Info Master DB').sheet1
# Clear existing data in the Google Sheet (optional)
sheet.clear()
print("Sheet Clear")
# sheet.worksheet_by_title(sheet_name)
# sheet.set_dataframe(df_21, (1,1), encoding='utf-8', fit=True)
# sheet.frozen_rows = 1
set_with_dataframe(worksheet=sheet, dataframe=df_21, include_index=False,
include_column_header=True, resize=True)
print("Google sheet complete")
