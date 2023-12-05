import pandas as pd
import requests
import base64
import json
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine
import urllib

class BambooHRDataProcessor:
    def __init__(self, subdomain, api_key):
        self.subdomain = subdomain
        self.api_key = api_key

    def fetch_bamboo_data(self):
        url = f"https://api.bamboohr.com/api/gateway.php/{self.subdomain}/v1/reports/custom?format=json"
        headers = {
            "accept": "application/json",
            'Authorization': 'Basic ' + base64.b64encode(f'{self.api_key}:x'.encode()).decode()
        }

        payload = {
            "fields": [
                'employeeNumber', 'fullName2', 'workEmail', 'homeEmail', 'mobilePhone',
                '91', 'jobTitle', 'location', 'department', 'dateOfBirth', 'gender',
                'customShiftTime', 'hireDate', 'status', 'terminationDate', '4314',
                'employmentHistoryStatus', 'customCampaignStatus', 'location1', 'count'
            ]
        }

        response = requests.post(url, json=payload, headers=headers, verify=False)
        return json.loads(response.text)['employees']

class DataProcessor:
    @staticmethod
    def clean_data(df):
        df['Mobile Phone'] = df['Mobile Phone'].str.extract('(\d+)').astype(float)

        country_prefixes = {
            'SA': 'South Africa', 'UK': 'United Kingdom', 'Bangladesh': 'Bangladesh',
            'Bulgaria': 'Bulgaria', 'US': 'United States', 'Dhaka': 'Bangladesh'
        }
        df['Place'] = df['Location'].map(country_prefixes)

        filtered_df = df[df['Place'].isin(['Bangladesh', 'South Africa', 'United Kingdom', 'Bulgaria', 'United States', 'Bangladesh'])]
        df_11 = filtered_df.sort_values(by='Work Email')
        df_12 = pd.DataFrame(data=df_11['Work Email'].value_counts().to_frame(name='Count'))
        df_21 = df_11.merge(df_12, left_on='Work Email', right_on='Work Email')
        df_21.drop('id', inplace=True, axis=1)

        return df_21

class DataExporter:
    @staticmethod
    def to_access(df, access_db_path):
        conn_str = (
            r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
            f"DBQ={access_db_path}"
        )
        connection_url = f"access+pyodbc:///?odbc_connect={urllib.parse.quote_plus(conn_str)}"
        acc_engine = create_engine(connection_url)

        df.to_sql('BambooHRManualDump', acc_engine, if_exists='replace', index=False)

    @staticmethod
    def to_google_sheet(df, credentials_path, sheet_title):
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        client = gspread.authorize(creds)

        sheet = client.open(sheet_title).sheet1
        sheet.clear()

        set_with_dataframe(worksheet=sheet, dataframe=df, include_index=False,
                           include_column_header=True, resize=True)

def main():
    bamboo_processor = BambooHRDataProcessor(subdomain='quantanite', api_key='24de5912d60f80f44ec6d309b11f56186324bdd7')
    bamboo_data = bamboo_processor.fetch_bamboo_data()

    df = pd.DataFrame(bamboo_data)
    df.rename(columns={'employeeNumber': 'Employee #', 'fullName2': 'First name Last name',
                       'workEmail': 'Work Email', 'homeEmail': 'Home Email',
                       'mobilePhone': 'Mobile Phone', '91': 'Reporting to',
                       'jobTitle': 'Job Title', 'location': 'Location',
                       'department': 'Department', 'dateOfBirth': 'Birth Date',
                       'gender': 'Gender', 'customShiftTime': 'Shift Type',
                       'hireDate': 'Hire Date', 'status': 'Status',
                       'terminationDate': 'Termination Date', '4314': 'Termination Reason',
                       'employmentHistoryStatus': 'Employment Status',
                       'customCampaignStatus': 'Campaign Status'}, inplace=True)

    cleaned_df = DataProcessor.clean_data(df)

    access_db_path = 'D:\\Office work\\DB\\ACCESS_DATA.accdb'
    DataExporter.to_access(cleaned_df, access_db_path)

    google_sheet_credentials_path = './bamboohr-405206-512d9252123a.json'
    sheet_title = 'Employee Info Master DB'
    DataExporter.to_google_sheet(cleaned_df, google_sheet_credentials_path, sheet_title)

if __name__ == "__main__":
    main()


print("data done")