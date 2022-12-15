import httplib2
from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials
from typing import Self
from loguru import logger


class Gsheets:
    def __init__(self, path: str, gsheets_id: str, md_g: str):
        self.credentials_file: str = path
        self.spreadsheet_id: str = gsheets_id
        self.majorDimension_g: str = md_g
        self.range_g: str | None = None
        self.table_data: dict | None = None

    def auth(self) -> Self:
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.credentials_file,
            ['https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive'])
        httpAuth = credentials.authorize(httplib2.Http())
        self = discovery.build('sheets', 'v4', http=httpAuth)
        return self

    def get_tables(self):
        values = self.auth().spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=self.range_g,
            majorDimension=self.major_dimension_g
        ).execute()
        return values

    def update_tables(self):
        values = self.auth().spreadsheets().values().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body=self.table_data
        ).execute()
        logger.info("List have updated")

    def add_list(self, name_list: str, row_count: int, column_count: int):
        values = self.auth().spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body= {
                "requests": [
                    {
                        "addSheet": {
                            "properties": {
                                "title": name_list,
                                    "gridProperties": {
                                        "rowCount": row_count,
                                        "columnCount": column_count
                                    }
                                }
                            }
                        }
                    ]
                    }).execute()
        logger.info("List have created")

def main():
    gsheets_data = Gsheets(
        r'D:\006 Development\Python Projects\security-depatment-os-employers - optimizied\Credential\gsheets.json',
        '1h6XcqwUwLbiwYOlJSFvWXzSZ8DFfMmqkKc2p1Mjjkx8', 'COLUMNS')
    gsheets_data.range_g = 'A1:C10'
    gsheets_data.table_data = {
        "valueInputOption": "USER_ENTERED",
        "data": [
            {"range": "B3:C4",
             "majorDimension": "ROWS",
             "values": [["This is B3", "This is C3"], ["This is B4", "This is C4"]]},
            {"range": "D5:E6",
             "majorDimension": "COLUMNS",
             "values": [["This is D5", "This is D6"], ["This is E5", "=5+5"]]}
        ]
    }
    gsheets_data.update_tables()

if __name__ == '__main__':
    main()
