import pyodbc
from typing import Self
from pprint import pprint
from loguru import logger
import gsheets
import json
from datetime import datetime, date


class Sql:
    def __init__(self, path):
        self.sql_path: str = path
        with open(self.sql_path, 'r') as fp:
            sql_creds = [x.strip() for x in fp.readlines()]
        self.connect = pyodbc.connect(
                        'DRIVER={ODBC Driver 18 for SQL Server};SERVER=' +
                        sql_creds[0] + ';DATABASE=' +
                        sql_creds[1] + ';ENCRYPT=no;UID=' +
                        sql_creds[2] + ';PWD=' +
                        sql_creds[3])
        logger.info("We've authorized")

    def select_get_data(self, query: str) -> list:
        db_cursor = self.connect.cursor()
        logger.info("Cursor have installed")
        result_query = db_cursor.execute(query)
        if result_query:
            logger.info("Query have done")
        lista=[list(x) for x in db_cursor.fetchall()]
        db_cursor.close()
        logger.info("Data have be saved in list")
        return lista

def get_last_week() -> str:
    num_last_week = datetime.today().isocalendar()[1] - 1
    current_year = date.today().year
    start = date.fromisocalendar(current_year, num_last_week, 1).strftime('%d.%m')
    end = date.fromisocalendar(current_year, num_last_week, 7).strftime('%d.%m')
    return start + ' - ' + end

def main():
    SQL_QUERY = """
                DECLARE @Week_Start_Date DATE, @Week_End_Date DATE
                SET @Week_Start_Date = DATEADD(DAY, -5 - DATEPART(WEEKDAY, GETDATE()), CAST(GETDATE() AS DATE))
                SET @Week_End_Date = DATEADD(DAY, 2 - DATEPART(WEEKDAY, GETDATE()), CAST(GETDATE() AS DATE))
                SELECT
                    convert(varchar, [viCDRSummary].[OfferedTime], 20) as DataTime
                    ,'https://mskais.sfcs.cc/cosmocorder/CallDetail.aspx?callid=0x' + convert(varchar(max), [viCDRSummary].[Call_ID], 2) as Call_ID
                    ,case
                        when [viCDRSummary].[CallDirection_ID] = 0 then [viCDRSummary].[FromParty]
                        when [viCDRSummary].[CallDirection_ID] = 1 then [viCDRSummary].[ToParty]
                        end as Phone
                    ,[MsgAgentConnected].[AgentFirstName]
                    ,[MsgAgentConnected].[AgentLastName]
                    ,case
                    when [viCDRSummary].[CallDirection_ID] = 0 then 'in'
                    when [viCDRSummary].[CallDirection_ID] = 1 then 'out'
                    end as Direction
                    ,[AgentConfiguration].[GroupName]
                    ,[evaluation].[eval] as Eval
                
                FROM
                    [viCDRSummary]
                    join [evaluation] on [evaluation].[newcallid] = [viCDRSummary].[Call_ID]
                    join [MsgAgentConnected] on [MsgAgentConnected].[Call_ID] = [viCDRSummary].[Call_ID]
                    join [AgentConfiguration] on [AgentConfiguration].[AgentGlobal_ID] = [MsgAgentConnected].[AgentGlobal_ID]
                
                WHERE
                    [viCDRSummary].[OfferedTime] between @Week_Start_Date and @Week_End_Date
                    and [AgentConfiguration].[GroupName] = 'Мобильное приложение'
                    and [evaluation].[eval] is not null
                    and [MsgAgentConnected].[BindType_ID] in (0, 6)
                    and [AgentConfiguration].[Reason_ID] = 0
                
                ORDER BY
                    [viCDRSummary].[OfferedTime]
                """
    #dbCursor.execute(SQL_QUERY)
    #pprint(dbCursor)

    sql = Sql(r"C:\Projects\Python\Credentials\sql_connection_string.txt")
    result_list = sql.select_get_data(SQL_QUERY)
    #pprint(result_list)

    google_cred = r"C:\Projects\Python\Credentials\gsheets.json"
    sheet_id = '10Yxmtk_YgFcubmz_8a9ezSSfOQJ0fgPCSiRJ3HqmFU4'
    md_g = "ROWS"
    gsheets_data = gsheets.Gsheets(google_cred, sheet_id, md_g)

    name_list = get_last_week()
    row_count = len(result_list)
    column_count = len(result_list[0])
    gsheets_data.range_g = f"{name_list}!A1:{chr(ord('@') + len(result_list[0]))}{len(result_list)}"
    gsheets_data.add_list(name_list, row_count, column_count)

    gsheets_data.table_data = {
        "valueInputOption": "USER_ENTERED",
        "data": [
            {"range": gsheets_data.range_g,
             "majorDimension": md_g,
             "values": result_list}
        ]
    }
    gsheets_data.update_tables()
    test = ""
    #pprint(gsheets_data.table_data)

if __name__ == "__main__":
    main()


