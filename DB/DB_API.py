import os
import pymysql
import pyodbc
import configparser
import pandas as pd


# 建立MySQL連接
class DB_209:
    # 建構式
    def __init__(self):
        config = configparser.ConfigParser()
        configPATH = './.config/config' if os.path.isfile('./.config/config') else '../.config/config'
        config.read(configPATH)

        self.config = {
            'host': config.get('DB_209', 'host'),
            'port': int(config.get('DB_209', 'port')),
            'user': config.get('DB_209', 'user'),
            'password': config.get('DB_209', 'password'),
            'charset': config.get('DB_209', 'charset'),
            'database': config.get('DB_209', 'database')
        }

    def sql_connect(self, sql):
        with pymysql.connect(
                    host=self.config['host'],
                    port=self.config['port'],
                    user=self.config['user'],
                    password=self.config['password'],
                    database=self.config['database']
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                data = cursor.fetchall()
                return data


# 建立MSSQL連接(pyodbc)
class DB_31:
    # 建構式
    def __init__(self):
        config = configparser.ConfigParser()
        configPATH = './.config/config' if os.path.isfile('./.config/config') else '../.config/config'
        config.read(configPATH)

        self.config = {
            'server': config.get('DB_31', 'server'),
            'username': config.get('DB_31', 'username'),
            'password': config.get('DB_31', 'password'),
            'driver': config.get('DB_31', 'driver'),
            'database': config.get('DB_31', 'database')
        }

    def sql_connect(self, sql):
        # 使用pyodbc連接，需安裝odbc驅動
        # 建立連接字串
        conn_str = f"""DRIVER={self.config['driver']};
                       SERVER={self.config['server']};
                       DATABASE={self.config['database']};
                       UID={self.config['username']};
                       PWD={self.config['password']}"""

        df = pd.DataFrame()  # 設置初始值
        try:
            connection = pyodbc.connect(conn_str)  # 建立連接
            df = pd.read_sql(sql, connection)

            connection.close()  # 關閉連接

        except Exception as e:
            print(f"Error: {e}")
        return df


#  王品設備異常
def alarm_sql_query(table_name, title, devices_type, date):
    formula = '0'
    Probe1_string = "MAX(CASE WHEN NAME = 'Probe1' THEN VALUE END)"
    if devices_type == 1:
        formula = f"{Probe1_string} > -18"
    elif devices_type == 2:
        formula = f"{Probe1_string} > 7 OR {Probe1_string} < 0"
    elif devices_type == 3:
        formula = f"{Probe1_string} > -5"

    insert_query = ""
    for i in range(1, 14):
        query = f""" OR (receiveTime BETWEEN DATE_SUB('{date} 03:00:00', INTERVAL {i} DAY) 
                             AND DATE_SUB('{date} 09:00:00', INTERVAL {i} DAY))"""
        insert_query += query

    sql_query = f"""
                SELECT receiveTime,
                       IFNULL(MAX(CASE WHEN NAME = 'Probe1' THEN VALUE END), NULL) AS `{title}`,
                       CASE WHEN {formula} THEN 'True' ELSE 'False' END AS Compare
                FROM ems_data.`{table_name}`
                Where receiveTime between '{date} 03:00:00' and '{date} 09:00:00' {insert_query}
                GROUP BY receiveTime
                HAVING MAX(CASE WHEN NAME = 'Probe1' THEN VALUE END) IS NOT NULL;
                """

    data = DB_209().sql_connect(sql_query)
    columns = ['receiveTime', title, 'Compare']

    df = pd.DataFrame(data, columns=columns)
    df = df.replace('', pd.NA)  # 將空字符串轉換為 NaN

    return df


def min_temperatures(devicesID, startTime, endTime):
    min_temp_sql_query = f'''SELECT MIN(CASE WHEN NAME = 'Probe1' THEN VALUE END) AS Min_Probe1
                             FROM ems_data.`{devicesID}`
                             WHERE receiveTime BETWEEN '{startTime}' AND '{endTime}' AND NAME = 'Probe1';
                         '''
    min_temperatures_result = DB_209().sql_connect(min_temp_sql_query)

    # return 最小溫度值
    return min_temperatures_result[0][0]


#  209客戶設備斷線
def device_disconnect():
    disconnect_sql_query = f'''SELECT a.id ,d.name, b.name, a.name, c.receiveTime, 
                                      ROUND(TIMESTAMPDIFF(SECOND, c.receiveTime, NOW()) / 60, 0)
                               FROM ems_information.devices a
                               LEFT JOIN ems_information.sites b on a.siteID = b.id
                               LEFT JOIN ems_information.status c on a.id = c.deviceID
                               LEFT JOIN ems_information.sites d on b.parent = d.id
                               WHERE a.enable = 1 
                               AND b.id not in (2,3,4,6,99) 
                               AND b.parent <> 0 
                               AND c.receiveTime < DATE_SUB(NOW(), INTERVAL 1 HOUR);'''
    data = DB_209().sql_connect(disconnect_sql_query)

    columns = ['設備編號', '公司', '分店', '設備名稱', '最後一筆資料時間', '已經斷線(分鐘)']
    df = pd.DataFrame(data, columns=columns)
    df = df.replace('', pd.NA)  # 將空字符串轉換為 NaN

    return df


#  空調未關警報(DB_31)
def AC_unclosed_alarm(selectTime):
    time_query = ''
    if selectTime == '0830':
        time_query = '''and CONVERT(char(19), a.time, 111) 
                        = CONVERT(char(19), dateadd(day, datediff(day, 1, GETDATE()), 0), 111) '''
    elif selectTime == '1900':
        time_query = 'and a.time >= dateadd(hour, -2, getdate()) '

    sql_query = f'''SELECT u.company, u.branch, d.device, d.device2,
                    min(a.createDate) startTime, max(a.time) endTime, count(d.device) times, a.text
                    FROM
                    (select u1.name as company, u2.ID, u2.name as branch 
                    FROM IESS_huanan.dbo.units u1 
                    inner join IESS_huanan.dbo.units u2 on u1.ID in (2, 332) and u1.ID = u2.parent) as u, 
                    (select d1.unitID as unitID, d1.name as device, d2.ID as ID, d2.name as device2 
                    FROM IESS_huanan.dbo.devices d1 inner join IESS_huanan.dbo.devices d2 on d1.ID = d2.parent) as d,
                    (select alt.createDate, alt.time, alt.send, alm.type, at.text, alm.sendType, alm.emailAddress,
                     alm.phone, alt.id, alt.device_ID, alm.funEnable 
                    FROM IESS_huanan.dbo.alert alt inner join IESS_huanan.dbo.alarm alm on alt.alarm_ID = alm.id 
                    inner join IESS_huanan.dbo.alarmText at on alm.alarmText_ID = at.id and alm.alarmText_ID = 5) as a
                    WHERE u.ID = d.unitID and d.ID = a.device_ID
                    and a.send = 0 and a.funEnable = 1 
                    {time_query} 
                    GROUP BY u.company, u.branch, d.device, d.device2, a.text;'''

    data = DB_31().sql_connect(sql_query)

    df = pd.DataFrame(data)
    df = df.rename(columns={
        'company': '公司',
        'branch': '分店',
        'device': '設備1',
        'device2': '設備2',
        'startTime': '發生時間',
        'endTime': '結束時間',
        'times': '觸發次數',
        'text': 'Text'
    })
    return df


# ----------測試區----------
if __name__ == '__main__':
    pass
    # AC_unclosed_alarm('0830')
