import os
import pymysql
import pyodbc
import configparser
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm


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

    def sql_connect_d1(self, sql):
        # 使用pyodbc連接，需安裝odbc驅動
        # 建立連接字串
        conn_str = f"""DRIVER={self.config['driver']};
                       SERVER={self.config['server']};
                       DATABASE=history_huanan;
                       UID={self.config['username']};
                       PWD={self.config['password']}"""

        df = pd.DataFrame()  # 設置初始值
        try:
            connection = pyodbc.connect(conn_str)  # 建立連接
            df = pd.read_sql(sql, connection)

            connection.close()  # 關閉連接

        except Exception as e:
            pass
            # print(f"Error: {e}")
        return df

    # update
    def update_connect(self, sql, func_type, **content):
        # 使用pyodbc連接，需安裝odbc驅動
        # 建立連接字串
        conn_str = f"""DRIVER={self.config['driver']};
                       SERVER={self.config['server']};
                       DATABASE={self.config['database']};
                       UID={self.config['username']};
                       PWD={self.config['password']}"""

        if func_type == 'S':
            rows_as_dicts = []
            try:
                conn = pyodbc.connect(conn_str)  # 建立連接
                cursor = conn.cursor()  # 建立游標
                cursor.execute(sql)

                # 獲取列信息
                columns = [column[0] for column in cursor.description]
                # 將每一行轉換為字典
                rows_as_dicts = [dict(zip(columns, row)) for row in cursor.fetchall()]

                cursor.close()  # 關閉游標
                conn.close()  # 關閉連接

            except Exception as e:
                print(f"Error: {e}")
            return rows_as_dicts
        elif func_type == 'U':
            try:
                conn = pyodbc.connect(conn_str)  # 建立連接
                cursor = conn.cursor()  # 建立游標

                # cursor.execute(sql, (content))  # 執行 UPDATE
                cursor.execute(sql)  # 執行 UPDATE
                conn.commit()  # 確認修改

                cursor.close()  # 關閉游標
                conn.close()  # 關閉連接
            except Exception as e:
                print(f"Error: {e}")


#  王品設備異常 1:冷凍 2:解凍 3:冷藏
def alarm_sql_query(table_name, title, devices_type, date):
    formula = '0'
    Probe1_string = "MAX(CASE WHEN NAME = 'Probe1' THEN VALUE END)"
    # 設備條件
    if devices_type == 1:  # 冷凍冰箱
        formula = f"{Probe1_string} > -18"
    elif devices_type == 2:  # 解凍冰箱
        formula = f"{Probe1_string} > -5"
    elif devices_type == 3:  # 冷藏冰箱
        formula = f"{Probe1_string} > 7 OR {Probe1_string} < 0"

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
                WHERE receiveTime between '{date} 03:00:00' and '{date} 09:00:00' {insert_query}
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


#  209客戶S800設備斷線
def device_disconnect():
    disconnect_sql_query = f'''SELECT a.id ,d.name, b.name, a.name, c.receiveTime, 
                                      ROUND(TIMESTAMPDIFF(SECOND, c.receiveTime, NOW()) / 60, 0)
                               FROM ems_information.devices a
                               LEFT JOIN ems_information.sites b on a.siteID = b.id
                               LEFT JOIN ems_information.status c on a.id = c.deviceID
                               LEFT JOIN ems_information.sites d on b.parent = d.id
                               WHERE a.enable = 1 
                               AND b.id not in (2,3,4,6) 
                               AND b.parent <> 0 
                               AND c.receiveTime < DATE_SUB(NOW(), INTERVAL 1 HOUR);'''
    data = DB_209().sql_connect(disconnect_sql_query)

    columns = ['設備編號', '公司', '分店', '設備名稱', '最後一筆資料時間', '已經斷線(分鐘)']
    df = pd.DataFrame(data, columns=columns)
    df = df.replace('', pd.NA)  # 將空字符串轉換為 NaN

    return df


#  空調未關警報查詢/更新(DB_31)
def getAlertList():
    # 更新/新增 alert 資料表內容
    def AlertEventUpdate(AlertContent):
        # d2_id = Alert['d2_id']
        # receiveTime = Alert['receiveTime']
        # Alert_id = Alert['id']
        #
        # sql = '''UPDATE IESS_huanan.dbo.alert
        #          SET time = getdate()
        #          WHERE device_ID = ?
        #          AND convert(date, time, 111) = convert(date, \'?\', 111)
        #          AND datediff(mi, CreateDate, getdate()) < 12 * 60
        #          AND datediff(mi, time, getdate()) < 30
        #          IF @@rowcount = 0
        #          INSERT INTO IESS_huanan.dbo.alert(device_ID, alarm_ID, time, checked, send)
        #          VALUES(?, ?, getdate(), 0, 0);'''

        update_query = '''UPDATE IESS_huanan.dbo.alert
                          SET time = getdate()
                          WHERE device_ID = {d2_id}
                          AND convert(date, time, 111) = convert(date, \'{receiveTime}\', 111)
                          AND datediff(mi, CreateDate, getdate()) < 12 * 60
                          AND datediff(mi, time, getdate()) < 30
                          IF @@rowcount = 0
                          INSERT INTO IESS_huanan.dbo.alert(device_ID, alarm_ID, time, checked, send)
                          VALUES({d2_id}, {id}, getdate(), 0, 0);'''.format(**AlertContent)
        DB_31().update_connect(update_query, 'U')

    sql = '''SELECT u1.name u1_name, u2.name u2_name, d1.name d1_name, d2.ID d2_id, 
             a.id, a.device_ID, a.type, a.timeStart, a.timeEnd, a.valueMax, a.valueMin, at.text, 
             s.receiveTime, s.DM DM, s.AI AI, s.DIO DIO, s.kW KW, s.kWh KWH, s.RA RA 
             FROM IESS_huanan.dbo.units u1 
             INNER JOIN IESS_huanan.dbo.units u2 on u1.ID = u2.parent and u1.Enable = 1 and u2.Enable = 1 
             INNER JOIN IESS_huanan.dbo.devices d1 on u2.ID = d1.unitID and d1.Enable = 1 
             INNER JOIN IESS_huanan.dbo.devices d2 on d1.ID = d2.parent and d2.Enable = 1 
             INNER JOIN IESS_huanan.dbo.alarm a on d2.ID = a.device_ID 
             INNER JOIN IESS_huanan.dbo.alarmText at on a.alarmText_ID = at.id 
             LEFT JOIN IESS_huanan.dbo.status s on a.device_ID = s.device_ID 
             WHERE a.funEnable = 1 and a.Enable = 1 and DATEDIFF(mi, s.receiveTime, GETDATE()) <= 2 
             ORDER BY u1.name, u2.name, d1.name;'''

    data = DB_31().update_connect(sql, 'S')
    for Alert in data:
        receiveDate = Alert['receiveTime'].strftime('%Y-%m-%d')
        if int(Alert['timeStart'].replace(':', '')) <= int(Alert['timeEnd'].replace(':', '')):
            timeStart = datetime.strptime(f"{receiveDate} {Alert['timeStart']}:00", '%Y-%m-%d %H:%M:%S')
            timeEnd = datetime.strptime(f"{receiveDate} {Alert['timeEnd']}:00", '%Y-%m-%d %H:%M:%S')
        else:
            timeStart = datetime.strptime(f"{receiveDate} {Alert['timeStart']}:00", '%Y-%m-%d %H:%M:%S')
            timeEnd = datetime.strptime(f"{receiveDate} {Alert['timeEnd']}:00",
                                        '%Y-%m-%d %H:%M:%S') + timedelta(days=1)

        if timeStart <= Alert['receiveTime'] <= timeEnd:
            value = int(Alert[Alert['type'].upper()])
            valueMax = int(Alert['valueMax'])
            valueMin = int(Alert['valueMin'])

            if value > valueMax or value < valueMin:
                AlertEventUpdate(Alert)


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
                    INNER JOIN IESS_huanan.dbo.units u2 on u1.ID in (2, 332) and u1.ID = u2.parent) as u, 
                    (select d1.unitID as unitID, d1.name as device, d2.ID as ID, d2.name as device2 
                    FROM IESS_huanan.dbo.devices d1 INNER JOIN IESS_huanan.dbo.devices d2 on d1.ID = d2.parent) as d,
                    (select alt.createDate, alt.time, alt.send, alm.type, at.text, alm.sendType, alm.emailAddress,
                     alm.phone, alt.id, alt.device_ID, alm.funEnable 
                    FROM IESS_huanan.dbo.alert alt INNER JOIN IESS_huanan.dbo.alarm alm on alt.alarm_ID = alm.id 
                    INNER JOIN IESS_huanan.dbo.alarmText at on alm.alarmText_ID = at.id and alm.alarmText_ID = 5) as a
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


def test():
    sql_1 = ["SELECT '101 儲蓄分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [112] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '103 城內分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1020] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '105 建成分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1984] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '106 中山分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2786] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '106 中山分行', '2F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2788] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '830 台東分行', '頂樓 14RT*2台分離式總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2110] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'"
]
    dataframes = []
    for sql in tqdm(sql_1, desc="Processing SQL Queries"):
        df1 = DB_31().sql_connect_d1(sql)
        dataframes.append(df1)

    merged_df = pd.concat(dataframes, ignore_index=True)  # 合併兩個 DataFrame
    merged_df.to_excel(r'C:\Users\GC-Rita\PycharmProjects\send_alert_table\merged_output.xlsx', index=False)  # 將合併後的 DataFrame 存儲到 Excel 檔案中
    print(merged_df)


# ----------測試區----------
if __name__ == '__main__':
    pass
    # test()
    # getAlertList()
