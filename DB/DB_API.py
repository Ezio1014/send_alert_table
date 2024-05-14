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
"SELECT '107 圓山分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1995] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '108 城東分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1960] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '108 城東分行', '2F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1962] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '109 西門分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [777] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '111 仁愛路分行', 'L1 總用電(1F右)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2799] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '111 仁愛路分行', 'LC11 總用電(1F左)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2805] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '111 仁愛路分行', 'EL1 總用電(1F左)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2807] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '111 仁愛路分行', 'EL1 總用電(1F右)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2801] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '111 仁愛路分行', 'P1 空調總用電(1F右)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2803] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '112 南京東路分行', '總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2775] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '112 南京東路分行', '2F 60RT 冰水機兩台總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2777] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '113 新生分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [434] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '114 大同分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1950] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '115 松山分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [87] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '116 中崙分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [75] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '117 南門分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1539] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '117 南門分行', '2F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2325] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '118 公館分行(1F 216-1號)', '1F 總用電(216-1號)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1775] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '118 公館分行(1F 216號)', '1F 總用電(216號)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1773] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '118 公館分行(2F 216-1號)', '2F 總用電(216-1號)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1771] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '118 公館分行(2F 216號)', '2F 總用電(216號)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1559] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '119 信義分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2006] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '120 忠孝東路分行', '2F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2017] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '121 和平分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2435] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '122 雙園分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [117] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '123 士林分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1921] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '124 東台北分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [3123] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '125 大安分行', '1F空調總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [424] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '125 大安分行', 'B1 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [3001] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '126 民生分行', 'B1 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2754] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '127 復興分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [924] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '128 龍江分行(1F右總 145號)', '1F 右總用電(145號)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1777] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '128 龍江分行(1F左總 143號)', '1F 左總用電(143號)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1570] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '128 龍江分行(2F右總 145號)', '2F 右總用電(145號)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1779] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '129 永吉分行(1F)', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [484] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '129 永吉分行(2F)', '2F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [3005] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '130 敦化分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [491] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '132 大直分行(1F 52號)', '1F 總用電 (52號)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [3013] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '132 大直分行(1F 56號)', '1F 總用電 (56號)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [696] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '133 敦和分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [429] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '134 東湖分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1581] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '136 東興分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [461] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '139 板橋文化分行(1F總)', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [766] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '143 南內湖分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1031] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '145 長安分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [933] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '147 懷生分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1041] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '148 中華路分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1592] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '149 信維分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [496] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '151 埔墘分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [439] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '152 石牌分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [345] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '153 瑞祥分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2736] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '154 台大分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [394] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '156 世貿分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [436] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '157 萬華分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [477] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '158 南港分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1603] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '159 華江分行', '空調總電(=8台VRV總用電)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [4886] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '159 華江分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [3293] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '160 板橋分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2980] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '161 三重分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [720] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '162 北三重分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [3283] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '163 新莊分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2205] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '163 新莊分行', '1F 空調總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [99] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '164 永和分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1614] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '165 中和分行', 'B1 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2446] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '166 新店分行', '總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [95] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '167 淡水分行', '總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1973] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '167 淡水分行', '1F空調總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1975] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '168 汐止分行', '1F-4F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [80] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '169 南永和分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [966] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '170 西三重分行(1&2F)', '1&2F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [686] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '170 西三重分行(3F)', '3F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [3003] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '171 南三重分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [755] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '172 雙和分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [372] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '172 雙和分行', '空調總用電(1+2F)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [374] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '173 新泰分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [97] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '173 新泰分行', '(空調總用電)VRV系統', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [98] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '174 二重分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2969] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '175 板新分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [3295] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '176 五股分行', '總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [5918] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '177 潭美分行', '總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [6058] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '178 北投分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1625] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '179 西湖分行', '1F 空調總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1053] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '179 西湖分行', '1F 總用電(右)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2323] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '179 西湖分行', '1F 總用電(左)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1051] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '180 積穗分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [105] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '182 福和分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [501] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '182 福和分行', '空調總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [503] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '183 南勢角分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [383] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '184 北蘆洲分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [472] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '185 蘆洲分行(1F)', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [978] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '185 蘆洲分行(2F)', '2F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [3015] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '186 土城分行', '總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [73] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '186 土城分行', '1F空調總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [74] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '186 土城分行', '2F空調總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1914] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '187 北新分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [405] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '189 天母分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [821] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '189 天母分行', '1F/2F分離式 空調總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [823] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '190 內湖分行(1F 157號)', '1F 總用電 (157號)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [3007] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '190 內湖分行(1F 159號)', '1F 總用電 (159號)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [701] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '190 內湖分行(2F 157號)', '2F 總用電 (157號)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [3009] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '191 樹林分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [103] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '192 樟樹灣分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [100] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '193 泰山分行(1F)', '1F 空調總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [790] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '193 泰山分行(1F)', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [788] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '194 三峽分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [71] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '195 文山分行(1F)', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1636] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '195 文山分行(2F)', '2F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1781] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '196 鶯歌分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [799] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '197 北新莊分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [987] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '198 北土城分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [449] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '199 林口站前分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [454] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '200 基隆分行(1F 公共用電)', '1F 總用電(公共用電)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2030] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '200 基隆分行(B1+1-4F 總)', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2028] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '201 基隆港口分行', 'B1 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1009] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '211 七堵分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1647] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '220 羅東分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2351] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '221 宜蘭分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2340] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '240 桃園分行', 'B1 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [91] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '241 中壢分行', 'B1 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2194] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '241 中壢分行', 'B1 空調總用電(ACP)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2196] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '242 楊梅分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2417] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '243 壢昌分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2125] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '244 北桃園分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2136] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '244 北桃園分行', '頂樓 20RT冰水機 2台總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2138] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '245 南崁分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2147] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '246 平鎮分行', '1F 總用電 (外面)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2158] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '247 八德分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2169] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '248 龜山分行', '總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [108] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '249 龍潭分行(1&2F)', '1&2F總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1128] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '249 龍潭分行(1&2F)', '頂樓 20RT+15RT 冰水機空調總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1130] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '249 龍潭分行(3F)', '3F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2275] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '250 大溪分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1139] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '251 內壢分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2393] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '252 林口分行(1F)', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2744] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '252 林口分行(2F)', '2F總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [4665] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '252 林口分行(3F)', '3F總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [4667] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '252 林口分行(4F)', '4F總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [4669] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '260 觀音分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2409] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '262 大園分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2377] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '268 青埔分行', '總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [6121] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '300 新竹分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2238] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '301 竹東分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2310] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '302 竹科分行', '1F 空調分離式總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [84] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '302 竹科分行', '1F總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [83] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '310 新豐分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2401] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '313 六家分行(1F)', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2425] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '313 六家分行(2F)', '2F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2426] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '320 竹南分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2369] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '321 頭份分行', '1F 總用電(新增)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2765] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '322 苗栗分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2361] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '323 竹北分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2249] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '351 大眾分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2265] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '400 豐原分行', 'B1 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [4400] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '401 東勢分行', '1F 總用電(外)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [998] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '402 清水分行', 'B1 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [879] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '403 西豐原分行', 'B2 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1106] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '422 南台中分行', 'B2 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [89] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '423 北台中分行(1F)', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2070] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '423 北台中分行(2F)', '2F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2072] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '424 中港路分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2207] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '425 大里分行', 'B1 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2052] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '426 水湳分行', '2F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [913] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '427 五權分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2041] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '429 大甲分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [3297] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '430 太平分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2227] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '431 中科分行', '2F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1095] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '451 沙鹿分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1073] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '500 草屯分行', '1F總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [3299] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '501 南投分行', 'B1 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1178] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '520 彰化分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1189] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '521 和美分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1200] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '522 員林分行', 'B1 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1211] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '523 鹿港分行', '1F 總用電(外)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1240] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '524 溪湖分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1251] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '540 斗六分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [506] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '542 西螺分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [467] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '600 嘉義分行', '1F總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [630] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '601 嘉南分行', 'B1總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [641] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '602 朴子分行', '1F總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [652] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '620 新營分行', 'B1 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1084] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '621 麻豆分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1062] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '622 永康分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1117] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '642 東台南分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1470] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '643 西台南分行', 'B1 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1459] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '644 北台南分行', 'B1 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1448] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '645 南都分行(1F)', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2123] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '645 南都分行(2F)', '2F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2122] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '646 安南分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1404] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '647 仁德分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1415] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '648 新市分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1426] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '681 金華分行', '1F 空調總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [322] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '681 金華分行', '總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [320] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '700 高雄分行', 'B2 空調總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1879] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '700 高雄分行', 'B2 總用電(1-3F)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [3332] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '701 東苓分行(1F )', '1F  總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1864] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '701 東苓分行(B1F)', 'B1 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [3334] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '702 新興分行', 'B1 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [859] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '703 三民分行', 'B1 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [676] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '704 苓雅分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1336] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '705 前鎮分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1325] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '706 博愛分行', '總用電2', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1805] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '707 南高雄分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1314] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '707 南高雄分行', '1F-2F空調 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1316] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '708 東高雄分行', 'B1總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [4557] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '709 大昌分行', 'B1 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [839] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '710 北高雄分行', '1F總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [4398] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '710 北高雄分行', '2F總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2745] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '711 楠梓分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1292] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '712 左營分行', '1F總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [3313] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '719 岡山分行', '1F總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [4396] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '720 鳳山分行(1-3F)', 'B1 總用電(1-3F)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1888] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '720 鳳山分行(4-5F)', '4F 總用電(4-5F)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2773] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '721 路竹分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1281] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '722 仁武分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [3336] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '751 籬仔內分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [902] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '752 五甲分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1369] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '753 光華分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1347] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '760 小港分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1920] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '765 桂林分行(1F 46號)', '1F 總用電(46號)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1903] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '765 桂林分行(2F 44號)', '2F 總用電(44號)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1901] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '800 屏東分行', '1F分離式總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1840] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '800 屏東分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1838] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '800 屏東分行', '2F分離式總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [5203] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '801 內埔分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1816] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '802 潮州分行(1-3F)', 'B1 總用電(1-3F)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1849] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '802 潮州分行(1-3F)', 'B1 空調總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1853] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '802 潮州分行(公設)', 'B1 總用電(公設)', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1851] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '813 佳冬分行', '1F 總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [1827] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '820 花蓮分行', '1&2F總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2091] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '820 花蓮分行', '3F總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2093] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '820 花蓮分行', '1F 空調總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2099] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
"SELECT '830 台東分行', '1F及3F總用電', ROUND((MAX(kWh) - MIN(kWh)),2) AS '20:00-24:00' FROM [2108] WITH (NOLOCK) WHERE receiveTime BETWEEN '2024-03-22 20:00:00.000' AND '2024-03-23 00:00:00.000'",
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
