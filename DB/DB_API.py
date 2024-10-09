import os
import pymysql
import pyodbc
import configparser
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
import logging


# log setting
def setup_loggers(log_folder='Log', log_files=None):
    """
    設置多個日誌文件日誌記錄器
    :param log_folder: 日誌文件夾路徑
    :param log_files: 包含日誌名稱和文件名的字典
    :return: 包含所有日誌記錄器的字典
    """
    # 獲取當前腳本所在的目錄
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # 將路徑再向上一層，獲取父目錄
    parent_dir = os.path.dirname(current_dir)

    # 設定log文件夾的路徑為父目錄下的 log 資料夾
    log_folder = os.path.join(parent_dir, 'log')

    # 定義多個日誌文件名稱
    log_files = {
        'alarm_DM_log': 'alarm_DM_log.txt',
        'alarm_AC_Err': 'alarm_AC_Err.txt',
        'alarm_device_Run': 'alarm_device_Run.txt',
        'alarm_EV_CO2': 'alarm_EV_CO2.txt',
        'alarm_Water_TFV': 'alarm_Water_TFV.txt'
    }

    # 檢查 Log 資料夾是否存在，如果不存在則創建
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    # 創建一個函數來設置每個日志文件的 logging 配置
    def setup_logger(log_name, log_file, level=logging.ERROR):
        logger = logging.getLogger(log_name)
        logger.setLevel(level)

        # 防止重複添加 handler
        if not logger.handlers:
            handler = logging.FileHandler(os.path.join(log_folder, log_file))
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    # 設置每個日志文件
    loggers = {}
    for key, file_name in log_files.items():
        loggers[key] = setup_logger(key, file_name)

    return loggers


# 初始化日志記錄器
log = setup_loggers()


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


class DB_SQL_MI:
    # 建構式
    def __init__(self):
        config = configparser.ConfigParser()
        configPATH = './.config/config' if os.path.isfile('./.config/config') else '../.config/config'
        config.read(configPATH)

        self.config = {
            'server': config.get('DB_SQL_MI', 'server'),
            'username': config.get('DB_SQL_MI', 'username'),
            'password': config.get('DB_SQL_MI', 'password'),
            'driver': config.get('DB_SQL_MI', 'driver'),
            'database': config.get('DB_SQL_MI', 'db_history')
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
    # print(sql_query)
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


#  王品設備異常 1:冷凍 2:解凍 3:冷藏 (SQL_MI版)
def alarm_sql_query_SQLMI(table_name, deviceName, devices_type, date):
    # 如果 devices_type 為 0，直接返回空的 DataFrame
    if devices_type == 0:
        columns = ['receiveTime', deviceName, 'Compare']
        return pd.DataFrame(columns=columns)  # 返回一個空的 DataFrame

    formula = '0 = 1'
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
        query = f""" OR (receiveTime BETWEEN DATEADD(DAY, -{i}, '{date} 03:00:00') 
                                         AND DATEADD(DAY, -{i}, '{date} 09:00:00'))"""
        insert_query += query

    sql_query = f"""
                SELECT receiveTime,
                       ISNULL(MAX(CASE WHEN NAME = 'Probe1' THEN VALUE END), NULL) AS '{deviceName}',
                       CASE WHEN {formula} THEN 'True' ELSE 'False' END AS Compare
                FROM [ems_data].[dbo].[{table_name}] WITH (NOLOCK)
                WHERE receiveTime between '{date} 03:00:00' and '{date} 09:00:00' {insert_query}
                GROUP BY receiveTime
                HAVING MAX(CASE WHEN NAME = 'Probe1' THEN VALUE END) IS NOT NULL
                ORDER BY receiveTime;
                """
    # print(sql_query)

    data = DB_SQL_MI().sql_connect(sql_query)
    columns = ['receiveTime', deviceName, 'Compare']

    df = pd.DataFrame(data, columns=columns)
    df = df.replace('', pd.NA)  # 將空字符串轉換為 NaN

    return df


def min_temperatures_SQLMI(devicesID, startTime, endTime):
    min_temp_sql_query = f'''SELECT MIN(CASE WHEN NAME = 'Probe1' THEN VALUE END) AS Min_Probe1
                             FROM [ems_data].[dbo].[{devicesID}] WITH (NOLOCK) 
                             WHERE receiveTime BETWEEN '{startTime}' AND '{endTime}' AND NAME = 'Probe1';
                         '''

    min_temperatures_result = DB_SQL_MI().sql_connect(min_temp_sql_query)
    # print(min_temperatures_result)

    # return 最小溫度值
    return min_temperatures_result.iloc[0]['Min_Probe1']


#  209客戶S800設備斷線
def device_disconnect_SQLMI():
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

    # u1.ID in (2, 332) 2：華銀 332：大潤發
    sql_query = f'''SELECT u.company, u.branch, d.device, d.device2,
                    min(a.createDate) startTime, max(a.time) endTime, count(d.device) times, a.text
                    FROM
                    (select u1.name as company, u2.ID, u2.name as branch 
                        FROM IESS_huanan.dbo.units u1 
                        INNER JOIN IESS_huanan.dbo.units u2 on u1.ID in (2, 332) and u1.ID = u2.parent) as u, 
                    (select d1.unitID as unitID, d1.name as device, d2.ID as ID, d2.name as device2 
                        FROM IESS_huanan.dbo.devices d1 
                        INNER JOIN IESS_huanan.dbo.devices d2 on d1.ID = d2.parent) as d,
                    (select alt.createDate, alt.time, alt.send, alm.type, at.text, alm.sendType, alm.emailAddress,
                     alm.phone, alt.id, alt.device_ID, alm.funEnable 
                        FROM IESS_huanan.dbo.alert alt 
                        INNER JOIN IESS_huanan.dbo.alarm alm on alt.alarm_ID = alm.id 
                        INNER JOIN IESS_huanan.dbo.alarmText at on alm.alarmText_ID = at.id 
                            and alm.alarmText_ID = 5) as a
                    WHERE u.ID = d.unitID and d.ID = a.device_ID
                    AND a.send = 0 and a.funEnable = 1 
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


# 需量(DM)警報
def alarm_DM():
    search_query = """
                   SELECT a.siteID AS '門市編號', b.name AS '門市名稱', a.deviceID AS '設備編號', a.alarm_value AS '警報值'
                   FROM [ems_information].[dbo].[alarm_Power_DM] a
                   INNER JOIN [ems_information].[dbo].[sites] b ON a.siteID = b.id
                   WHERE a.enable = 1
                   """
    device_list = DB_SQL_MI().sql_connect(search_query)
    device_list['觸發時間'] = None
    device_list['數值'] = None

    # 遍歷查詢結果的每一列
    for index, row in device_list.iterrows():
        deviceID = row['設備編號']
        alarm_value = row['警報值']

        # 第二次查詢，根據deviceID和alarm_value進行查詢
        query = f"""
                 SELECT TOP(1) [receiveTime], [name], [value]
                 FROM [ems_data].[dbo].[{deviceID}]
                 WHERE name = 'DM'
                 AND value > {alarm_value}
                 AND receiveTime BETWEEN 
                     CONVERT(DATETIME, CONVERT(VARCHAR, DATEADD(DAY, -1, GETDATE()), 23) + ' 09:00:00') 
                     AND CONVERT(DATETIME, CONVERT(VARCHAR, DATEADD(DAY, -1, GETDATE()), 23) + ' 21:00:00');
                 """

        try:
            result = DB_SQL_MI().sql_connect(query)

            # 如果有查詢到數據，則將 receiveTime 和 value 更新到原始 dataframe 中
            if not result.empty:
                device_list.at[index, '觸發時間'] = result.iloc[0]['receiveTime']
                device_list.at[index, '數值'] = result.iloc[0]['value']
        except Exception as e:
            # 使用 alarm_DM_log 的日誌記錄錯誤
            log['alarm_DM_log'].error(f"Error querying deviceID {deviceID} with alarm_value {alarm_value}: {str(e)}")
            log['alarm_DM_log'].handlers[0].flush()  # 確保日誌寫入到文件

    return device_list


# CO2 濃度警報
def alarm_CO2():
    search_query = """
                   SELECT a.siteID AS '門市編號', b.name AS '門市名稱', a.deviceID AS '設備編號', a.alarm_value AS '警報值'
                   FROM [ems_information].[dbo].[alarm_EV_CO2] a
                   INNER JOIN [ems_information].[dbo].[sites] b ON a.siteID = b.id
                   WHERE a.enable = 1
                   """

    device_list = DB_SQL_MI().sql_connect(search_query)
    device_list['觸發時間'] = None
    device_list['數值'] = None

    # 遍歷查詢結果的每一列
    for index, row in device_list.iterrows():
        deviceID = row['設備編號']
        alarm_value = row['警報值']

        # 第二次查詢，根據deviceID和alarm_value進行查詢
        query = f"""
                 SELECT TOP(1) [receiveTime], [name], [value]
                 FROM [ems_data].[dbo].[{deviceID}]
                 WHERE name = 'TFV'
                 AND value > {alarm_value}
                 AND receiveTime BETWEEN 
                     CONVERT(DATETIME, CONVERT(VARCHAR, DATEADD(DAY, -1, GETDATE()), 23) + ' 09:00:00') 
                     AND CONVERT(DATETIME, CONVERT(VARCHAR, DATEADD(DAY, -1, GETDATE()), 23) + ' 21:00:00');
                 """

        try:
            result = DB_SQL_MI().sql_connect(query)

            # 如果有查詢到數據，則將 receiveTime 和 value 更新到原始 dataframe 中
            if not result.empty:
                device_list.at[index, '觸發時間'] = result.iloc[0]['receiveTime']
                device_list.at[index, '數值'] = result.iloc[0]['value']
        except Exception as e:
            # 使用 alarm_DM_log 的日誌記錄錯誤
            log['alarm_EV_CO2'].error(f"Error querying deviceID {deviceID} with alarm_value {alarm_value}: {str(e)}")
            log['alarm_EV_CO2'].handlers[0].flush()  # 確保日誌寫入到文件

    return device_list


# 累積水流量警報
def alarm_TFV():
    search_query = """
                   SELECT a.siteID AS '門市編號', b.name AS '門市名稱', a.deviceID AS '設備編號', a.alarm_value AS '警報值'
                   FROM [ems_information].[dbo].[alarm_Water_TFV] a
                   INNER JOIN [ems_information].[dbo].[sites] b ON a.siteID = b.id
                   WHERE a.enable = 1
                   """

    device_list = DB_SQL_MI().sql_connect(search_query)
    device_list['觸發時間'] = None
    device_list['數值'] = None

    # 遍歷查詢結果的每一列
    for index, row in device_list.iterrows():
        deviceID = row['設備編號']
        alarm_value = row['警報值']

        # 第二次查詢，根據deviceID和alarm_value進行查詢
        query = f"""
                 SELECT [receiveTime], [name], [value]
                 FROM [ems_data].[dbo].[{deviceID}]
                 WHERE name = 'TFV'
                 AND receiveTime BETWEEN 
                     CONVERT(DATETIME, CONVERT(VARCHAR, DATEADD(DAY, -1, GETDATE()), 23) + ' 08:55:00') 
                     AND CONVERT(DATETIME, CONVERT(VARCHAR, GETDATE(), 23) + ' 09:00:00');
                 """

        try:
            result = DB_SQL_MI().sql_connect(query)

            # 如果有查詢到數據，則將 receiveTime 和 value 更新到原始 dataframe 中
            if not result.empty:
                # 如果所有的 value 都相同，則選擇第一筆資料
                if result['value'].nunique() == 1:
                    # 取得第一筆和最後一筆資料的 receiveTime
                    first_receive_time = result.iloc[0]['receiveTime']
                    last_receive_time = result.iloc[-1]['receiveTime']

                    # 計算兩者之間的時間差（以小時為單位）
                    time_difference = (last_receive_time - first_receive_time).total_seconds() / 3600

                    # 如果時間差超過 警報值(小時)，則更新最後一筆的資料
                    if time_difference >= alarm_value:
                        device_list.at[index, '觸發時間'] = result.iloc[-1]['receiveTime']
                        device_list.at[index, '數值'] = result.iloc[-1]['value']
        except Exception as e:
            # 使用 alarm_DM_log 的日誌記錄錯誤
            log['alarm_Water_TFV'].error(f"Error querying deviceID {deviceID} with alarm_value {alarm_value}: {str(e)}")
            log['alarm_Water_TFV'].handlers[0].flush()  # 確保日誌寫入到文件

    return device_list


# 設備運作警報
def alarm_DeviceRun():
    search_query = """
                   SELECT a.siteID AS '門市編號', b.name AS '門市名稱', a.deviceID AS '設備編號', a.alarm_value AS '警報值'
                   FROM [ems_information].[dbo].[alarm_device_Run] a
                   INNER JOIN [ems_information].[dbo].[sites] b ON a.siteID = b.id
                   WHERE a.enable = 1
                   """


# 空調異常警報
def alarm_ACErr():
    search_query = """
                   SELECT a.siteID AS '門市編號', b.name AS '門市名稱', a.deviceID AS '設備編號', a.alarm_value AS '警報值'
                   FROM [ems_information].[dbo].[alarm_AC_Err] a
                   INNER JOIN [ems_information].[dbo].[sites] b ON a.siteID = b.id
                   WHERE a.enable = 1
                   """

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
    # pass
    alarm_DM()
    # getAlertList()
