import os
import pyodbc
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import configparser
import pandas as pd
from datetime import datetime, timedelta
# from tqdm import tqdm
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
            'db_history': config.get('DB_SQL_MI', 'db_history'),
            'db_info': config.get('DB_SQL_MI', 'db_info'),
            'host': config.get('DB_SQL_MI', 'server_host'),
            'port': config.get('DB_SQL_MI', 'server_port')

        }

    def sql_connect(self, sql):
        # 使用pyodbc連接，需安裝odbc驅動
        connection_string = (
            f"mssql+pyodbc://{quote_plus(self.config['username'])}:{quote_plus(self.config['password'])}@"
            f"{self.config['host']}:{self.config['port']}/{self.config['db_history']}?"
            "driver=ODBC+Driver+17+for+SQL+Server"
        )
        engine = create_engine(connection_string)

        # df = pd.DataFrame()  # 設置初始值
        try:
            df = pd.read_sql(sql, engine)  # 使用 SQLAlchemy engine 讀取資料
            return df
        except Exception as e:
            print(f"Error: {e}")

    def insert(self, table_name, columns, values):
        """
        通用插入功能
        :param table_name: 資料表名稱
        :param columns: 欄位名稱（list）
        :param values: 插入的值（list of tuples）
        """
        conn_str = f"""DRIVER={self.config['driver']};
                       SERVER={self.config['server']};
                       DATABASE={self.config['db_info']};
                       UID={self.config['username']};
                       PWD={self.config['password']};
                       TrustServerCertificate=yes;"""

        connection = None
        cursor = None  # 確保變數在 finally 區塊中可被引用

        try:
            connection = pyodbc.connect(conn_str)
            cursor = connection.cursor()

            # 動態構建 INSERT 語句
            placeholders = ", ".join(["?" for _ in columns])
            column_names = ", ".join(columns)
            insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"

            # 批量插入
            cursor.executemany(insert_query, values)
            connection.commit()

            print(f"資料成功插入表 {table_name}")

        except pyodbc.Error as e:
            print(f"插入失敗：{e}")

        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def update(self, table_name, updates, condition):
        """
        通用更新功能
        :param table_name: 資料表名稱
        :param updates: 欄位更新字典 {column: value}
        :param condition: 更新條件 (string)
        """
        conn_str = f"""DRIVER={self.config['driver']};
                       SERVER={self.config['server']};
                       DATABASE={self.config['db_info']};
                       UID={self.config['username']};
                       PWD={self.config['password']};
                       TrustServerCertificate=yes;"""

        connection = None
        cursor = None  # 確保變數在 finally 區塊中可被引用

        try:
            connection = pyodbc.connect(conn_str)
            cursor = connection.cursor()

            # 動態構建 UPDATE 語句
            set_clause = ", ".join([f"{col} = ?" for col in updates.keys()])
            update_query = f"UPDATE {table_name} SET {set_clause} WHERE {condition}"

            cursor.execute(update_query, list(updates.values()))
            connection.commit()

            print(f"資料成功更新表 {table_name}")

        except pyodbc.Error as e:
            print(f"更新失敗：{e}")

        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()


#  ---王品設備異常警報(SQL_MI版)---
#  取得門市資料
def get_store_info():
    brand_sql = '''SELECT a.id , a.name, a.brand , a.corp, b.bname 
                    FROM [ems_information].[dbo].[sites] a 
                    LEFT JOIN [ems_information].[dbo].[sites_brand] b on a.brand = b.id
                    WHERE brand in (SELECT id 
                                    FROM [ems_information].[dbo].[sites_brand] 
                                    WHERE corp in (2,3))
                    AND a.enable = 1
                    AND b.enable = 1;
                '''
    data = DB_SQL_MI().sql_connect(brand_sql)
    return data


#  取得門市設備資料
def get_devices_info(siteID):
    device_sql = f'''SELECT id, name, alarm_type 
                     FROM [ems_information].[dbo].[devices] 
                     WHERE siteID = {siteID}
                     AND enable = 1;'''
    data = DB_SQL_MI().sql_connect(device_sql)
    return data


#  取得區經理負責門市列表
def get_alarm_sites(memberID):
    query = f"""SELECT siteID 
                FROM [ems_information].[dbo].[sites_alarm_member_WP] 
                WHERE enable = 1 
                AND memberID = ;
            """

    data = DB_SQL_MI().sql_connect(query)

    if not data.empty and 'siteID' in data.columns:
        sites_list = data['siteID'].tolist()
    else:
        sites_list = []  # 如果沒有結果，返回空列表

    return sites_list


#  警報查詢語法 1:冷凍 2:解凍 3:冷藏
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

    # 設備數據天數(14天)
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


# 優化中
def alarm_sql_query_SQLMI_Test(deviceID, deviceName, devices_type, date):
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

    # 設備數據天數(1天)
    sql_query = f"""
                SELECT receiveTime,
                       ISNULL(MAX(CASE WHEN NAME = 'Probe1' THEN VALUE END), NULL) AS '{deviceName}',
                       CASE WHEN {formula} THEN 'True' ELSE 'False' END AS Compare
                FROM [ems_data].[dbo].[{deviceID}] WITH (NOLOCK)
                WHERE receiveTime between '{date} 03:00:00' and '{date} 09:00:00' 
                GROUP BY receiveTime
                HAVING MAX(CASE WHEN NAME = 'Probe1' THEN VALUE END) IS NOT NULL
                ORDER BY receiveTime;
                """
    # print(sql_query)

    data = DB_SQL_MI().sql_connect(sql_query)
    columns = ['receiveTime', deviceName, 'Compare']

    df = pd.DataFrame(data, columns=columns)
    df = df.replace('', pd.NA)  # 將空字串轉換為 NaN

    return df


# 設備最低溫
def min_temperatures_SQLMI(devicesID, startTime, endTime, mode=None):
    min_temp_sql_query = f'''SELECT MIN(CASE WHEN NAME = 'Probe1' THEN VALUE END) AS Min_Probe1
                             FROM [ems_data].[dbo].[{devicesID}] WITH (NOLOCK) 
                             WHERE NAME = 'Probe1' AND receiveTime BETWEEN 
                          '''
    # 前日凌晨 3-12 時
    if mode == 'yesterday_0912':
        min_temp_sql_query += f"""
                               DATEADD(DAY, -1, CONVERT(DATETIME, CONCAT(CONVERT(DATE, '{startTime}'), ' 03:00:00')))
                               AND DATEADD(DAY, -1, CONVERT(DATETIME, CONCAT(CONVERT(DATE, '{endTime}'), ' 12:00:00')))
                               """
    # 當日凌晨 3-9 時
    elif mode == 'today_0309':
        min_temp_sql_query += f"""
                               CONVERT(DATETIME, CONCAT(CONVERT(DATE, '{startTime}'), ' 03:00:00'))
                               AND CONVERT(DATETIME, CONCAT(CONVERT(DATE, '{endTime}'), ' 09:00:00'))
                               """
    # 當日指定區間
    elif mode is None:
        min_temp_sql_query += f"'{startTime}' AND '{endTime}';"

    min_temperatures_result = DB_SQL_MI().sql_connect(min_temp_sql_query)
    return min_temperatures_result.iloc[0]['Min_Probe1']


# 王品工程部
def member_EN():
    sql_query = f'''SELECT b.name, b.email, a.TC_WOW_EN, a.TC_WOW_FS, a.TC_WOW_MA, a.memberID, a.siteID
                    FROM [ems_information].[dbo].[alarm_permission] a
                    LEFT JOIN [ems_information].[dbo].[member_info] b ON a.memberID = b.id
                    WHERE b.corpID in (4)
                    AND b.enable = 1
                    AND a.TC_WOW_EN = 1;
                 '''
    member_info = DB_SQL_MI().sql_connect(sql_query)

    return member_info


# 王品食安部
def member_FS():
    sql_query = f'''SELECT b.name, b.email, a.TC_WOW_EN, a.TC_WOW_FS, a.TC_WOW_MA, a.memberID, a.siteID
                    FROM [ems_information].[dbo].[alarm_permission] a
                    LEFT JOIN [ems_information].[dbo].[member_info] b ON a.memberID = b.id
                    WHERE b.corpID in (4)
                    AND b.enable = 1
                    AND a.TC_WOW_FS = 1;
                 '''
    member_info = DB_SQL_MI().sql_connect(sql_query)

    return member_info


# 王品區經理
def member_MA():
    sql_query = f'''SELECT b.name, b.email, a.TC_WOW_EN, a.TC_WOW_FS, a.TC_WOW_MA, a.memberID, a.siteID
                    FROM [ems_information].[dbo].[alarm_permission] a
                    LEFT JOIN [ems_information].[dbo].[member_info] b ON a.memberID = b.id
                    WHERE b.corpID in (4)
                    AND b.enable = 1
                    AND a.TC_WOW_MA = 1
                    AND a.memberID <> 0;
                 '''
    member_info = DB_SQL_MI().sql_connect(sql_query)

    return member_info


# 王品門市
def member_Store():
    sql_query = f'''SELECT b.name, b.email, a.TC_WOW_EN, a.TC_WOW_FS, a.TC_WOW_MA, a.memberID, a.siteID
                    FROM [ems_information].[dbo].[alarm_permission] a
                    LEFT JOIN[ems_information].[dbo].[sites] b ON a.siteID = b.id
                    WHERE b.corp in (4)
                    AND b.enable = 1
                    AND a.TC_WOW_MA = 0
                    AND a.siteID <> 0;
                 '''
    member_info = DB_SQL_MI().sql_connect(sql_query)

    return member_info


# 王品 SA(目前僅有IESS)
def member_SA():
    sql_query = f'''SELECT b.name, b.email, a.TC_WOW_EN, a.TC_WOW_FS, a.TC_WOW_MA, a.memberID, a.siteID
                    FROM [ems_information].[dbo].[alarm_permission] a
                    LEFT JOIN [ems_information].[dbo].[member_info] b ON a.memberID = b.id
                    WHERE b.corpID = 4 
                    AND b.enable = 1;
                 '''
    member_info = DB_SQL_MI().sql_connect(sql_query)

    return member_info


#  ---永曜資料庫客戶S800設備斷線---
def device_disconnect_member():
    sql_query = f'''SELECT a.name, a.email
                    FROM [ems_information].[dbo].[member_info] a
                    LEFT JOIN [ems_information].[dbo].[alarm_permission] b on a.id = b.memberID
                    WHERE a.enable = 1 AND b.dc_disc = 1;
                '''
    data = DB_SQL_MI().sql_connect(sql_query)
    return data


def device_disconnect_SQLMI():
    disconnect_sql_query = f'''SELECT a.id '設備編號', d.name '公司', b.name '分店', a.name '設備名稱', 
                                      c.receiveTime '最後一筆資料時間', 
                                      ROUND(DATEDIFF(SECOND, c.receiveTime, GETDATE()) / 60, 0) '已經斷線(分鐘)'
                               FROM [ems_information].[dbo].[devices] a
                                LEFT JOIN [ems_information].[dbo].[sites] b on a.siteID = b.id
                                LEFT JOIN [ems_information].[dbo].[status] c on a.id = c.deviceID
                                LEFT JOIN [ems_information].[dbo].[sites] d on b.parent = d.id
                               WHERE a.enable = 1 
                                AND b.id not in (2,3,4,6) 
                                AND b.parent <> 0 
                                AND c.receiveTime < DATEADD(HOUR, -1, GETDATE())
                               ORDER BY a.siteID, a.id;
                            '''
    data = DB_SQL_MI().sql_connect(disconnect_sql_query)

    # columns = ['設備編號', '公司', '分店', '設備名稱', '最後一筆資料時間', '已經斷線(分鐘)']
    # df = pd.DataFrame(data, columns=columns)
    df = data.replace('', pd.NA)  # 將空字符串轉換為 NaN

    return df


#  ---空調未關警報查詢/更新(DB_31)---
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


# ---需量(DM)警報---
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

    # 將 DataFrame 中的資料插入 alarm_EV_log 表
    db = DB_SQL_MI()

    # 清洗 device_list，移除數值為 "NONE" 的資料
    device_list_cleaned = device_list[device_list['數值'].notnull()]

    # 構建插入的欄位和值
    table_name = "alarm_Power_log"
    columns = ["siteID", "DeviceID", "alarmDate", "alarmType", "value", "notifyID"]
    values = [
        (row['門市編號'], row['設備編號'], row['觸發時間'], 'DM', row['數值'], 8)
        for _, row in device_list_cleaned.iterrows()
    ]

    if values:
        # 調用通用 insert 方法
        db.insert(table_name, columns, values)

    return device_list


# ---CO2 濃度警報---
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
                 WHERE name = 'CO2'
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

    # 將 DataFrame 中的資料插入 alarm_EV_log 表
    db = DB_SQL_MI()

    # 清洗 device_list，移除數值為 "NONE" 的資料
    device_list_cleaned = device_list[device_list['數值'].notnull()]

    # 構建插入的欄位和值
    table_name = "alarm_EV_log"
    columns = ["siteID", "DeviceID", "alarmDate", "alarmType", "value", "notifyID"]
    values = [
        (row['門市編號'], row['設備編號'], row['觸發時間'], 'CO2', row['數值'], 6)
        for _, row in device_list_cleaned.iterrows()
    ]

    if values:
        # 調用通用 insert 方法
        db.insert(table_name, columns, values)

    return device_list


# ---累積水流量警報---
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


# ---設備運作警報---
def alarm_DeviceRun():
    search_query = """
                   SELECT a.siteID AS '門市編號', b.name AS '門市名稱', a.deviceID AS '設備編號', a.alarm_value AS '營業結束時間'
                   FROM [ems_information].[dbo].[alarm_device_Run] a
                   INNER JOIN [ems_information].[dbo].[sites] b ON a.siteID = b.id
                   WHERE a.enable = 1
                   """


def insert_alarm_DeviceRun(df):
    # 將 DataFrame 中的資料插入 alarm_devices_log 表
    db = DB_SQL_MI()

    # 構建插入的欄位和值
    table_name = "alarm_devices_log"
    columns = ["siteID", "localDeviceID", "alarmType", "notifyID"]
    values = [
        (row['門市編號'], row['設備編號'], 'run', 2)  # alarmDate 使用 SQL 的 GETDATE()
        for _, row in df.iterrows()
    ]

    # 調用通用 insert 方法
    db.insert(table_name, columns, values)


# ---空調異常警報---
def alarm_AC_Err():
    search_query = """
                   SELECT a.siteID AS '門市編號', b.name AS '門市名稱', a.deviceID AS '設備編號', a.alarm_value AS '警報值'
                   FROM [ems_information].[dbo].[alarm_AC_Err] a
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
        print(f'ACErr_deviceID：{deviceID}')

        # 第二次查詢，根據deviceID和alarm_value進行查詢
        query = f"""
                 SELECT TOP(1) [receiveTime], [name], [value]
                 FROM [ems_data].[dbo].[{deviceID}]
                 WHERE name = 'Err'
                 AND value <> {alarm_value}
                 AND receiveTime BETWEEN 
                     CONVERT(DATETIME, CONVERT(VARCHAR, DATEADD(DAY, -1, GETDATE()), 23) + ' 06:30:00') 
                     AND CONVERT(DATETIME, CONVERT(VARCHAR, DATEADD(DAY, -1, GETDATE()), 23) + ' 22:00:00');
                 """

        try:
            result = DB_SQL_MI().sql_connect(query)

            # 如果有查詢到數據，則將 receiveTime 和 value 更新到原始 dataframe 中
            if not result.empty:
                device_list.at[index, '觸發時間'] = result.iloc[0]['receiveTime']
                device_list.at[index, '數值'] = result.iloc[0]['value']
        except Exception as e:
            # 使用 alarm_DM_log 的日誌記錄錯誤
            log['alarm_EV_CO2'].error(f"Error querying deviceID {deviceID} with alarm_value {device_list}: {str(e)}")
            log['alarm_EV_CO2'].handlers[0].flush()  # 確保日誌寫入到文件

    return device_list


def test():
    pass


#  ----------測試區----------
if __name__ == '__main__':
    pass
    # alarm_DM()
    # getAlertList()
