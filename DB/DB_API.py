import pymysql
import configparser
import pandas as pd


# 建立MySQL連接
class DB:
    # 建構式
    def __init__(self):
        config = configparser.ConfigParser()
        config.read('./.config/config')  # 正式路徑
        # config.read('../.config/config')  # 當前測試用路徑

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


#  王品設備異常
def alarm_sql_query(table_name, title, devices_type, date):
    formula = '0'
    if devices_type == 1:
        formula = "MAX(CASE WHEN NAME = 'Probe1' THEN VALUE END) > -18"
    elif devices_type == 2:
        formula = "MAX(CASE WHEN NAME = 'Probe1' THEN VALUE END) > 7"
    elif devices_type == 3:
        formula = "MAX(CASE WHEN NAME = 'Probe1' THEN VALUE END) > -5"

    sql_query = f"""
                SELECT receiveTime,
                       IFNULL(MAX(CASE WHEN NAME = 'Probe1' THEN VALUE END), NULL) AS `{title}`,
                       CASE WHEN {formula} THEN 'True' ELSE 'False' END AS Compare
                FROM ems_data.`{table_name}`
                Where receiveTime between '{date} 03:00:00' and '{date} 09:00:00'
                GROUP BY receiveTime
                HAVING MAX(CASE WHEN NAME = 'Probe1' THEN VALUE END) IS NOT NULL;
                """

    data = DB().sql_connect(sql_query)
    columns = ['receiveTime', title, 'Compare']

    df = pd.DataFrame(data, columns=columns)
    df = df.replace('', pd.NA)  # 將空字符串轉換為 NaN

    return df


def min_temperatures(devicesID, date):
    min_temp_sql_query = f'''SELECT MIN(CASE WHEN NAME = 'Probe1' THEN VALUE END) AS Min_Probe1
                             FROM ems_data.`{devicesID}`
                             WHERE receiveTime BETWEEN '{date} 03:00:00' AND '{date} 09:00:00' AND NAME = 'Probe1';
                         '''
    min_temperatures_result = DB().sql_connect(min_temp_sql_query)

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
    data = DB().sql_connect(disconnect_sql_query)

    columns = ['設備編號', '公司', '分店', '設備名稱', '最後一筆資料時間', '已經斷線(分鐘)']
    df = pd.DataFrame(data, columns=columns)
    df = df.replace('', pd.NA)  # 將空字符串轉換為 NaN

    return df


# ----------測試區----------
if __name__ == '__main__':
    pass
    # db = device_disconnect()
    # print(db)
