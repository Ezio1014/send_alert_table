import pymysql
import configparser
import pandas as pd


# 建立MySQL連接
class DB:
    # 建構式
    def __init__(self):
        config = configparser.ConfigParser()
        config.read('./.config/config')

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

    # 將空字符串轉換為 NaN
    df = df.replace('', pd.NA)
    return df


def min_temperatures(devicesID, date):
    min_temp_sql_query = f'''SELECT MIN(CASE WHEN NAME = 'Probe1' THEN VALUE END) AS Min_Probe1
                            FROM ems_data.`{devicesID}`
                            WHERE receiveTime BETWEEN '{date} 03:00:00' AND '{date} 09:00:00' AND NAME = 'Probe1';
                         '''
    min_temperatures_result = DB().sql_connect(min_temp_sql_query)

    # return 最小溫度值
    return min_temperatures_result[0][0]


# ----------測試區----------
if __name__ == '__main__':
    pass
    # db = DB()

