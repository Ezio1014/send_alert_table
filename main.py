import os
import json
import configparser
from datetime import datetime, timedelta
from Model.data_save2excel import data_save2excel
from Model.df_dealing import df_dealing, build_html_table
from Model.send_mail import mail_setting
from DB.DB_API import device_disconnect


# 王品警報主程式
def run_alert_WOWprime():
    # config import，目前獲取當日資料 days=0
    config = configparser.ConfigParser()
    config.read('.config/config')
    part_fileName = str((datetime.now().date()) - timedelta(days=int(config.get('fileDate', 'days'))))

    # 王品警報成員設定檔 & 路徑
    # file_path = os.path.join('./Member_info', 'WOWprime.json')      # 正式設定檔
    file_path = os.path.join('./Member_info', 'test_sample.json')   # 測試設定檔
    excel_file_path = os.path.join(os.getcwd(), "data", f'{part_fileName}.xlsx')

    if not os.path.isfile(excel_file_path):
        data_save2excel(file_path, part_fileName)

    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
        member_dict = data['Member']

        for member in member_dict:
            mem = member_dict[member]
            html_table = df_dealing(mem['dep_NO'], mem['store'])
            if html_table == 'empty':
                continue
            else:
                Mail = mail_setting()
                addr = mem["mail"]
                name = mem["name"]
                Mail.send_mail(addr, html_table, name, 1)


# 209設備斷線主程式
def run_device_disconnect():
    config = configparser.ConfigParser()
    config.read('.config/config')

    # 209設備斷線成員設定檔 & 路徑
    file_path_209 = os.path.join('./Member_info', 'device_disconnect_209.json')

    table = device_disconnect()
    with open(file_path_209, 'r', encoding='utf-8') as file:
        data = json.load(file)
        member_dict = data['Member']

        for member in member_dict:
            mem = member_dict[member]
            html_table = build_html_table(table)
            if html_table == 'empty':
                continue
            else:
                Mail = mail_setting()
                addr = mem["mail"]
                name = mem["name"]
                Mail.send_mail(addr, html_table, name, 2)


if __name__ == '__main__':
    # function
    run_alert_WOWprime()
    run_device_disconnect()
    print('done.')
