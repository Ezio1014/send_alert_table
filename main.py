import os
import json
import configparser
from datetime import datetime, timedelta
from Model.data_save2excel import data_save2excel
from Model.df_dealing import df_dealing
from Model.send_mail import mail_setting


def run():
    # config import，目前獲取當日資料 days=0
    config = configparser.ConfigParser()
    config.read('.config/config')
    part_fileName = str((datetime.now().date()) - timedelta(days=int(config.get('fileDate', 'days'))))
    # 成員設定檔 & 路徑
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
                Mail.send_mail(addr, html_table, name)


if __name__ == '__main__':
    # function
    run()
    print('done.')
