# 系統套件
import os
import sys
import json
import time
import configparser
from datetime import datetime, timedelta

# 程式套件
from Model.data_save2excel import data_save2excel
from Model.df_dealing import df_dealing, build_html_table
from Model.send_mail import mail_setting
from DB.DB_API import device_disconnect, AC_unclosed_alarm


#  ------共用 Function--------------------------------------------------------------------------------------------------
# 發送郵件(姓名、地址、內容、郵件類型)
def sendMail(name, addr, msg_Subject, table_Subject, mail_content):
    Mail = mail_setting()
    Mail.send_mail(name, addr, msg_Subject, table_Subject, mail_content)


#  --------------------------------------------------------------------------------------------------------------------

#  ------Decorator-----------------------------------------------------------------------------------------------------
# 計時器
def timer_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f'Done! Elapsed Time: {elapsed_time:.2f} seconds')

    return wrapper


#  --------------------------------------------------------------------------------------------------------------------

#  ------程式區---------------------------------------------------------------------------------------------------------
# 王品警報主程式
@timer_decorator
def run_alert_WOWprime():
    # config import，目前獲取當日資料 days=0
    config = configparser.ConfigParser()
    config.read('.config/config')
    part_fileName = str((datetime.now().date()) - timedelta(days=int(config.get('fileDate', 'days'))))

    # 王品警報成員設定檔 & 路徑
    # file_path = os.path.join('./Member_info', 'WOWprime.json')      # 正式設定檔
    file_path = os.path.join('./Member_info', 'test_sample.json')  # 測試設定檔
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
                sendMail(mem["name"], mem["mail"], "王品/群品 冷櫃溫度異常發信", "冷櫃溫度異常報表", html_table)


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
                sendMail(mem["name"], mem["mail"], "209設備斷線警報", "209設備斷線報表", html_table)


# 0830、1915 空調未關警報
def run_AC_unclosed_alarm(sendTime):
    member_dict = {'永曜雲端科技有限公司': 'contact@iess.com.tw',
                   'Annie': 'annie@iess.com.tw',
                   'jentetsai': 'jentetsai@gmail.com',
                   'Ezio': 'ezio@iess.com.tw'
                   }
    if sendTime == '0830':
        df = AC_unclosed_alarm('0830')
        html_table = build_html_table(df)
        for name, addr in member_dict.items():
            sendMail(name, addr, "IESS空調未關警報(0830)", "空調未關報表", html_table)
    elif sendTime == '1915':
        df = AC_unclosed_alarm('1900')
        html_table = build_html_table(df)
        for name, addr in member_dict.items():
            sendMail(name, addr, "IESS空調未關警報(1915)", "空調未關報表", html_table)


#  --------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    # function
    if len(sys.argv) == 2:
        if sys.argv[1] == 'alert_WOWprime':
            run_alert_WOWprime()
        elif sys.argv[1] == 'device_disconnect':
            run_device_disconnect()
        elif sys.argv[1] == 'AC_unclosed_0830':
            run_AC_unclosed_alarm('0830')
        elif sys.argv[1] == 'AC_unclosed_1915':
            run_AC_unclosed_alarm('1900')
        else:
            print(f"Unknown function: {sys.argv[1]}")
    else:
        print("Usage: python script.py <function_name>")

    # ------測試區------
    # run_alert_WOWprime()
    # run_device_disconnect()
