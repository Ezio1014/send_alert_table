# 系統套件
import os
import sys
import json
import time
import configparser
import logging
from datetime import datetime, timedelta

# 程式套件
from Model.data_save2excel import data_save2excel
from Model.df_dealing import df_dealing, build_html_table
from Model.send_mail import mail_setting
from Model import alarm_Power_DM, alarm_EV_CO2, alarm_AC_Err, alarm_device_Run, alarm_Water_TFV
from DB.DB_API import device_disconnect, AC_unclosed_alarm, getAlertList

# 永曜設備斷線警報
from DB.DB_API import device_disconnect_member, device_disconnect_SQLMI

# 設定基本的日誌配置
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler('app.log')  # 將日誌寫入文件
                        # logging.StreamHandler()          # 將日誌輸出到控制台
                    ])

logger = logging.getLogger(__name__)


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
    # file_path = os.path.join('./Member_info', 'WOWprime.json')     # 正式設定檔
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
            dep_name = mem['name']
            if html_table == 'empty':
                print(f'dep_name：{dep_name} is empty')
                continue
            else:
                sendMail(mem["name"], mem["mail"], "王品/群品 冷櫃溫度異常發信", "冷櫃溫度異常報表", html_table)
                logger.info("{} Mail已發送".format(mem["name"]))


# 209設備斷線主程式
def run_device_disconnect():
    # config = configparser.ConfigParser()
    # config.read('.config/config')
    #
    # # 209設備斷線成員設定檔 & 路徑
    # file_path_209 = os.path.join('./Member_info', 'device_disconnect_209.json')
    #
    # table = device_disconnect()
    # with open(file_path_209, 'r', encoding='utf-8') as file:
    #     data = json.load(file)
    #     member_dict = data['Member']
    #
    #     for member in member_dict:
    #         mem = member_dict[member]
    #         html_table = build_html_table(table)
    #         if html_table == 'empty':
    #             continue
    #         else:
    #             sendMail(mem["name"], mem["mail"], "209設備斷線警報", "209設備斷線報表", html_table)
    member_list = device_disconnect_member()
    table = device_disconnect_SQLMI()

    # 迴圈處理每一位成員的資料
    for _, mem in member_list.iterrows():
        # 建立要傳遞的 HTML 表格內容
        html_table = build_html_table(table)
        if html_table == 'empty':
            continue
        else:
            sendMail(mem["name"], mem["email"], "209設備斷線警報", "209設備斷線報表", html_table)


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


#  ------星巴克警報------
# ---需量(DM)警報---
def run_alarm_Power_DM():
    alarm_Power_DM.save2excel()

    # 載入JSON檔案
    current_dir = os.path.dirname(os.path.abspath(__file__))
    json_file_path = os.path.join(current_dir, '.', 'Member_info', 'alarm_Power_DM.json')

    # EXCEL檔案路徑
    current_date = datetime.now().strftime('%Y-%m-%d')
    excel_file_path = f"./data/alarm_Power_DM/{current_date}.xlsx"

    with open(json_file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)

        # 根據JSON中的Member進行篩選和打印
    for member_id, member_info in json_data['Member'].items():
        store = member_info['store']
        name = member_info['name']
        mail = member_info['mail']

        result_df = alarm_Power_DM.filter_data_by_store(store, excel_file_path)

        # 如果沒有內容則跳過此迴圈
        if result_df.empty:
            continue

        html_table = build_html_table(result_df)  # 有結果時生成HTML表格
        sendMail(name, mail, "用電需量警報", "用電需量(DM)超過設定標準報表", html_table)  # 發送郵件

        # # 打印篩選結果
        # print(f"篩選結果 - {member_info['name']}:")
        # print(result_df)
        # print("\n")  # 每個篩選結果之間留一個空行


# ---CO2 濃度警報---
def run_alarm_EV_CO2():
    alarm_EV_CO2.save2excel()

    # 載入JSON檔案
    current_dir = os.path.dirname(os.path.abspath(__file__))
    json_file_path = os.path.join(current_dir, '.', 'Member_info', 'alarm_EV_CO2.json')

    # EXCEL檔案路徑
    current_date = datetime.now().strftime('%Y-%m-%d')
    excel_file_path = f"./data/alarm_EV_CO2/{current_date}.xlsx"

    with open(json_file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)

        # 根據JSON中的Member進行篩選和打印
    for member_id, member_info in json_data['Member'].items():
        store = member_info['store']
        name = member_info['name']
        mail = member_info['mail']

        result_df = alarm_EV_CO2.filter_data_by_store(store, excel_file_path)

        # 如果沒有內容則跳過此迴圈
        if result_df.empty:
            continue

        html_table = build_html_table(result_df)  # 有結果時生成HTML表格
        sendMail(name, mail, "CO2濃度超標警報", "CO2濃度超標報表", html_table)  # 發送郵件


# ---累積水流量警報---
def run_alarm_Water_TFV():
    alarm_Water_TFV.save2excel()

    # 載入JSON檔案
    current_dir = os.path.dirname(os.path.abspath(__file__))
    json_file_path = os.path.join(current_dir, '.', 'Member_info', 'alarm_Water_TFV.json')

    # EXCEL檔案路徑
    current_date = datetime.now().strftime('%Y-%m-%d')
    excel_file_path = f"./data/alarm_Water_TFV/{current_date}.xlsx"

    with open(json_file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)

        # 根據JSON中的Member進行篩選和打印
    for member_id, member_info in json_data['Member'].items():
        store = member_info['store']
        name = member_info['name']
        mail = member_info['mail']

        result_df = alarm_Water_TFV.filter_data_by_store(store, excel_file_path)

        # 如果沒有內容則跳過此迴圈
        if result_df.empty:
            continue

        html_table = build_html_table(result_df)  # 有結果時生成HTML表格
        sendMail(name, mail, "水流量數值未變化警報", "水流量數值報表", html_table)  # 發送郵件


# ---設備運作警報---
# 查詢並儲存
def run_alarm_device_Run_saveFile():
    alarm_device_Run.save2excel()


# 發送郵件
def run_alarm_device_Run_sendEMail():
    # 載入JSON檔案
    current_dir = os.path.dirname(os.path.abspath(__file__))
    json_file_path = os.path.join(current_dir, '.', 'Member_info', 'alarm_device_Run.json')

    # EXCEL檔案路徑
    current_date = datetime.now().strftime('%Y-%m-%d')
    excel_file_path = f"./data/alarm_device_Run/{current_date}.xlsx"

    with open(json_file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)

        # 根據JSON中的Member進行篩選和打印
    for member_id, member_info in json_data['Member'].items():
        store = member_info['store']
        name = member_info['name']
        mail = member_info['mail']

        result_df = alarm_device_Run.filter_data_by_store(store, excel_file_path)

        # 如果 '運作狀態' 欄位是字串 "NONE" 而不是真正的 None，使用以下代碼進行過濾：
        result_df = result_df[result_df['運作狀態'].str.upper() != 'NONE']

        # 如果沒有內容則跳過此迴圈
        if result_df.empty:
            continue

        html_table = build_html_table(result_df)  # 有結果時生成HTML表格
        sendMail(name, mail, "設備未關警報", "設備未關報表", html_table)  # 發送郵件


# ---空調異常警報---
def run_alarm_AC_Err():
    alarm_AC_Err.save2excel()

    # 載入JSON檔案
    current_dir = os.path.dirname(os.path.abspath(__file__))
    json_file_path = os.path.join(current_dir, '.', 'Member_info', 'alarm_AC_Err.json')

    # EXCEL檔案路徑
    current_date = datetime.now().strftime('%Y-%m-%d')
    excel_file_path = f"./data/alarm_AC_Err/{current_date}.xlsx"

    with open(json_file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)

        # 根據JSON中的Member進行篩選和打印
    for member_id, member_info in json_data['Member'].items():
        store = member_info['store']
        name = member_info['name']
        mail = member_info['mail']

        result_df = alarm_AC_Err.filter_data_by_store(store, excel_file_path)

        # 如果沒有內容則跳過此迴圈
        if result_df.empty:
            continue

        html_table = build_html_table(result_df)  # 有結果時生成HTML表格
        sendMail(name, mail, "空調設備異常故障警報", "空調設備異常故障報表", html_table)  # 發送郵件


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
            run_AC_unclosed_alarm('1915')
        elif sys.argv[1] == 'UpdateAlertTable':
            getAlertList()
        elif sys.argv[1] == 'alarm_Power_DM':
            run_alarm_Power_DM()
        elif sys.argv[1] == 'alarm_EV_CO2':
            run_alarm_EV_CO2()
        elif sys.argv[1] == 'alarm_Water_TFV':
            run_alarm_Water_TFV()
        elif sys.argv[1] == 'alarm_device_Run_saveFile':
            run_alarm_device_Run_saveFile()
        elif sys.argv[1] == 'alarm_device_Run_sendEMail':
            run_alarm_device_Run_sendEMail()
        elif sys.argv[1] == 'alarm_AC_Err':
            run_alarm_AC_Err()
        else:
            print(f"Unknown function: {sys.argv[1]}")
    else:
        print("Usage: python script.py <function_name>")

    # ------測試區------
    # run_alarm_device_Run()
