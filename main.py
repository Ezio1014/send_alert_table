# 系統套件
import os
import sys
import json
import time
# import configparser
import logging
from datetime import datetime, timedelta

# 程式套件
from Model.data_save2excel import data_save2excel
from Model.df_dealing import df_dealing, build_html_table
from Model.send_mail import mail_setting
from Model import alarm_Power_DM, alarm_EV_CO2, alarm_AC_Err, alarm_device_Run, alarm_Water_TFV
from DB.DB_API import AC_unclosed_alarm, getAlertList

# 王品客製化警報
from DB.DB_API import member_EN, member_FS, member_MA, member_Store, member_SA, get_alarm_sites

# 永曜設備斷線警報
from DB.DB_API import device_disconnect_member, device_disconnect_SQLMI

# 棄用套件
# from DB.DB_API import device_disconnect

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
def sendMail(name, addr, msg_Subject, dialogue, table_Subject, mail_content, attachment):
    Mail = mail_setting()
    Mail.send_mail(name, addr, msg_Subject, dialogue, table_Subject, mail_content, attachment)


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
def run_alert_WOWprime(fileDate=None):
    # 獲取當日資料 fileDate=0
    if fileDate is not None and isinstance(fileDate, int):
        today = datetime.now().date() - timedelta(days=fileDate)
    else:
        today = datetime.now().date()

    def main_sendEmail(df, attachment, store, dialogue):
        for index, row in df.iterrows():
            dep_name = row["name"]
            email = row["email"]
            siteID = row["siteID"]
            memberID = row["memberID"]

            if store == 'ALL' or store == '冷藏':
                html_table = df_dealing(today, str(dep_NO))
            elif store == '區經理':
                try:
                    store_list = get_alarm_sites(memberID)  # 提取 store 鍵的值，默認為空列表
                    html_table = df_dealing(today, str(dep_NO), store_list)
                except json.JSONDecodeError:
                    print(f"無效的 store 格式: {store}")
                    continue
            elif store == '門市':
                try:
                    store_list = [siteID] if siteID else []  # 提取 siteID 鍵的值，默認為空列表
                    html_table = df_dealing(today, str(dep_NO), store_list)
                except json.JSONDecodeError:
                    print(f"無效的 store 格式: {store}")
                    continue
            else:
                continue

            if html_table == "empty":
                print(f"收件人：{dep_name} is empty")
                continue
            else:
                try:
                    sendMail(dep_name, email, "王品/群品 冷櫃溫度異常發信", dialogue, "冷櫃溫度異常報表", html_table, attachment)
                except Exception as e:
                    logger.error("{} Mail發送失敗，未知錯誤：{}".format(dep_name, str(e)))  # 捕捉其他未知錯誤

    data_save2excel(str(today))  # 執行異常設備數據查詢

    for dep_NO in range(5):
        if dep_NO == 0:
            member_info = member_SA()
            main_sendEmail(member_info, 0, 'ALL', '<h3>資訊部</h3>')
        elif dep_NO == 1:
            member_info = member_EN()
            main_sendEmail(member_info, 0, 'ALL', '<h3>工程部</h3>')
        elif dep_NO == 2:
            member_info = member_FS()
            main_sendEmail(member_info, 0, '冷藏', '<h3>食安部</h3>')
        elif dep_NO == 3:
            member_info = member_MA()
            main_sendEmail(member_info, 1, '區經理', '<h3>您轄區門店今日異常設備清單入下，請協助確認門店改善進度，謝謝~</h3>')
        elif dep_NO == 4:
            member_info = member_Store()
            main_sendEmail(member_info, 1, '門市', '<h3>今日異常設備清單如下，請先依附件學習卡進行初步異常排除</h3>'
                                                  '<h3>如果隔天還是有異常，請使用就修中心進行報修，謝謝~</h3>')


# 永曜設備斷線主程式
def run_device_disconnect():
    member_list = device_disconnect_member()
    table = device_disconnect_SQLMI()

    # 迴圈處理每一位成員的資料
    for _, mem in member_list.iterrows():
        # 建立要傳遞的 HTML 表格內容
        html_table = build_html_table(table)
        if html_table == 'empty':
            continue
        else:
            sendMail(mem["name"], mem["email"], "209設備斷線警報", '', "209設備斷線報表", html_table, 0)


# 華南資料庫 0830、1915 空調未關警報
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
            sendMail(name, addr, "IESS空調未關警報(0830)", '', "空調未關報表", html_table, 0)
    elif sendTime == '1915':
        df = AC_unclosed_alarm('1900')
        html_table = build_html_table(df)
        for name, addr in member_dict.items():
            sendMail(name, addr, "IESS空調未關警報(1915)", '', "空調未關報表", html_table, 0)


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
        sendMail(name, mail, "用電需量警報", '', "用電需量(DM)超過設定標準報表", html_table, 0)  # 發送郵件

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
        sendMail(name, mail, "CO2濃度超標警報", '', "CO2濃度超標報表", html_table, 0)  # 發送郵件


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
        sendMail(name, mail, "水流量數值未變化警報", '', "水流量數值報表", html_table, 0)  # 發送郵件


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
        result_df['運作狀態'] = result_df['運作狀態'].astype(str)
        result_df = result_df[result_df['運作狀態'].str.upper() != 'NONE']

        # 如果沒有內容則跳過此迴圈
        if result_df.empty:
            continue

        html_table = build_html_table(result_df)  # 有結果時生成HTML表格
        sendMail(name, mail, "設備未關警報", '', "設備未關報表", html_table, 0)  # 發送郵件


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
        sendMail(name, mail, "空調設備異常故障警報", '', "空調設備異常故障報表", html_table, 0)  # 發送郵件


#  --------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    # function
    if len(sys.argv) == 2:
        if sys.argv[1] == 'alert_WOWprime':  # 王品冷櫃警報
            run_alert_WOWprime()
        elif sys.argv[1] == 'device_disconnect':  # 永曜設備斷線警報
            run_device_disconnect()
        elif sys.argv[1] == 'AC_unclosed_0830':  # 華南資料庫 0830 空調未關警報
            run_AC_unclosed_alarm('0830')
        elif sys.argv[1] == 'AC_unclosed_1915':  # 華南資料庫 1915 空調未關警報
            run_AC_unclosed_alarm('1915')
        elif sys.argv[1] == 'UpdateAlertTable':  # 空調未關警報查詢
            getAlertList()
        elif sys.argv[1] == 'alarm_Power_DM':  # 需量(DM)警報
            run_alarm_Power_DM()
        elif sys.argv[1] == 'alarm_EV_CO2':  # CO2 濃度警報
            run_alarm_EV_CO2()
        elif sys.argv[1] == 'alarm_Water_TFV':  # 累積水流量警報
            run_alarm_Water_TFV()
        elif sys.argv[1] == 'alarm_device_Run_saveFile':  # 設備運作警報(存EXCEL)
            run_alarm_device_Run_saveFile()
        elif sys.argv[1] == 'alarm_device_Run_sendEMail':  # 設備運作警報(發送郵件)
            run_alarm_device_Run_sendEMail()
        elif sys.argv[1] == 'alarm_AC_Err':  # 空調異常警報
            run_alarm_AC_Err()
        else:
            print(f"Unknown function: {sys.argv[1]}")
    else:
        print("Usage: python script.py <function_name>")

    # ------測試區------
    # run_alert_WOWprime()
    pass
