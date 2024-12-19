import os
import pandas as pd
from datetime import datetime, time, timedelta
from DB import DB_API
import pymysql

# 創建 DB 實例
# db_SQL_MI = DB_API.DB_SQL_MI()
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)  # 避免自動折行顯示

# 路徑設定
excel_path = os.path.join(os.getcwd(), "./data")  # 總表
log_path = os.path.join(os.getcwd(), "./log")  # LOG

error = 0


# 計算時間差
def calculate_time_difference(start_time, end_time):
    time_diff = end_time - start_time
    return time_diff.total_seconds() / 3600  # 將秒轉換為小時


# 尋找連續的 True 頭尾並計算時間差
def find_continual_true(data):
    # 日期轉換
    def date_trans(d):
        return d.date()
    continual_true = []
    start_time = None

    for idx, row in data.iterrows():
        receive_time = row['receiveTime']
        compare = row['Compare']

        if compare == "True":
            if start_time is None:
                start_time = receive_time
            # 20240111 新增判定：compare 為 True 且 start_time 為當日最後一筆資料時，跨日後碰上 compare = True
            elif start_time is not None and date_trans(start_time) != date_trans(receive_time):
                end_time = datetime.combine(start_time.date(), time(9, 0, 0))
                continual_true.append((start_time, end_time))
                start_time = receive_time
        else:
            if start_time is not None and date_trans(start_time) == date_trans(receive_time):
                end_time = receive_time
                continual_true.append((start_time, end_time))
                start_time = None
            # 20240111 新增判定：compare 為 True 且 start_time 為當日最後一筆資料時，跨日後碰上 compare = False
            elif start_time is not None and date_trans(start_time) != date_trans(receive_time):
                end_time = datetime.combine(start_time.date(), time(9, 0, 0))
                continual_true.append((start_time, end_time))
                start_time = None

    if start_time is not None:
        end_time = data.iloc[-1]['receiveTime']
        continual_true.append((start_time, end_time))

    return continual_true


#  Excel檔案儲存(王品)
def save_results_to_excel(units_NO, units, storesID, storesName, deviceID, devices, temp_type,
                          abnormal_dates, abnormal_times, durations, min_temp, action, file_path):
    # 格式化日期
    for i in range(len(abnormal_dates)):
        formatted_date = datetime.strptime(abnormal_dates[i], '%Y-%m-%d').date().strftime('%m月%d日').lstrip('0')
        abnormal_dates[i] = formatted_date

    # 檔案路徑
    full_path = os.path.join(file_path, f"WP_Abnormal_device_30days.xlsx")

    # 創建資料夾(如果不存在的話)
    if not os.path.exists(file_path):
        os.makedirs(file_path)

    # Excel 格式
    excel_format = {'事業處編號': units_NO,
                    '事業處': units,
                    '店編': storesID,
                    '店別': storesName,
                    'DeviceID': deviceID,
                    '設備編號': devices,
                    '溫層設定': temp_type,
                    '異常判定': '異常',
                    '判定日期': abnormal_dates,
                    '異常時間': abnormal_times,
                    '持續時間(小時)': durations,
                    '區間最低溫(攝氏)': min_temp,
                    '異常天數': '',
                    '行動方案': action}

    # 創建Excel(如果不存在的話)
    if not os.path.exists(full_path):
        df = pd.DataFrame(excel_format)
        df.to_excel(full_path, sheet_name='異常設備列表', index=False, na_rep='NULL')
    else:
        with pd.ExcelWriter(full_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
            df = pd.DataFrame(excel_format)

            existing_df = pd.read_excel(full_path, sheet_name='異常設備列表')  # 讀取現有的 Excel 文件
            combined_df = pd.concat([existing_df, df], ignore_index=True)  # 合併兩個 DataFrame
            combined_df.to_excel(writer, index=False, sheet_name='異常設備列表', na_rep='NULL')  # 將合併後的 DF 寫入工作表


def run(numbers, names, storeID, storeName, date, brand_name, siteID, alarmType):
    # 儲存結果的列表
    units_NO = []        # 事業處編號
    units = []           # 事業處
    stores_ID = []       # 店編
    stores_name = []     # 店別
    deviceID = []        # 設備ID
    devices = []         # 設備編號
    temp_type = []       # 溫層設定
    abnormal_dates = []  # 判定日期
    abnormal_times = []  # 異常時間
    durations = []       # 持續時間(小時)
    min_temp = []        # 區間最低溫(攝氏)
    action = []          # 行動方案

    # 其他設定
    tempType_dict = {1: "冷凍", 2: "解凍", 3: "冷藏"}  # 溫層類別

    for i in range(len(numbers)):
        dataframes = []
        devices_type = alarmType[i]
        device_ID, device_Name = numbers[i], names[i]
        try:
            # df = DB_API.alarm_sql_query_SQLMI(numbers[i], names[i], devices_type, date)  # DB_SQL_MI
            df = DB_API.alarm_sql_query_SQLMI_Test(numbers[i], names[i], devices_type, date)  # 每日資料

            dataframes.append(df)
            combined_df = pd.concat(dataframes, ignore_index=True)  # 將多個 DataFrame 合併成一個
            combined_df['receiveTime'] = pd.to_datetime(combined_df['receiveTime'])  # 將 receiveTime 轉換為 datetime 格式
            continual_true_intervals = find_continual_true(combined_df)  # 尋找連續的 True 頭尾並計算時間差

            deviceName = ''
            for interval in continual_true_intervals:
                start_time, end_time = interval
                time_diff = calculate_time_difference(start_time, end_time)
                if time_diff <= 1:
                    continue
                if deviceName != names[i]:
                    deviceName = names[i]

                # DB_SQL_MI 從結果中提取最小溫度值
                min_temperatures = DB_API.min_temperatures_SQLMI(numbers[i], start_time, end_time)

                print(f"事業處：{brand_name}，店別：{storeName}，設備編號：{names[i]}，"
                      f"異常時間：{start_time.strftime('%H:%M:%S')} 至 {end_time.strftime('%H:%M:%S')}，"
                      f"持續時間：{time_diff:.2f}小時，區間最低溫：{min_temperatures}")

                # 將當前設備的結果加入總結果列表
                units_NO.append(f"{siteID}")
                units.append(f"{brand_name}")
                stores_ID.append(f"{storeID}")
                stores_name.append(f"{storeName}")
                deviceID.append(f"{numbers[i]}")
                devices.append(f"{names[i]}")
                temp_type.append(tempType_dict[devices_type])
                abnormal_dates.append(f"{start_time.strftime('%Y-%m-%d')}")
                abnormal_times.append(f"{start_time.strftime('%H:%M:%S')} 至 {end_time.strftime('%H:%M:%S')}")
                durations.append(f"{time_diff:.2f}")
                min_temp.append(f"{min_temperatures}")

                # 行動方案 action 邏輯
                min_temperatures_yesterday = DB_API.min_temperatures_SQLMI_yesterday(numbers[i], start_time, end_time)
                if devices_type == 1:  # 冷凍
                    if min_temperatures_yesterday is None:
                        action.append(f"yesterday temp None")
                    elif min_temperatures is None:
                        action.append(f"today temp None")
                    elif int(min_temperatures_yesterday) < -18 and int(min_temperatures) < -18:
                        action.append(f"納入年度保養")
                    elif int(min_temperatures_yesterday) < -18:
                        action.append(f"測試夜間散熱方案")
                    else:
                        action.append(f"派員檢修")
                elif devices_type == 3:
                    if time_diff > 2:  # 冷藏
                        action.append(f"派員檢修")
                    else:
                        action.append(f"-")
                else:
                    action.append(f"-")

        except pymysql.err.ProgrammingError as e:
            # except 基本設定
            global error
            error_msg = f'device_ID：{device_ID}\ndevice_Name：{device_Name}\nERROR Msg：{e}'
            log_file_name = os.path.join(log_path, f'{date}_log.txt')

            if not os.path.exists(log_file_name):
                with open(log_file_name, 'w'):
                    pass
            with open(log_file_name, 'a') as log_file:
                log_file.write(error_msg + '\n')
            error += 1
            # print(error_msg)

    # 存檔
    save_results_to_excel(units_NO, units, stores_ID, stores_name, deviceID, devices, temp_type, abnormal_dates, abnormal_times,
                          durations, min_temp, action, excel_path)


def get_Devices_Data(date):
    siteID_result = DB_API.get_store_info()
    df_sites = pd.DataFrame(siteID_result, columns=["id", "name", "brand", "bname"])  # 將查詢結果轉換為 DataFrame
    site_ids = df_sites["id"].tolist()          # 提取門市編號
    site_names = df_sites["name"].tolist()      # 提取門市名稱
    brand_ids = df_sites["brand"].tolist()      # 提取門市所屬品牌編號
    brand_names = df_sites["bname"].tolist()    # 提取門市所屬品牌名稱

    for n in range(len(site_ids)):
        deviceID_result = DB_API.get_devices_info(site_ids[n])
        df_device = pd.DataFrame(deviceID_result, columns=["id", "name", "alarm_type"])  # 將查詢結果轉換為 DataFrame
        df_device = df_device[df_device['name'].notna() & df_device['name'].str.strip().ne('')]

        device_ids = df_device["id"].tolist()                   # 初始化 device_ids 變數
        device_names = df_device["name"].tolist()               # 初始化 device_names 變數
        device_alarmType = df_device["alarm_type"].tolist()     # 初始化 device_alarmType 變數

        print(f'店鋪ID：{site_ids[n]}\n店鋪名稱：{site_names[n]}\n設備ID：{device_ids}\n設備名稱：{device_names}')
        run(device_ids, device_names, site_ids[n], site_names[n], date, brand_names[n], brand_ids[n], device_alarmType)


# 主程式
def data_save2excel(date):
    # 計算連續異常天數
    def calculate_consecutive_days(group):
        group = group.reset_index(drop=True)
        group['判定日期'] = pd.to_datetime(group['判定日期'], format='%m月%d日')
        consecutive_days = 0
        last_date = None
        unique_dates = group['判定日期'].drop_duplicates().reset_index(drop=True)

        for idx in range(len(unique_dates) - 1, -1, -1):
            current_date = unique_dates[idx]
            if last_date is None or (last_date - current_date).days == 1:
                consecutive_days += 1
            else:
                break
            last_date = current_date

        group.loc[group.index[-1], '異常天數'] = consecutive_days  # 填入最後一行
        group['判定日期'] = group['判定日期'].dt.strftime('%m月%d日').str.lstrip('0')  # 還原格式
        return group

    # Basic Setting  **刪除 14 天前的資料**
    full_path = os.path.join(excel_path, f"WP_Abnormal_device_30days.xlsx")  # 檔案路徑

    today = datetime.strptime(date, '%Y-%m-%d')
    cutoff_today = today
    cutoff_30days = today - timedelta(days=30)  # 計算14天前的日期

    cutoff_today_str = cutoff_today.strftime('%m月%d日').lstrip('0')
    cutoff_14days_str = cutoff_30days.strftime('%m月%d日').lstrip('0')  # 格式化為字串與 Excel 中的日期匹配

    # 讀取 Excel 並清洗資料
    df = pd.read_excel(full_path, sheet_name='異常設備列表')
    df = df[df['判定日期'] > cutoff_14days_str]     # 刪除判定日期早於或等於截止日期的資料(刪除14天前資料)
    df = df[df['判定日期'] < cutoff_today_str]      # 刪除判定日期等於當日日期的資料(重覆執行時要先刪除舊的)

    # 將更新後的資料寫回 Excel
    with pd.ExcelWriter(full_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        df.to_excel(writer, index=False, sheet_name='異常設備列表', na_rep='NULL')

    # -執行異常設備查詢-
    get_Devices_Data(date)

    # 累積天數計算
    df = pd.read_excel(full_path, sheet_name='異常設備列表')  # 讀取更新後的 Excel 資料
    df = df.sort_values(by=['事業處編號', '店編', 'DeviceID', '判定日期'])  # 排序資料

    # 根據 DeviceID 分群計算異常天數
    df['異常天數'] = 0
    df = df.groupby(['DeviceID'], group_keys=False).apply(calculate_consecutive_days)

    # 更新 Excel
    with pd.ExcelWriter(full_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        df.to_excel(writer, index=False, sheet_name='異常設備列表', na_rep='NULL')


# ----------測試區----------
if __name__ == '__main__':
    # get_Devices_Data('2024-12-17')
    data_save2excel('2024-12-17')
    pass
