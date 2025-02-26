import os
import json
import requests
import pandas as pd
from datetime import datetime
from DB.DB_API import insert_alarm_DeviceRun


def save2excel():
    # 載入 JSON 檔案
    current_dir = os.path.dirname(os.path.abspath(__file__))
    json_file_path = os.path.join(current_dir, '..', 'Member_info', 'alarm_device_Run.json')

    with open(json_file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)

    # 存放 Li 和 AC 不符合條件的設備
    device_list = []

    # 遍歷每個門市進行 API 查詢
    for store_name, site_id in json_data['Store'].items():
        # 發送 API 請求
        url = "http://114.34.49.41:18070/get/device/data"
        body = {
            "SiteID": site_id
        }

        try:
            response = requests.post(url, json=body)
            response.raise_for_status()  # 如果回應狀態碼不為 200，將會拋出錯誤
            data = response.json()

            # 判斷 Li 設備
            for key, value in data.items():
                if key.startswith("Li"):  # 找到 Li 開頭的設備
                    lig_value = value["Data"].get("Lig")
                    if lig_value != "0":
                        device_list.append({
                            "門市名稱": store_name,
                            "門市編號": site_id,
                            "設備編號": key,
                            "設備名稱": value.get("name", "Unknown"),
                            "運作狀態": lig_value
                        })

                # 判斷 AC 設備
                if key.startswith("AC"):  # 找到 AC 開頭的設備
                    run_value = value["Data"].get("Run")
                    if run_value != "0":
                        device_list.append({
                            "門市名稱": store_name,
                            "門市編號": site_id,
                            "設備編號": key,
                            "設備名稱": value.get("name", "Unknown"),
                            "運作狀態": run_value
                        })

        except requests.exceptions.RequestException as e:
            print(f"API 請求失敗，門市: {store_name}, 錯誤: {e}")

    df_devices = pd.DataFrame(device_list)  # 將設備列表轉換為 DataFrame
    df_devices = df_devices[df_devices['設備名稱'] != 'NONE']  # 過濾

    df_sorted = df_devices.sort_values(by=['門市編號', '設備編號'])  # 排序

    # 獲取當前日期，並生成檔案名稱
    current_date = datetime.now().strftime('%Y-%m-%d')  # 格式化日期為 YYYY-MM-DD
    file_name = f"{current_date}.xlsx"

    # 定義保存路徑，路徑為上一層的 data/alarm_device_Run 資料夾
    parent_dir = os.path.dirname(current_dir)
    save_dir = os.path.join(parent_dir, 'data', 'alarm_device_Run')

    # 檢查資料夾是否存在，如果不存在則創建
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    # 完整的保存路徑
    file_path = os.path.join(save_dir, file_name)

    # 將排序後的資料保存為 Excel 檔案
    df_sorted.to_excel(file_path, index=False)
    print(f"檔案已保存到: {file_path}")

    try:
        # 寫入資料庫
        df_filtered = df_sorted[df_sorted['運作狀態'] != 'NONE']  # 過濾掉運作狀態為 'NONE' 的資料
        insert_alarm_DeviceRun(df_filtered)
    except KeyError as e:
        print(f"資料處理時發生錯誤：欄位缺失 - {e}")
    except Exception as e:
        print(f"寫入資料庫時發生錯誤：{e}")


def save2SQL_MI():
    pass


def filter_data_by_store(store, excel_file_path):
    # 讀取EXCEL檔案
    df = pd.read_excel(excel_file_path)

    # 根據JSON檔案進行篩選
    # 如果store是"ALL"，則不篩選，印出所有資料
    if store == "ALL":
        return df
    else:
        # 否則根據store編號進行篩選
        filtered_df = df[df['門市編號'].isin(store)]
        return filtered_df


if __name__ == '__main__':
    # save2excel()
    pass
