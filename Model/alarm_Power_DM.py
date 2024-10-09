import os
from datetime import datetime
import pandas as pd
from DB import DB_API


def save2excel():
    # 獲取資料
    data1 = DB_API.alarm_DM()

    # 過濾 receiveTime 和 value 不為 None 的資料
    filtered_data = data1[(data1['觸發時間'].notna()) & (data1['數值'].notna())]

    # 獲取當前日期，並生成檔案名稱
    current_date = datetime.now().strftime('%Y-%m-%d')  # 格式化日期為 YYYY-MM-DD
    file_name = f"{current_date}.xlsx"

    # 定義保存路徑，路徑為上一層的 data/alarm_DM 資料夾
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    save_dir = os.path.join(parent_dir, 'data', 'alarm_Power_DM')

    # 檢查資料夾是否存在，如果不存在則創建
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    # 完整的保存路徑
    file_path = os.path.join(save_dir, file_name)

    # 將過濾後的資料保存為 Excel 檔案
    filtered_data.to_excel(file_path, index=False)
    print(f"檔案已保存到: {file_path}")


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


# ----------測試區----------
if __name__ == '__main__':
    # save2excel()
    #
    # # 載入JSON檔案
    # current_dir = os.path.dirname(os.path.abspath(__file__))
    # json_file_path = os.path.join(current_dir, '..', 'Member_info', 'alarm_Power_DM.json')
    #
    # 獲取當前日期並生成EXCEL檔案名
    # current_date = datetime.now().strftime('%Y-%m-%d')
    # excel_file_path = f"../data/alarm_DM/{current_date}.xlsx"  # EXCEL檔案在此路徑
    #
    # with open(json_file_path, 'r', encoding='utf-8') as f:
    #     json_data = json.load(f)
    #
    #     # 根據JSON中的Member進行篩選和打印
    # for member_id, member_info in json_data['Member'].items():
    #     store = member_info['store']
    #     result_df = filter_data_by_store(store, excel_file_path)
    #
    #     # 打印篩選結果
    #     print(f"篩選結果 - {member_info['name']}:")
    #     print(result_df)
    #     print("\n")  # 每個篩選結果之間留一個空行
    pass
