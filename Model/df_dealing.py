from datetime import datetime, timedelta
# import os
import pandas as pd
# import configparser

# 基本設定
# config = configparser.ConfigParser()
# configPATH = './.config/config' if os.path.isfile('./.config/config') else '../.config/config'
# config.read(configPATH)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)  # 避免自動折行顯示
# fileName = str((datetime.now().date()) - timedelta(days=int(config.get('fileDate', 'days'))))
excel_path = f"./data/WP_Abnormal_device_30days.xlsx"


def build_html_table(table):
    html_table = table.to_html(index=False, classes='table table-condensed table-bordered')
    html_table = html_table.replace('<th>', '<th style="text-align: center;">')
    html_table = html_table.replace('<td>', '<td style="text-align: center;">')
    return html_table


# 王品主程式
def df_dealing(dep_number, store=None):
    # 篩選條件設定
    def today_date():
        start_date = datetime.strptime(str((datetime.now().date())), '%Y-%m-%d').date()
        todayDate = start_date.strftime('%m月%d日').lstrip('0')  # 格式化日期
        return todayDate

    def continual_date(days):
        date_list = []
        for i in range(days):
            start_date = datetime.strptime(str((datetime.now().date() - timedelta(days=i))), '%Y-%m-%d').date()
            Date = start_date.strftime('%m月%d日').lstrip('0')  # 格式化日期
            date_list.append(Date)
        return date_list

    df = pd.read_excel(excel_path)  # Excel 總表讀取
    table = None  # 製表

    # 當天異常傳送
    if dep_number == "0":
        date_today = today_date()
        table = df[(df['判定日期'] == date_today)]
    # 連續14天異常傳送
    elif dep_number == "1":
        continual_date = continual_date(14)
        tb = df[df['判定日期'].isin(continual_date)]
        sites_list = tb['店編'].unique().tolist()

        for site in sites_list:
            site_tb = df[df['店編'] == site]
            unique_devices = site_tb['設備編號'].unique().tolist()
            for device in unique_devices:
                device_tb = site_tb[site_tb['設備編號'] == device]
                days_list = device_tb['判定日期'].unique().tolist()
                # print(days_list)
                if len(days_list) > 13:
                    df.drop(df[(df['設備編號'] == device) & (df['店編'] == site)].index, inplace=True)

        date_today = today_date()
        table = df[(df['判定日期'] == date_today)]
    # 當天冷藏異常傳送
    elif dep_number == "2":
        date_today = today_date()
        table = df[(df['判定日期'] == date_today) & (df['溫層設定'] == '冷藏') & (df['持續時間(小時)'] > 2)]
    # 當天設備異常傳送(門店主管)有多個門店
    elif dep_number == "3":
        continual_date = continual_date(7)
        tb = df[(df['店編'].isin(store)) & (df['判定日期'].isin(continual_date))]
        sites_list = tb['店編'].unique().tolist()

        for site in sites_list:
            site_tb = tb[tb['店編'] == site]
            unique_devices = site_tb['設備編號'].unique().tolist()
            for device in unique_devices:
                device_tb = site_tb[site_tb['設備編號'] == device]
                days_list = device_tb['判定日期'].unique().tolist()
                if len(days_list) > 6:
                    df.drop(tb[(tb['設備編號'] == device) & (tb['店編'] == site)].index, inplace=True)

        df_7day = df[(df['店編'].isin(store)) & (df['判定日期'].isin(continual_date))]
        date_today = today_date()
        table = df_7day[df_7day['判定日期'] == date_today]

    elif dep_number == "4":  # 當天設備異常傳送(指定門店)
        date_today = today_date()
        table = df[(df['判定日期'] == date_today) & (df['店編'].isin(store))]

    table = table.drop(columns=["事業處編號", "店編", "DeviceID"])

    if table.shape[0] == 0:
        return 'empty'
    else:
        table = build_html_table(table)
        return table


# ----------測試區----------
if __name__ == '__main__':
    # function
    pass
