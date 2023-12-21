from datetime import datetime, timedelta
import pandas as pd
import configparser

# 基本設定
config = configparser.ConfigParser()
config.read('./.config/config')
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)  # 避免自動折行顯示
fileName = str((datetime.now().date()) - timedelta(days=int(config.get('fileDate', 'days'))))
excel_path = f"./data/{fileName}.xlsx"


def build_html_table(table):
    html_table = table.to_html(index=False, classes='table table-condensed table-bordered')
    html_table = html_table.replace('<th>', '<th style="text-align: center;">')
    html_table = html_table.replace('<td>', '<td style="text-align: center;">')
    return html_table


# 主程式
def df_dealing(dep_number, store=None):
    # Excel 總表讀取
    df = pd.read_excel(excel_path)

    # 製表
    table = None
    if dep_number == "0" or dep_number == "1":
        table = df
    elif dep_number == "2":
        filtered_data = df[df['溫層設定'] == '冷藏']
        table = filtered_data[filtered_data['持續時間(小時)'] > 2]
    elif dep_number == "3":
        table = df[df['店編'].isin(store)]
    elif dep_number == "4":
        table = df[df['店編'].isin(store)]

    table = table.drop(columns=["事業處編號", "店編"])
    if table.shape[0] == 0:
        return 'empty'
    else:
        table = build_html_table(table)
        return table


# ----------測試區----------
if __name__ == '__main__':
    # function
    excel_path = f"../data/{fileName}.xlsx"
    df_dealing("3", "王品牛排")
