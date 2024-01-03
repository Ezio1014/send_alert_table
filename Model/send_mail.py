import email.message  # 準備訊息物件設定
import smtplib  # 連線到SMTP Sever
import configparser


class mail_setting:
    def __init__(self):
        config = configparser.ConfigParser()
        config.read('./.config/config')

        self.config = {
            "Sender": config.get('mailSender_formal', 'Sender'),
            "acc_in": config.get('mailSender_formal', 'acc_in'),
            "pwd_in": config.get('mailSender_formal', 'pwd_in')
        }

    def send_mail(self, recipient, mail_content, name, msg_Subject=None):
        # 建立訊息物件
        msg = email.message.EmailMessage()

        # 利用物件建立基本設定
        msg["From"] = self.config["Sender"]
        msg["To"] = recipient

        # msg_Subject(收件人)，王品：1, 209：2
        if msg_Subject == 1:
            msg["Subject"] = "王品/群品 冷櫃溫度異常發信"
        elif msg_Subject == 2:
            msg["Subject"] = "209設備斷線發信"
        else:
            pass

        # 寄送郵件主要內容
        # msg.set_content("測試郵件純文字內容") #純文字信件內容
        msg.add_alternative(f"<h3>{name}您好，</h3>異常報表：\n{mail_content}", subtype="html")  # HTML信件內容

        server = smtplib.SMTP_SSL("smtp.gmail.com", 465)  # 建立gmail連驗
        server.login(self.config["acc_in"], self.config["pwd_in"])
        server.send_message(msg)
        server.close()  # 發送完成後關閉連線


# ----------測試區----------
if __name__ == '__main__':
    pass
    # import pandas as pd
    # fileName = str(datetime.now().date())
    # df = pd.read_excel(f"../data/{fileName}.xlsx")
    #
    # html_table = df.to_html(index=False, classes='table table-condensed table-bordered')
    # html_table = html_table.replace('<th>', '<th style="text-align: center;">')
    # html_table = html_table.replace('<td>', '<td style="text-align: center;">')

    # Mail = mail_setting()
    # addr = "a224607@gmail.com"
    # Mail.send_mail(addr, html_table)
