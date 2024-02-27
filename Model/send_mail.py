import os
import email.message  # 準備訊息物件設定
import smtplib  # 連線到SMTP Sever
import configparser


class mail_setting:
    def __init__(self):
        config = configparser.ConfigParser()
        configPATH = './.config/config' if os.path.isfile('./.config/config') else '../.config/config'
        config.read(configPATH)

        self.config = {
            "Sender": config.get('mailSender_formal', 'Sender'),
            "acc_in": config.get('mailSender_formal', 'acc_in'),
            "pwd_in": config.get('mailSender_formal', 'pwd_in')
        }

    def send_mail(self, name, recipient, msg_Subject, table_Subject, mail_content):
        # 建立訊息物件
        msg = email.message.EmailMessage()

        # 利用物件建立基本設定
        msg["From"] = self.config["Sender"]
        msg["To"] = recipient
        msg["Subject"] = msg_Subject  # 郵件主旨

        # 寄送郵件主要內容
        msg_content = f"<h2>{name}您好，</h2><h3>{table_Subject}：</h3>\n{mail_content}"
        msg.add_alternative(msg_content, subtype="html")

        server = smtplib.SMTP_SSL("smtp.gmail.com", 465)  # 建立gmail連線驗證
        server.login(self.config["acc_in"], self.config["pwd_in"])
        server.send_message(msg)
        server.close()  # 發送完成後關閉連線


# ----------測試區----------
if __name__ == '__main__':
    pass
