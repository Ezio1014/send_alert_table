import os
import email.message  # 準備訊息物件設定
import smtplib  # 連線到SMTP Sever
import configparser
from pathlib import Path


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

    def send_mail(self, name, recipient, msg_Subject, dialogue, table_Subject, mail_content, attachment=None):
        # 建立訊息物件
        msg = email.message.EmailMessage()

        # 利用物件建立基本設定
        msg["From"] = self.config["Sender"]
        msg["To"] = recipient
        msg["Subject"] = msg_Subject  # 郵件主旨

        # 寄送郵件主要內容
        msg_content = f"<h2>{name}您好，</h2>{dialogue}<h3>{table_Subject}：</h3>\n{mail_content}"
        msg.add_alternative(msg_content, subtype="html")

        # 檢查是否需要加入附件
        if attachment == 1:
            # 附件路徑
            base_dir = Path(__file__).resolve().parent
            attachment_path = base_dir / "file/冰箱初步異常排除學習卡.pdf"

            if attachment_path.exists():
                with open(attachment_path, "rb") as at:
                    file_data = at.read()
                    file_name = attachment_path.name
                    msg.add_attachment(file_data, maintype="application", subtype="pdf", filename=file_name)
            else:
                print(f"附件 {attachment_path} 不存在，無法附加至郵件。")

        # 發送郵件
        try:
            server = smtplib.SMTP_SSL("smtp.gmail.com", 465)  # 建立gmail連線驗證
            server.login(self.config["acc_in"], self.config["pwd_in"])
            server.send_message(msg)
            server.close()  # 發送完成後關閉連線
            print(f"{name} 郵件已成功發送。")
        except Exception as e:
            print(f"{name}發送郵件時發生錯誤: {e}")
            raise


# ----------測試區----------
if __name__ == '__main__':
    pass
