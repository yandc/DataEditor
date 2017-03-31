
#!/usr/bin/python
# -*- coding: utf-8 -*-
from email.MIMEText import MIMEText
from email.mime.multipart import MIMEMultipart
from email.Header import Header
from email.Utils import formatdate
from email import parser
import smtplib
import imaplib

class EmailUtil:
    def __init__(self, server ="", username="", password=""):
        self.server = server
        self.username = username
        self.password = password
        
    def letter(self, subject, content, To, files):
        if isinstance(To, basestring):
            To = [To]
        mail = MIMEMultipart()
        mail['Subject'] = Header(subject, 'utf-8')
        mail['From'] = self.username
        mail['To'] = ';'.join(To)
        mail['Date'] = formatdate()

        text = MIMEText(content, 'html', 'utf-8')
        mail.attach(text)

        for f in files:
            part = MIMEText(open(f, 'rb').read(), 'base64', 'utf-8')
            part['Content-Type'] = 'application/octet-stream'
            part['Content-Disposition'] = 'attachment; filename="%s"'%f
            mail.attach(part)
        return mail.as_string()

    def sendEmail(self, toAddrList, subject, content='', files=[]):
        content = self.letter(subject, content, toAddrList, files)
        try:
            smtp = smtplib.SMTP_SSL('smtp.'+self.server, smtplib.SMTP_SSL_PORT)
            smtp.login(self.username, self.password)
            
            smtp.sendmail(self.username, toAddrList, content)
            smtp.close()
        except Exception as e:
            print e

    def recvEmail(self, criterion = 'Unseen'):
        result = []
        try:
            imap = imaplib.IMAP4_SSL('imap.'+self.server)
            imap.login(self.username, self.password)
            imap.select('INBOX')
            resp, items = imap.search(None, criterion)
            pdb.set_trace()
            for i in items[0].split():
                typ, content = imap.fetch(i, '(RFC822)')
                msg = email.message_from_string(content[0][1])
                res = {
                    'from':msg['From'].split(),
                    'subject':msg['Subject'],
                    'content':[],
                    'files':[]
                }
                for part in msg.walk():
                    filename = part.get_filename()
                    contentType = part.get_content_type()
                    if filename:
                        data = part.get_payload(decode=True)
                        res['files'].append(data)
                    else:
                        data = part.get_payload(decode=True)
                        res['content'].append(data)
                result.append(res)
        except Exception as e:
            print e
        return result

if __name__ == "__main__":
    mail = EmailUtil('sina.com', 'dataspy@sina.com', 'YDC21415926')
    #mail.sendEmail('yandechen@mia.com', 'test', 'test')
    result = mail.recvEmail()
    print result
    
