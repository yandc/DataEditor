
#!/usr/bin/python
# -*- coding: utf-8 -*-
import imaplib
from pyzmail import *
from envelopes import Envelope
import pdb

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
        mail = Envelope(toAddrList, self.username, subject, content)
        for f in files:
            mail.add_attachment(f)
        try:
            mail.send('smtp.'+self.server, login=self.username, password=self.password, tls=True)
        except Exception as e:
            print e

    def recvEmail(self, criterion = 'Unseen'):
        result = []
        try:
            imap = imaplib.IMAP4_SSL('imap.'+self.server)
            imap.login(self.username, self.password)
            imap.select('INBOX')
            resp, items = imap.search(None, criterion)
            for i in items[0].split():
                pdb.set_trace()
                typ, content = imap.fetch(i, '(RFC822)')
                msg = PyzMessage.factory(content[0][1])
                imap.store(i, '+FLAGS', '\\seen')
                res = {
                    'from': msg.get_address('from')[1],
                    'cc': [x[1] for x in msg.get_addresses('cc')],
                    'subject': msg.get_subject(),
                    'content':[]
                }
                for part in msg.mailparts:
                    if part.is_body:
                        data = part.get_payload().decode(part.charset)
                        res['content'].append(data)
                    else:
                        filename = part.filename
                        data = part.get_payload()
                        res['content'].append({'filename':filename, 'data':data})
                result.append(res)
        except Exception as e:
            print e
        return result

if __name__ == "__main__":
    mail = EmailUtil('sina.com', 'dataspy@sina.com', 'YDC21415926')
    result = mail.recvEmail()
    print result
    
