#############################################################################
#    Copyright 2010 Dhananjay Nene 
#    
#    Licensed under the Apache License, Version 2.0 (the "License"); 
#    you may not use this file except in compliance with the License. 
#    You may obtain a copy of the License at 
#        
#        http://www.apache.org/licenses/LICENSE-2.0 
#    
#    Unless required by applicable law or agreed to in writing, software 
#    distributed under the License is distributed on an "AS IS" BASIS, 
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
#    See the License for the specific language governing permissions and 
#    limitations under the License.
############################################################################# 

from email.encoders import encode_base64
from email.mime.application import MIMEApplication
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from sumpter import Segment, Config
import email
import mimetypes
import smtplib

class SmtpMailSender(Segment):

    def attach_file(self, msg, filename, fp, ctype):
        maintype, subtype = ctype.split('/', 1)
        if maintype == 'text':
            attach = MIMEText(fp.read(), _subtype=subtype)
        elif maintype == 'message':
            attach = email.message_from_file(fp)
        elif maintype == 'image':
            attach = MIMEImage(fp.read(), _subtype=subtype)
        elif maintype == 'audio':
            attach = MIMEAudio(fp.read(), _subtype=subtype)
        elif maintype == 'application':
            attach = MIMEApplication(fp.read(), _subtype=subtype)
        else:
            attach = MIMEBase(maintype, subtype)
            attach.set_payload(fp.read())
            encode_base64(attach)
            
        attach.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(attach)

    def perform(self,ctx,val):

        config = Config(self,ctx,val,'from_addr','to_addrs','subject','body','files','host','port','tls','username','password')
        recipients = config.to_addrs.split(',')

        msg = MIMEMultipart()
        msg['Subject'] = config.subject
        msg['From'] = config.from_addr
        msg['To'] = config.to_addrs
        msg.attach(MIMEText(config.body, 'plain'))

        for filespec in config.files :
            if isinstance (filespec,tuple) :
                filename,fp,ctype = filespec
                self.attach_file(msg, filename, fp, ctype)
            else :
                filename = filespec
                ctype, encoding = mimetypes.guess_type(filename)
                if ctype is None or encoding is not None :
                    ctype='application/octet-stream'
                with file(filename) as fp :
                    self.attach_file(msg, filename, fp, ctype)

        # The actual mail send
        server = smtplib.SMTP('%s:%s'%(config.host,config.port))
        if config.tls is True :
            server.starttls()
        server.login(config.username,config.password)
        server.sendmail(config.from_addr, recipients, msg.as_string())
        server.quit()
        