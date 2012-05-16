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
__author__ = '<a href="nkhalasi@vayana.in">Naresh Khalasi</a>'

from sumpter import ctx_ref, drop_dict_ref
from sumpter import Drop
from sumpter.communication.mail import SmtpMailSender
from smtpd import DebuggingServer
import unittest
import asyncore
import thread
import sys

# a simple class with a write method
class WritableObject:
    def __init__(self):
        self.content = []
    def write(self, string):
        self.content.append(string)

class TestMail(unittest.TestCase):
    def setUp(self):
        self.mailserver = DebuggingServer(('localhost',20025), None)
        thread.start_new_thread(asyncore.loop,())

    def tearDown(self):
        self.mailserver.close()

    def testHTMLFormattedMailSendWithoutAttachments(self):
        pype_ctx = {'to_addrs':'abc@def.com', 'subject':'Test Subject'}
        pype_val = {'body':'This is a test mail', 'files':[], 'body_mimetype':'html', 'reply_to':'somename@replyto.com', 'from_addr': 'override@overridden.com'}

        pype = SmtpMailSender(
            'mailsendtester',
            subject = ctx_ref('subject'),
            from_addr = drop_dict_ref('from_addr', default='xyz@def.com'),
            to_addrs = ctx_ref('to_addrs'),
            body = drop_dict_ref('body'),
            body_mimetype = drop_dict_ref('body_mimetype', default='plain'),
            debug = drop_dict_ref('debug', default=0),
            files = drop_dict_ref('files'),
            reply_to = drop_dict_ref('reply_to'),
            host = 'localhost',
            port = 20025,
            tls = False
        )
        mail_content_collector = WritableObject()
        __original_stdout, sys.stdout = sys.stdout, mail_content_collector
        pype.send(Drop(pype_ctx,pype_val))
        sys.stdout = __original_stdout
        mail_dump = [a for a in mail_content_collector.content if a != '\n']
        self.__validate_html_mail_dump_with_no_attachments__(mail_dump)

    def __validate_html_mail_dump_with_no_attachments__(self, mail_dump):
        self.assertEquals(len(mail_dump), 19)
        self.assertEquals('Subject: Test Subject', mail_dump[3])
        self.assertEquals('From: override@overridden.com', mail_dump[4])
        self.assertEquals('To: abc@def.com', mail_dump[5])
        self.assertEquals('Reply-To: somename@replyto.com', mail_dump[6])

