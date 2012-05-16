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
import unittest
from Queue import Queue
from lazr.smtptest.server import QueueServer
from lazr.smtptest.controller import Controller

class TestMail(unittest.TestCase):
    MAILSENDER_PYPE = SmtpMailSender(
        'mailsendtester',
        subject = ctx_ref('subject'),
        from_addr = drop_dict_ref('from_addr', default='xyz@def.com'),
        to_addrs = ctx_ref('to_addrs'),
        body = drop_dict_ref('body'),
        body_mimetype = drop_dict_ref('body_mimetype', default='plain'),
        debug = drop_dict_ref('debug', default=0),
        files = drop_dict_ref('files'),
        reply_to = drop_dict_ref('reply_to', optional=True),
        host = 'localhost',
        port = 9925,
        tls = False
    )

    def setUp(self):
        self.queue = Queue()
        self.server = QueueServer('localhost',9925,self.queue)
        self.controller = Controller(self.server)
        self.controller.start()
        self.controller.reset()
        self.mails = []

    def tearDown(self):
        self.controller.stop()

    def __read_mail__(self):
        from Queue import Empty
        while True:
            try:
                message = self.queue.get_nowait()
                self.mails.append(message)
#                import sys
#                print >> sys.stderr, '\n-----------------'
#                print >>sys.stderr, 'Headers\t==> ', self.mails[0].items()
#                print >>sys.stderr, 'Body\t==> ', self.mails[0].get_payload(0)
#                print >> sys.stderr, '-----------------\n'
            except Empty:
                break


    def test_if_mail_is_sent_correctly(self):
        pype_ctx = {'to_addrs':'abc@def.com', 'subject':'Test Subject'}
        pype_val = {'body':'This is a test mail', 'files':[]}

        TestMail.MAILSENDER_PYPE.send(Drop(pype_ctx,pype_val))
        self.__read_mail__()
        self.assertEquals(self.mails[0]['To'], 'abc@def.com')
        self.assertEquals(self.mails[0]['Subject'], 'Test Subject')
        self.assertTrue(self.mails[0].get_payload(0).as_string().find('This is a test mail') != -1)


    def test_if_default_config_is_used(self):
        pype_ctx = {'to_addrs':'abc@def.com', 'subject':'Test Subject'}
        pype_val = {'body':'This is a test mail', 'files':[]}

        TestMail.MAILSENDER_PYPE.send(Drop(pype_ctx,pype_val))
        self.__read_mail__()
        self.assertEquals(self.mails[0].get_payload(0)['Content-Type'], 'text/plain; charset="us-ascii"')
        self.assertEquals(self.mails[0]['From'], 'xyz@def.com')
        self.assertEquals(self.mails[0].get('Reply-To', None), None)


    def test_if_overridden_config_is_used(self):
        pype_ctx = {'to_addrs':'abc@def.com', 'subject':'Test Subject'}
        pype_val = {'body':'This is a test mail', 'files':[], 'body_mimetype':'html', 'reply_to':'somename@replyto.com', 'from_addr': 'Some Body <override@overridden.com>'}

        TestMail.MAILSENDER_PYPE.send(Drop(pype_ctx,pype_val))
        self.__read_mail__()
        self.assertEquals(self.mails[0].get_payload(0)['Content-Type'], 'text/html; charset="us-ascii"')
        self.assertEquals(self.mails[0]['From'], 'Some Body <override@overridden.com>')
        self.assertEquals(self.mails[0].get('Reply-To', None), 'somename@replyto.com')
