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

from sumpter import Collector, Drop, ctx_ref, drop_dict_ref
from sumpter.communication.http import HttpReader
import BaseHTTPServer
import thread
import unittest

class TestHttp(unittest.TestCase):
    server_on = False
    class MyServer(BaseHTTPServer.BaseHTTPRequestHandler):
        def do_GET(self) :
            if self.path == '/hello' :
                self.send_response(200)
                self.send_header('Content-type','text/plain')
                self.end_headers()
                self.wfile.write('world')
            else :
                raise Exception('unknown-path %s' % self.path)
        
    def setUp(self):
        if TestHttp.server_on == False:
            server_class=BaseHTTPServer.HTTPServer
            handler_class=TestHttp.MyServer
            server_address = ('', 8001)
            httpd = server_class(server_address, handler_class)
            thread.start_new_thread(httpd.serve_forever,())
            TestHttp.server_on = True

    
    def tearDown(self):
        pass
    
    def testSimpleHttpGet(self):
        coll = Collector('collector')
        pype = HttpReader('google-reader',url='http://localhost:8001/hello') | coll 
        drop = Drop(None,{})
        pype.head.send(drop)
        self.assertEquals(coll.vals,['world'])
 
    def testSimpleHttpGetWithUrlCtxRef(self):
        coll = Collector('collector')
        pype = HttpReader('google-reader',url=ctx_ref('url')) | coll 
        drop = Drop({'url':'http://localhost:8001/hello'},{})
        pype.head.send(drop)
        self.assertEquals(coll.vals,['world'])
 
    def testSimpleHttpGetWithUrlValRef(self):
        coll = Collector('collector')
        pype = HttpReader('google-reader',url=drop_dict_ref('url')) | coll 
        drop = Drop({},{'url':'http://localhost:8001/hello'})
        pype.head.send(drop)
        self.assertEquals(coll.vals,['world'])
