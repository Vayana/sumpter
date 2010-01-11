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

from sumpter import Drop, drop_dict_ref
from sumpter.formatters.jsonformatter import JsonParser
import unittest
class TestJson(unittest.TestCase):
    def setUp(self):
         pass
    
    def tearDown(self):
        pass
    
    def testSimpleParse(self):
        val = {}
        val['jsonstring'] = '["foo", {"bar":["baz", null, 1.0, 2]}]'
        drop = Drop({},val)
        seg = JsonParser('jsonparser',jsonstring=drop_dict_ref('jsonstring'))
        result = seg.send(drop)
        print '==============>',result
