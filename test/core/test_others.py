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
from sumpter import *
import unittest


class TestUtilityFunctions(unittest.TestCase):
    def test_drop_dict_ref(self):
        val = {'foo':'bar'}
        x = drop_dict_ref('foo')(None, {}, val)
        self.assertEquals(x, 'bar')
        val = {}
        x = drop_dict_ref('foo', default='baz')(None, {}, val)
        self.assertEquals(x, 'baz')
        x = drop_dict_ref('foo', default='baz', optional=True)(None, {}, val)
        self.assertEquals(x, 'baz')
        x = drop_dict_ref('foo', optional=True)(None, {}, val)
        self.assertEquals(x, None)
        x = drop_dict_ref('foo')
        self.assertRaises(KeyError, x, None, {}, val)

    def test_ctx_ref(self):
        ctx = {'foo':'bar'}
        x = ctx_ref('foo')(None,ctx, {})
        self.assertEquals(x, 'bar')
        ctx = {}
        x = ctx_ref('foo', default='baz')(None, ctx, {})
        self.assertEquals(x, 'baz')
        x = ctx_ref('foo', optional=True)(None,ctx, {})
        self.assertEquals(x, None)
        x = ctx_ref('foo', default='baz', optional=True)(None, ctx, {})
        self.assertEquals(x, 'baz')
        x = ctx_ref('foo')
        self.assertRaises(KeyError, x, None, ctx, {})
