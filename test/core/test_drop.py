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


class TestDrop(unittest.TestCase):
    def setUp(self):
        self.ctx = {1:'hello','world':'foo'}
        self.val = ['foo',('bar','baz'),{'boom':555}]

    def tearDown(self):
        pass

    def get_new_drop(self):
        return Drop(self.ctx,self.val)

    def testConstruction(self):
        drop = self.get_new_drop()
        self.assert_(isinstance(drop.id,(int,long)))
        self.assertEqual(drop.trace,[])
        self.assertEqual(drop.parents,[])
        self.assertEqual(drop.children,[])
        self.assertEqual(drop.alive,True)
        self.assertEqual(drop.ctx, self.ctx)
        self.assertEqual(drop.val, self.val)

    def testEmptyConstruction(self):
        drop = Drop(None,None)
        self.assertEqual(drop.ctx,{})
        self.assertEqual(drop.val,None)

    def testDifferentIds(self):
        drop1 = Drop(None,None)
        drop2 = Drop(None,None)
        self.assertNotEqual(drop1.id, drop2.id)

    def testChildCreation(self):
        drop_parent = self.get_new_drop()
        drop_child = drop_parent.create_child('hello')
        self.assertEqual(drop_child.ctx,self.ctx)
        self.assertEqual(drop_child.val,'hello')
        self.assertEqual(drop_parent.children,[drop_child])
        self.assertEqual(drop_child.parents,[drop_parent])

    def testTrace(self):
        drop = self.get_new_drop()
        drop.record('One')
        drop.record('Two')
        self.assertEqual(drop.trace,['One','Two'])
        self.assertEqual(drop.get_trace(),'One=>Two')

    def testKillDrop(self):
        drop = self.get_new_drop()
        drop.kill()
        self.assertEqual(drop.alive,False)

    def testRecordAKilledDrop(self):
        drop = self.get_new_drop()
        drop.kill()
        try :
            drop.record('foo')
            self.fail('Should have raised a runtime error on drop.record() for a killed drop')
        except Exception as e :
            self.assertEquals(e,PypeRuntimeError('inactive-drop-operation-record',drop,'foo'))

    def testCreateChildOnKilledDrop(self):
        drop = self.get_new_drop()
        drop.kill()
        try :
            drop.create_child('hello')
            self.fail('Should have raised a runtime error on drop.create_child() for a killed drop')
        except Exception as e :
            self.assertEquals(e,PypeRuntimeError('inactive-drop-operation-create-child',drop))

    def testKillAKilledDrop(self):
        drop = self.get_new_drop()
        drop.kill()
        try :
            drop.kill()
            self.fail('Should have raised a runtime error on drop.kill() for a killed drop')
        except Exception as e :
            self.assertEquals(e,PypeRuntimeError('inactive-drop-operation-kill',drop))

