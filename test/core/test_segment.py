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


class TestSegment(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def get_new_segment(self):
        return Segment('sample',{'param1': 'value1', 'param2' : 'value2'})

    def testEmptyConstruction(self):
        seg = Segment('dummy',param1 = 'value1', param2= 'value2')
        self.assertEquals(seg.name,'dummy')
        self.assertEquals(seg.param1,'value1')
        self.assertEquals(seg.param2,'value2')
        self.assertEquals(seg.next,[])
        self.assertEquals(seg.prev,[])

    def testSetNext(self):
        seg1 = Segment('sample1',param1 = 'value1', param2= 'value2')
        seg2 = Segment('sample2',param1 = 'value1', param2= 'value2')
        seg1.set_next(seg2)
        self.assertEqual(seg1.next,[seg2])
        self.assertEqual(seg2.prev,[seg1])

    def testGetParam(self):
        drop = Drop({'first': 1, 'second' : 2} ,{'fifth':5})
        seg = Segment('sample',second = 22, third= 3)
        self.assertEquals(seg.get_param_val(ctx_ref('first'),drop.ctx,drop.val),1)
        self.assertEquals(seg.get_param_val('second',drop.ctx,drop.val),22)
        self.assertEquals(seg.get_param_val(ctx_ref('second'),drop.ctx,drop.val),2)
        self.assertEquals(seg.get_param_val('third',drop.ctx,drop.val),3)
        self.assertEquals(seg.get_param_val('fourth',drop.ctx,drop.val),None)
        self.assertEquals(seg.get_param_val(drop_dict_ref('fifth'),drop.ctx,drop.val),5)

    def testTwoOrSegments(self):
        seg1 = Segment('sample1',param1 = 'value1', param2= 'value2')
        seg2 = Segment('sample2',param1 = 'value1', param2= 'value2')
        pype = seg1 | seg2
        self.assert_(isinstance(pype,Pype))
        self.assertEqual(pype.head,seg1)
        self.assertEqual(pype.tail,seg2)
        self.assertEqual(pype.segs,[seg1,seg2])
        self.assertEqual(seg1.next,[seg2])
        self.assertEqual(seg2.prev,[seg1])

    def testFourOrSegments(self):
        seg1 = Segment('sample1',param1 = 'value1', param2= 'value2')
        seg2 = Segment('sample2',param1 = 'value1', param2= 'value2')
        seg3 = Segment('sample3',param1 = 'value1', param2= 'value2')
        seg4 = Segment('sample4',param1 = 'value1', param2= 'value2')
        pype = seg1 | seg2 | seg3 | seg4
        self.assert_(isinstance(pype,Pype))
        self.assertEquals(pype.head,seg1)
        self.assertEquals(pype.tail,seg4)
        self.assertEquals(pype.segs,[seg1,seg2,seg3,seg4])
        self.assertEqual(seg1.next,[seg2])
        self.assertEqual(seg2.prev,[seg1])
        self.assertEqual(seg2.next,[seg3])
        self.assertEqual(seg3.prev,[seg2])
        self.assertEqual(seg3.next,[seg4])
        self.assertEqual(seg4.prev,[seg3])

    def testForkConfig(self):
        seg1 = Segment('sample1',param1 = 'value1', param2= 'value2')
        seg2 = Segment('sample2',param1 = 'value1', param2= 'value2')
        seg3 = Segment('sample3',param1 = 'value1', param2= 'value2')
        pype = seg1 | (seg2,seg3)
        self.assert_(isinstance(pype,Pype))
        self.assertEquals(pype.head,seg1)
        self.assertEquals(pype.tail,(seg2,seg3))
        self.assertEquals(pype.segs,[seg1,(seg2,seg3)])
        self.assertEquals(seg1.prev,[])
        self.assertEquals(seg1.next,[seg2,seg3])
        self.assertEquals(seg2.prev,[seg1])
        self.assertEquals(seg2.next,[])
        self.assertEquals(seg3.prev,[seg1])
        self.assertEquals(seg3.next,[])

    def testForkJoinConfig(self):
        seg1 = Segment('sample1',param1 = 'value1', param2= 'value2')
        seg2 = Segment('sample2',param1 = 'value1', param2= 'value2')
        seg3 = Segment('sample3',param1 = 'value1', param2= 'value2')
        seg4 = Segment('sample4',param1 = 'value1', param2= 'value2')
        pype = seg1 | (seg2,seg3) | seg4
        self.assert_(isinstance(pype,Pype))
        self.assertEquals(pype.head,seg1)
        self.assertEquals(pype.tail,seg4)
        self.assertEquals(pype.segs,[seg1,(seg2,seg3),seg4])
        self.assertEquals(seg1.prev,[])
        self.assertEquals(seg1.next,[seg2,seg3])
        self.assertEquals(seg2.prev,[seg1])
        self.assertEquals(seg2.next,[seg4])
        self.assertEquals(seg3.prev,[seg1])
        self.assertEquals(seg3.next,[seg4])
        self.assertEquals(seg4.prev,[seg2,seg3])
        self.assertEquals(seg4.next,[])

    def testForkJoinConfigWithMerge(self):
        seg1 = Segment('sample1',param1 = 'value1', param2 = 'value2')
        seg2 = Segment('sample2',param1 = 'value1', param2 = 'value2')
        seg3 = Segment('sample3',param1 = 'value1', param2 = 'value2')
        seg4 = Segment('sample4',param1 = 'value1', param2 = 'value2')
        join = Join('join1',hello = 'world')
        pype = seg1 | (seg2,seg3) | join | seg4
        self.assert_(isinstance(pype,Pype))
        self.assertEquals(pype.head,seg1)
        self.assertEquals(pype.tail,seg4)
        self.assertEquals(pype.segs,[seg1,(seg2,seg3),join,seg4])
        drop = Drop(None,'hello')
        drop = seg1.send(drop)
        self.assertEquals(drop.trace,[seg1,seg2,join,seg3,join])
        self.assertEquals(len(drop.children),1)
        child_drop = drop.children[0]
        self.assertEquals(child_drop.trace,[seg4])
        self.assertEquals(len(child_drop.parents),1)
        self.assert_(isinstance(child_drop,Drop))
        self.assertEquals(child_drop.parents[0],drop)

    def testForkJoinConfigAcrossPypes(self):
        seg1 = Segment('sample1',param1 = 'value1', param2= 'value2')
        seg2 = Segment('sample2',param1 = 'value1', param2= 'value2')
        seg3 = Segment('sample3',param1 = 'value1', param2= 'value2')
        seg4 = Segment('sample4',param1 = 'value1', param2= 'value2')
        seg5 = Segment('sample5',param1 = 'value1', param2= 'value2')
        seg6 = Segment('sample6',param1 = 'value1', param2= 'value2')
        seg7 = Segment('sample7',param1 = 'value1', param2= 'value2')
        seg8 = Segment('sample8',param1 = 'value1', param2= 'value2')
        pype = seg1 | seg2 | (seg3 | seg4, seg5 | seg6) | seg7 | seg8
        self.assert_(isinstance(pype,Pype))
        self.assertEquals(pype.head,seg1)
        self.assertEquals(pype.tail,seg8)
        self.assertEquals(pype.segs,[seg1,seg2,((seg3,seg4),(seg5,seg6)),seg7,seg8])
        self.assertEquals(seg1.prev,[])
        self.assertEquals(seg1.next,[seg2])
        self.assertEquals(seg2.prev,[seg1])
        self.assertEquals(seg2.next,[seg3,seg5])
        self.assertEquals(seg3.prev,[seg2])
        self.assertEquals(seg3.next,[seg4])
        self.assertEquals(seg4.prev,[seg3])
        self.assertEquals(seg4.next,[seg7])
        self.assertEquals(seg5.prev,[seg2])
        self.assertEquals(seg5.next,[seg6])
        self.assertEquals(seg6.prev,[seg5])
        self.assertEquals(seg6.next,[seg7])
        self.assertEquals(seg7.prev,[seg4,seg6])
        self.assertEquals(seg7.next,[seg8])
        self.assertEquals(seg8.prev,[seg7])
        self.assertEquals(seg8.next,[])
        drop = Drop(None,None)
        drop = seg1.send(drop)
        self.assertEquals(drop.trace,[seg1,seg2,seg3,seg4,seg7,seg8,seg5,seg6,seg7,seg8])

    def testSimpleSeg(self):
        def reverse_str(ctx,val):
            return val[::-1]

        drop = Drop({'first': 1, 'second' : 2} ,'hello world!')
        seg = Segment('sample1', reverse_str, param1 = 'value1', param2= 'value2')
        self.assert_(seg not in drop.trace)
        new_drop = seg.send(drop)
        self.assertEquals(new_drop.val,'!dlrow olleh')
        self.assert_(seg in new_drop.trace)
        self.assertEquals(drop,new_drop)


    def testConsumingFunc(self):
        def consumer(ctx,val):
            # I just consumed your value.
            return None

        drop = Drop({'first': 1, 'second' : 2} ,'hello world!')
        seg = Segment('sample1', consumer, param1 = 'value1', param2= 'value2')
        self.assert_(seg not in drop.trace)
        new_drop = seg.send(drop)
        self.assert_(seg in new_drop.trace)
        self.assertEquals(drop,new_drop)

    def testSplittingFunc(self):
        track = []
        def splitter(ctx,val):
            for subval in val :
                yield subval
        def tracker(ctx,val):
            track.append(val)
            return val
        def exception_raiser(ctx,val):
            raise Exception('foo')
        drop = Drop({'first': 1, 'second' : 2} ,'hello')

        # simple splitter

        seg1 = Segment('sample1', splitter, param1 = 'value1', param2= 'value2')
        seg2 = Segment('sample2', splitter, param1 = 'value1', param2= 'value2')
        seg1 | seg2
        self.assert_(seg1 not in drop.trace)
        new_drop = seg1.send(drop)
        self.assert_(seg1 in new_drop.trace)
        self.assertEquals(drop,new_drop)
        self.assertEquals(len(new_drop.children),5)
        for child in drop.children :
            self.assert_(type(child),Drop)
        self.assertEquals('hello',"".join(child.val for child in drop.children))

    def testSplittingFuncWithListWithException(self):
        track = []
        def splitter(ctx,val):
            return tuple(char for char in val)
        def exception_raiser(ctx,val):
            if val == 'l' : raise Exception('foo')
            else : return val
        def tracker(ctx,val):
            track.append(val)
            return val
        drop = Drop({'first': 1, 'second' : 2} ,'hello')

        # simple splitter

        seg1 = Segment('sample1', splitter, param1 = 'value1', param2= 'value2')
        seg2 = Segment('sample2', exception_raiser, param1 = 'value1', param2= 'value2')
        seg1 | seg2
        self.assert_(seg1 not in drop.trace)
        try :
            new_drop = seg1.send(drop)
        except PypeRuntimeError as e:
            print e
            inner_exception = e.args[0]
            seg2_in_exception = e.args[1]
            self.assertEquals(inner_exception.args[0],'foo')
            self.assertEquals(seg2_in_exception,seg2)
        else :
            self.fail('Expected Exception Not received')

        for child in drop.children :
            self.assert_(type(child),Drop)
        self.assertEquals('hel',"".join(child.val for child in drop.children))

    def testSplittingFuncWithListWithIgnoreException(self):
        track = []
        def splitter(ctx,val):
            return tuple(char for char in val)
        def exception_raiser(ctx,val):
            if val == 'l' : raise Exception('foo')
            else : return val
        def tracker(ctx,val):
            track.append(val)
            return val
        drop = Drop({'first': 1, 'second' : 2} ,'hello')

        # simple splitter

        seg1 = Segment('sample1', splitter, param1 = 'value1', param2= 'value2',ignore_child_exceptions = True)
        seg2 = Segment('sample2', exception_raiser, param1 = 'value1', param2= 'value2')
        seg1 | seg2
        self.assert_(seg1 not in drop.trace)
        new_drop = seg1.send(drop)

        self.assert_(seg1 in new_drop.trace)
        self.assertEquals(drop,new_drop)
        self.assertEquals(len(new_drop.children),5)
        for child in drop.children :
            self.assert_(type(child),Drop)
            if child.aborted:
                self.assertEquals(child.aborted_on_exception.args[0].args[0],'foo')
                self.assertEquals(child.aborted_on_exception.args[1],seg2)
        self.assertEquals('hello',"".join(child.val for child in drop.children))

    def testSequence(self):
        track = []
        def tracker(drop):
            track.append(drop)
            return drop
        drop = Drop({'first': 1, 'second' : 2} ,'hello')
        seg1 = DropSegment('sample1', tracker, param1 = 'value1', param2= 'value2')
        seg2 = DropSegment('sample1', tracker, param1 = 'value1', param2= 'value2')
        seg1 | seg2
        self.assert_(seg1 not in drop.trace)
        self.assert_(seg2 not in drop.trace)
        new_drop = seg1.send(drop)
        self.assert_(seg1 in new_drop.trace)
        self.assert_(seg2 in new_drop.trace)
        self.assertEquals(drop,new_drop)
        self.assertEquals(track,[drop,drop])
