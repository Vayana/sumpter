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

from types import GeneratorType, FunctionType
import StringIO
import hashlib
import yaml
          
class PypeError(Exception):
    def __init__(self,key,*args):
        self.key = key
        self.args = args
    def __eq__(self,other):
        return self.key == other.key and self.args == other.args

class PypeRuntimeError(PypeError):
    pass

class PypeConfigurationError(PypeError):
    pass

def ctx_ref(param):
    def ctx_extract(seg,ctx,val):
        return ctx[param]
    return ctx_extract

def drop_dict_ref(param):
    def drop_dict_extract(seg,ctx,val):
        return val[param]
    return drop_dict_extract

def drop_attr_ref(param):
    def drop_attr_extract(seg,ctx,val):
        return val.__dict__[param]
    return drop_attr_extract


    
class Drop(object):
    """
    A drop is an encapsulated piece of data flowing through the pipeline.
    """
    
    id_ctr = 0
    
    @staticmethod
    def get_next_id():
        val= Drop.id_ctr
        Drop.id_ctr = Drop.id_ctr + 1
        return val
    
    def __init__(self,ctx,val,**kw):
        """ Initialiser for drop. """
        
        # Get a unique (runtime) identifier for the drop
        self.id = self.get_next_id()
        
        # The root id is the id to group together the drop and all its
        # children drops
        self.root_id = id
        
        # Enable tracing forces much more substantial runtime data collection
        # Primarily should be used for debugging and diagnostics
        self.enable_trace = kw.get('pp_enable_trace',False)

        # Initialise Context
        if not ctx : ctx = {}
        self.ctx = ctx
        
        # Initialise value
        self.val = val
        
        # trace is a tracking variable to indicate what 
        # segments this drop passed through
 
        self.trace = []
        
        # parents is an attribute indicating the drops which
        # at some time transferred control / data into this 
        # drop
        
        self.parents = []
        
        # children is an attribute listing the children drops
        # into which this drop transfers control to
        
        self.children = []
        
        # alive is a variable that gets set to False once
        # this drop has completed all its necessary activities
        # (by transferring work to another drop or having reached
        # the end of the pipeline. 
        self.alive = True
                    
    def record(self,seg):
        """ record that this drop was processed by the given seg """
        if self.alive :
            self.trace.append(seg)
        else :
            raise PypeRuntimeError('inactive-drop-operation-record',self,seg)
    
    def create_child(self,val):
        if self.alive :
            new_drop = Drop(self.ctx,val)
            new_drop.root_id = self.root_id
            new_drop.parents.append(self)
            self.children.append(new_drop)
            return new_drop
        else :
            raise PypeRuntimeError('inactive-drop-operation-create-child',self)
    
    def kill(self):
        if self.alive :
            self.alive = False
        else :
            raise PypeRuntimeError('inactive-drop-operation-kill',self)
    
    def get_trace(self):
        return '=>'.join(str(seg) for seg in self.trace)

class Pype(object):
    """
    Pype is a construction helper which helps building pipelines. It primarily is meant to assemble pipelines using
    the or operator. Since the or operator semantics cannot be played around with, the only way I could figure out to
    allow a fluent interface even while continuing to use the or operator (since it is quite intuititive) was to create
    an intermediate class primarily used during pipeline construction stage.
    """
    def __init__(self,head):
        """ Initialise the Pype with the first segment """
        self.segs = [head]
        self.head = head
        self.tail = head
    def set_next(self,next):
        """ Set the next segment to be transferred control to after the end of this pipe """
        if isinstance(next,Pype) :
            self.tail.set_next(next.head)
        elif isinstance(next,Segment) :
            self.tail.set_next(next)
    def __or__(self,next):
        """ append the next segment, pype, tuple of segments/pypes (a fork) at the end of this pype """
        if isinstance(self.tail,Segment) :
            if isinstance(next,Pype) :
                self.segs.extend(next.segs)
                self.tail.set_next(next.head)
                self.tail = next.tail
            elif isinstance(next,Segment) :
                self.tail.set_next(next)
                self.segs.append(next)
                self.tail = next
            elif isinstance(next,tuple) :
                new_next = ()
                for val in next :
                    if isinstance(val,Pype) :
                        new_next = new_next + (tuple(seg for seg in val.segs),)
                    elif isinstance(val,Segment) :
                        new_next = new_next + (val,)
                self.segs.append(new_next)
                for seg in next :
                    if isinstance(seg,Pype) :
                        self.tail.set_next(seg.head)
                    elif isinstance(seg,Segment) :
                        self.tail.set_next(seg)
                self.tail = new_next
        elif isinstance(self.tail,tuple) :
            if isinstance(next,Pype) :
                for segp in self.tail :
                    segp.set_next(next.head)
                self.segs.extend(next.segs)
                self.tail = next.tail
            elif isinstance(next,Segment) :
                for segp in self.tail :
                    if isinstance(segp,Segment) :
                        segp.set_next(next)
                    elif isinstance(segp,tuple) :
                        segp[-1].set_next(next)
                self.segs.append(next)
                self.tail = next
            elif isinstance(next,tuple) :
                self.segs.append(next)
                for segp in self.tail :
                    for segn in next :
                        if isinstance(segn,Pype) :
                            segp.set_next(segn.head)
                        elif isinstance(segn,Segment) :
                            segp.set_next(segn)
                self.tail = next
        
        return self
            
class Segment(object):
    """ 
    A segment represent an element in a pipeline. A pipeline is constructed 
    by joining various Segments together
    """
    
    def __init__(self,name,func = None, **kw):
        self.name = name
        self.__dict__.update(kw)
        self.next = []
        self.prev = []
        self.func = func
    
    def set_next(self,next):
        self.next.append(next)
        next.prev.append(self)
        
    def __or__(self,next):
        return Pype(self) | next
    
    def get_param_val(self,param,ctx,val):
        if isinstance(param,basestring) :
            ref = self.__dict__.get(param,None)
            if isinstance(ref,FunctionType) :
                return ref(self,ctx,val)
            else :
                return ref
        elif isinstance(param,FunctionType) :
            return param(self,ctx,val)

    def send(self,drop):
        drop.record(self)
        if self.func != None:
            val = self.func(drop.ctx,drop.val)
        else :
            val = self.perform(drop.ctx,drop.val)
        
        if not val and val != drop.val :
            # drop was consumed. eg by a batcher. do nothing
            pass    
        elif isinstance(val,(GeneratorType,list,tuple)) :
            for child_val in val :
                for nxt in self.next :
                    child_drop = drop.create_child(child_val)
                    nxt.send(child_drop)
        else :
            drop.val = val
            for nxt in self.next :
                nxt.send(drop)
 
        return drop
    
    def __str__(self):
        return '%s(%s)' % (type(self).__name__, self.name)
    
    def __repr__(self):
        return '%s(%s)' % (type(self).__name__, self.name)
    
    def perform(self,ctx,val):
        print "%s:%s" % (self.name,val)
        return val

class DropSegment(Segment):
    def send(self,drop):
        drop.record(self)
        if self.func != None:
            new_drop = self.func(drop)
        else :
            new_drop = self.perform(drop)
        
        if not new_drop :
            # drop was consumed. eg by a batcher. do nothing
            pass    
        elif isinstance(new_drop,(GeneratorType,list,tuple)) :
            for child_drop in new_drop :
                for nxt in self.next :
                    nxt.send(child_drop)
        else :
            for nxt in self.next :
                nxt.send(new_drop)
 
        return drop


class Collector(Segment):
    def __init__(self,name,**kw):
        super(Collector,self).__init__(name,**kw)
        self.vals = []
        
    def perform(self,ctx,val):
        self.vals.append(val)
        return val
        
        
def root_id_of_drop(drop):
    return drop.root_id

def collate_vals(seg_val_map):
    return tuple(drop.val for drop in seg_val_map.values())

class Join(DropSegment):
    def __init__(self,name,id_func = None,val_func = None, **kw):
        super(Join,self).__init__(name,**kw)
        self.id_map = {}
        if id_func is None :
            self.id_func = root_id_of_drop
        else :
            self.id_func = id_func
        
        if val_func is None :
            self.val_func = collate_vals
        else :
            self.val_func = val_func
        
    def perform(self,drop):
        id = self.id_func(drop)
        arrived = self.id_map.get(id, None)
        if not arrived :
            arrived = {}
            self.id_map[id] = arrived
        key = drop.trace[-2]
        arrived[key] = drop
        if len(arrived) == len(self.prev) :
            str = self.val_func(arrived)
            newdrp = drop.create_child(str)

            for nxt in self.next :
                nxt.send(newdrp)
                
class Config(object):
    def __init__(self,seg,ctx,val,*args):
        for arg in args :
            self.__dict__[arg] = seg.get_param_val(arg,ctx,val)
            
from collections import defaultdict
class Node(object):
    def __init__(self,name,indexed = True):
        self.name = name
        self.children = []
        self.attrs = {}
        self.indexed = indexed
        if self.indexed :
            self.index = defaultdict(list)
    def add(self,child):
        self.children.append(child)
        if self.indexed :      
            self.index[child.name] = child
    def set(self,attr_name,attr_val):
        self.attrs[attr_name] = attr_val
    def get(self,attr_name):
        return self.attrs[attr_name]
    def attribute_names(self):
        return self.attrs.iterkeys()
    def children(self,name = None):
        if not name :
            return self.children.__iter__()
        elif self.indexed :
            return self.index[name].__iter__()
        else :
            raise 'Indexed access on an unindexed Node'
    def attribute_pairs(self):
        return self.attrs.iteritems()