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
from sumpter import Segment, Config, Generic
from xml.dom import minidom
from xml.sax import make_parser
from xml.sax.handler import ContentHandler
import cStringIO
import logging
import types
import xml.sax

log = logging.getLogger(__name__)      

class SimpleXmlComposer(Segment):
    def __init__(self,name,**kwargs):
        super(SimpleXmlComposer,self).__init__(name,**kwargs)
        self.xml_to_obj = {}
        self.obj_to_xml = {}
        
    def perform(self,ctx,val):
        config = Config(self,ctx,val,'content','root','generate_xml_string')
        doc = minidom.Document()
        self.compose(doc,doc,config.root, config.content)
        if config.generate_xml_string :
            strio = cStringIO.StringIO()
            doc.writexml(strio, "", "  ", "\n", "UTF-8")
            val['xml'] = strio.getvalue()
            strio.close()
        else :
            val['xml'] = doc
        return val
    
    def compose(self,doc,parent,elemname,content):
        element = doc.createElement(elemname)
        parent.appendChild(element)
        elements = False
        if hasattr(content,'_elements') : elements = content._elements 
        if isinstance(content,(types.ListType,types.TupleType)) :
            newname = elemname[:-1]
            for item in content : 
                self.handle_namevalue(doc, element, newname, item)
        elif isinstance(content,(types.DictType)) :
            for name,val in content.items() :
                self.handle_namevalue(doc, element, name, val, elements)
        elif hasattr(content,'__dict__') :
            for name,val in content.__dict__.items() :
                if not name.startswith('_') :
                    self.handle_namevalue(doc, element, name, val, elements)
        else :
            text = doc.createTextNode(str(content))
            element.appendChild(text)

    def handle_namevalue(self,doc,element,name,val, elements = False):
        if isinstance(val,(types.ListType,types.TupleType,types.DictType)) :
            self.compose(doc,element,name,val)
        elif hasattr(val,'__dict__') :
            self.compose(doc,element,name,val)
        else :
            if not elements :
                element.setAttribute(name,val)
            else :
                self.compose(doc,element,name,val)

class Element(object):
    def __init__(self,name,attrs, trim = True):
        self.name = name
        self.attrs = dict((str(name), attrs.getValue(name)) for name in attrs.getNames())
        self.content = []
        self.trim = trim
    def append_content(self,content):
        if self.trim : content = content.strip()
        if len(content) > 0 : self.content.append(content)
    def get_content(self):
        if self.trim :
            return " ".join(self.content)
        else :
            return "".join(self.content)
    def to_generic(self):
        return Generic(**self.attrs)
    def add_attr(self,name,val):
        self.attrs[str(name)] = val
    def __repr__(self):
        return 'Element[%s]' % self.name

class ElementList(object):
    def __init__(self,name,attrs, trim = True):
        self.name = name
        self.content = []
        self.attrs = []
        self.trim = trim
    def append_content(self,content):
        if self.trim : content = content.strip()
        if len(content) > 0 : self.content.append(content)
    def add_attr(self,name,val):
        self.attrs.append((name,val))
    def to_generic(self):
        return tuple((name, val.to_generic if isinstance(val,(Element,ElementList)) else val) for name,val in self.attrs)
        
class XmlHandler(xml.sax.handler.ContentHandler):
    def __init__(self,multitags):
        ContentHandler.__init__(self)
        self.current = None
        self.stack = []
        self.result = None
        self.multitags = multitags
                
    def startElement(self, name, attrs) :
        if self.current : self.stack.append(self.current)
        if name in self.multitags :
            self.current = ElementList(name,attrs)
        else :
            self.current = Element(name,attrs)
        
    def characters(self,content):
        self.current.append_content(content)
        
    def endElement(self,name):
        if self.current.name != name : raise Exception('Internal error')
        child = self.current
        if len(self.stack) > 0 :
            self.current = self.stack.pop()
        else :
            self.current = None
        
        if self.current :
            if len(child.attrs) > 0 :
                self.current.add_attr(name,child.to_generic())
            else :
                self.current.add_attr(name,child.get_content())
        else :
            if len(child.attrs) > 0 :
                self.result = child.to_generic()
            else :
                self.result = child.get_content()
            

        
class SimpleXmlParser(Segment) :
    def __init__(self,name,**kwargs):
        super(SimpleXmlParser,self).__init__(name,**kwargs)
        
    def perform(self,ctx,val):
        config = Config(self,ctx,val,'xml','multitags')
        if config.multitags :
            config.multitags = config.multitags.split(',')
        parser = make_parser()
        handler = XmlHandler(config.multitags)
        parser.setContentHandler(handler)
        parser.parse(cStringIO.StringIO(config.xml))
        val['model'] = handler.result
        return val

    