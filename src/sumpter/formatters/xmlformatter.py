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
from sumpter import Segment, Config
from xml.dom import minidom
import cStringIO
import logging
import types

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

