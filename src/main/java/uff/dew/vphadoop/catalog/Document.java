package uff.dew.vphadoop.catalog;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

public class Document {
    
    private String name = null;
    
    private Map<String, Element> elementsByPathMap;
    private Map<Integer,Element> elementsByIdMap;
    
    public Document() {
        elementsByPathMap = new HashMap<String, Element>();
        elementsByIdMap = new HashMap<Integer, Element>();
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }

    public void readFromRawFile(File file) throws Exception {
        
        try(FileInputStream is = new FileInputStream(file)) {
            
            name = file.getName();
            
            XMLInputFactory factory = XMLInputFactory.newInstance();
            XMLStreamReader stream = factory.createXMLStreamReader(is);
            
            String currentPath = "";
            
            while (stream.hasNext()) {
                int type = stream.next();
                
                switch (type) {
                case XMLStreamReader.START_ELEMENT:
                    
                    String elmName = stream.getLocalName();
                    String previousPath = currentPath;
                    currentPath +=  "/" + elmName;
                    
                    Element element = elementsByPathMap.get(currentPath);
                    
                    if (element == null) {
                        element = new Element(elmName);
                        element.setPath(currentPath);
                        if (!previousPath.equals("")) {
                            Element parent = elementsByPathMap.get(previousPath);
                            element.setParent(parent);
                        }
                        elementsByPathMap.put(currentPath, element);
                        elementsByIdMap.put(new Integer(element.getId()), element);
                    }
                    element.setCount(element.getCount() + 1);

                    break;
                    
                case XMLStreamReader.END_ELEMENT:
                    // we finished processing this element, return current path 
                    currentPath = currentPath.substring(0, currentPath.lastIndexOf('/'));
                    break;
                }
            } 
        }
    }
    
    public void readFromCatalogStream(XMLStreamReader stream) throws Exception {
        name = stream.getAttributeValue(null, "name");
        
        while (stream.hasNext()) {
            int type = stream.next();
            if (type == XMLStreamReader.START_ELEMENT) {
                
                if (stream.getLocalName() == "element") {
                    String idString = stream.getAttributeValue(null, "id");
                    String name = stream.getAttributeValue(null, "name");
                    String countString = stream.getAttributeValue(null, "count");
                    String path = stream.getAttributeValue(null, "path");
                    String parentString = stream.getAttributeValue(null, "parent");
                    
                    if (idString != null && countString != null 
                            && name != null && path != null 
                            && parentString != null) {

                        int id = Integer.parseInt(idString);
                        int cardinality = Integer.parseInt(countString);
                        int parentId = Integer.parseInt(parentString);

                        Element element = new Element(id, name, cardinality, path);
                        element.setParentId(parentId);
                        elementsByIdMap.put(new Integer(id), element);
                        elementsByPathMap.put(path, element);
                    }    
                } else {
                    throw new Exception ("Should not have found anything but \"element\" elements here!");
                }
            } 
            else if (type == XMLStreamReader.END_ELEMENT) {
                if (stream.getLocalName() == "document") {
                    for (Element e : elementsByIdMap.values()) {
                        if (e.getParentId() != -1) {
                            e.setParent(elementsByIdMap.get(new Integer(e.getParentId())));
                        }
                    }
                    // document reading done. 
                    break;
                }
            }
        }
    }
    
    public String getAsXml() {
        
        if (elementsByPathMap.isEmpty()) {
            System.err.println("Something wrong. Can't export document to xml since there are no elements in this document");
            return null;
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append("<document name=\"" + name + "\">\n");
        for (Element e: elementsByPathMap.values()) {
            sb.append("<element id=\""+e.getId() +"\" name=\""+e.getName()+
                    "\" count=\""+e.getCount()+"\" path=\""+e.getPath()+"\" parent=\""+
                    (e.getParent()!=null?e.getParent().getId():-1)+"\"/>\n");
        }
        sb.append("</document>\n");
        return sb.toString();
    }

    public int getCardinality(String path) {

        int cardinality = 0;

        Element e = elementsByPathMap.get(path);
        if (e != null) {
            cardinality = e.getCount();
        }

        return cardinality;
    }
}
