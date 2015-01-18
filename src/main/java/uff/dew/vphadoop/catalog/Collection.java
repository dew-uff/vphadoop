package uff.dew.vphadoop.catalog;

import java.io.File;
import java.io.FileFilter;
import java.util.HashMap;
import java.util.Map;

import javax.xml.stream.XMLStreamReader;

public class Collection {
    
    private String name;
    
    private Map<String,Document> documents;
    
    public Collection() {
        documents = new HashMap<String, Document>();
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }

    public void addDocument(Document d) throws Exception {
        
        if (documents.containsKey(d.getName())) {
            throw new Exception("One document with name \"" + d.getName() + "\" already exists in collection ");
        }
        
        documents.put(d.getName(), d);
    }
    
    public Document getDocument(String name) {
        return documents.get(name);
    }
    
    public void readFromRawDirectory(File directory) throws Exception {
        
        File[] innerFiles = directory.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                if (!file.isDirectory() && file.getName().endsWith(".xml")) {
                    return true;
                } 
                return false;
            }
        }); 
        
        name = directory.getName();
        
        for (File file : innerFiles) {
            
            try {
                Document d = new Document();
                d.readFromRawFile(file);
                documents.put(d.getName(), d);
            }
            catch (Exception e) {
                System.err.println("Something wrong during document reading: " + e.getMessage() +". Skipping file \"" + file.getAbsolutePath() + "\".");
            }
        }
    }

    public void readFromCatalogStream(XMLStreamReader stream) throws Exception {
        name = stream.getLocalName();
        
        while (stream.hasNext()) {
            int type = stream.next();
            
            if (type == XMLStreamReader.START_ELEMENT) {
                if (stream.getLocalName() == "document") {
                    Document doc = new Document();
                    doc.readFromCatalogStream(stream);
                    documents.put(doc.getName(), doc);
                }
            }
            else if (type == XMLStreamReader.END_ELEMENT) {
                if (stream.getLocalName() == "document") {
                    break;
                }
            }
        }
    }

    public String getAsXml() {
        StringBuilder sb = new StringBuilder();
        
        if (documents.isEmpty()) {
            sb.append("<collection name=\"" + name + "\" />\n");
        }
        else {
            sb.append("<collection name=\"" + name + "\">\n");
            for (Document document : documents.values()) {
                sb.append(document.getAsXml());
            }
            sb.append("</collection>\n");
        }
        
        return sb.toString();
    }
            
            

}
