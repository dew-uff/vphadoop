package uff.dew.vphadoop.xquery;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class XPathExpression {
    
    private List<String> parts;
    private String document;
    
    public XPathExpression(String doc, String xpath) {
        document = doc;
        
        parts = new ArrayList<String>();
        
        StringTokenizer st = new StringTokenizer(xpath, "/");
        while (st.hasMoreTokens()) {
            parts.add(st.nextToken());
        }
    }
    
    public int getNumLevels() {
        return parts.size()-1;
    }
    
    public String getSubPath(int level) {
        return getSubPath(0,level,true);
    }
    
    public String getFullPath() {
        return getSubPath(parts.size()-1);
    }
    
    public String getPathWithSelection(String selection, int level) {
        StringBuilder result = new StringBuilder();
        result.append(getSubPath(level));
        result.append("["+ selection +"]");
        if (level < (parts.size()-1))
            result.append(getSubPath(level+1,(parts.size()-1),false));
        return result.toString();
    }
    
    private String getSubPath(int levelStart, int levelEnd, boolean includeDoc) {
        StringBuilder result = new StringBuilder();
        if (includeDoc) {
            result.append("doc('" + document + "')");
        }
        for (int i = levelStart; i <= levelEnd && i < parts.size(); i++) {
            result.append("/" + parts.get(i));
        }
        return result.toString();
    }
}
