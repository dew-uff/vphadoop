package uff.dew.svp.catalog;

public class Element {
    
    private static int counter = 0;
    
    private String name;
    private int count;
    private Element parent;
    private String path;
    private int id = counter++;
    private int parentId;
    
    public Element(String name) {
        this.name = name;
        this.count = 0;
        this.parent = null;
        this.path = "";
        this.parentId = -1;
    }
    
    public Element(String name, int count) {
        this.name = name;
        this.count = count;
        this.parent = null;
        this.path = "";
        this.parentId = -1;
    }

    public Element() {
        this.name = "";
        this.count = 0;
        this.parent = null;
        this.path = "";
        this.parentId = -1;
    }
    
    public Element(int id, String name, int count, String path) {
    	this.id = id;
    	this.name = name;
    	this.count = count;
    	this.path = path;
    	this.parent = null;
    	this.parentId = -1;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void setParent(Element parent) {
        this.parent = parent;
        if (parent != null){
        	this.parentId = parent.getId();
        } else {
        	this.parentId = -1;
        }
    }

    public Element getParent() {
        return parent;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;        
    }
    
    public void setId(int id) {
        this.id = id;
    }
    
    public int getId() {
        return id;
    }

    public void setParentId(int parentId) {
        this.parentId = parentId;
    }

    public int getParentId() {
        return parentId;
    }   
}
