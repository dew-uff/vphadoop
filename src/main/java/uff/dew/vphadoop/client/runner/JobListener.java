package uff.dew.vphadoop.client.runner;

public interface JobListener {

    public void mapProgressChanged(int value);
    
    public void reduceProgressChanged(int value);
    
    public void completed(boolean successful);
}
