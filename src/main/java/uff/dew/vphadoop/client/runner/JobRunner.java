package uff.dew.vphadoop.client.runner;

import uff.dew.vphadoop.client.JobException;

public interface JobRunner {
    
    public static final int HADOOP = 1;
    public static final int LOCAL = 2;
    
    public void runJob() throws JobException;
    
    public int getType();
    
    public String getResult();
}