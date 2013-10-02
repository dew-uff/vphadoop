package uff.dew.vphadoop.client.runner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import uff.dew.vphadoop.client.JobException;

public abstract class BaseJobRunner implements JobRunner {
    
    private static final long JOB_STATUS_POLLING_PERIOD = 2000;

    protected String xquery;
    private int previousMapProgress;
    private int previousReduceProgress;
    
    private List<JobListener> listeners;
    private Thread notifierThread;
    
    public BaseJobRunner(String xquery) {
        this.xquery = xquery;
        this.listeners = new ArrayList<JobListener>();
    }
    
    public String getQuery() {
        return xquery;
    }
    
    public void runJob() throws JobException {
        
        try {
            prepare();
            
            doRunJob();
            
        } catch (Exception e) {
            if (notifierThread.isAlive()) {
                notifierThread.interrupt();
            }
        }

        
        startMonitoring();
    }
    
    public void addListener(JobListener jl) {
        if (!listeners.contains(jl)) {
            listeners.add(jl);
        }
    }
    
    public void removeListener(JobListener jl) {
        listeners.remove(jl);
    }
    
    private void startMonitoring() {
        
        previousMapProgress = 0;
        previousReduceProgress = 0;
        
        Runnable r = new Runnable() {
            
            @Override
            public void run() {

                while(!isFinished()) {
                    int currentMapProgress = getMapProgress();
                    if (currentMapProgress != previousMapProgress) {
                        previousMapProgress = currentMapProgress;
                        notifyMapProgressChanged(previousMapProgress);
                    }

                    int currentReduceProgress = getReduceProgress();
                    if (currentReduceProgress != previousReduceProgress) {
                        previousReduceProgress = currentReduceProgress;
                        notifyReduceProgressChanged(previousReduceProgress);
                    }
                    
                    try {
                        Thread.sleep(JOB_STATUS_POLLING_PERIOD);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                
                notifyJobCompleted(isSuccessFul());
            }
        };
        
        notifierThread = new Thread(r);
        notifierThread.start();
    }
    
    private void notifyMapProgressChanged(int value) {
        for (JobListener listener : listeners) {
            listener.mapProgressChanged(value);
        }
    }
    
    private void notifyReduceProgressChanged(int value) {
        for (JobListener listener : listeners) {
            listener.reduceProgressChanged(value);
        }
    }
    
    private void notifyJobCompleted(boolean successful) {
        for (JobListener listener : listeners) {
            listener.completed(successful);
        }
    }

    protected abstract void prepare() throws IOException;
    
    protected abstract void doRunJob() throws IOException, InterruptedException, ClassNotFoundException;
    
    protected abstract int getMapProgress();
    
    protected abstract int getReduceProgress();
    
    public abstract boolean isFinished();
    
    public abstract boolean isSuccessFul();
}
