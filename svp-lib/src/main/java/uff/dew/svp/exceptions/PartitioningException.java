package uff.dew.svp.exceptions;

public class PartitioningException extends Exception {

    private static final long serialVersionUID = 7843155821418990756L;

    public PartitioningException(Exception cause) {
        super("Error during partitioning execution", cause);
        
    }
    
    public PartitioningException(String message) {
        super(message);
    }
}
