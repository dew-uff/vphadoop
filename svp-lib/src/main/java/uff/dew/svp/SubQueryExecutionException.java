package uff.dew.svp;

public class SubQueryExecutionException extends Exception {

    /**
     * 
     */
    private static final long serialVersionUID = -669938251586993557L;

    public SubQueryExecutionException(Exception e) {
        super("Error during subquery execution",e);
    }
}
