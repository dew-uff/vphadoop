package uff.dew.svp.engine;

public class XQueryEngineException extends Exception {

    private static final long serialVersionUID = -2608844990753530513L;
    
    public XQueryEngineException(Exception cause) {
        super("Something wrong in XQueryEngine.",cause);
    }
}
