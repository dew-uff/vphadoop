package uff.dew.svp.db;

public class DatabaseException extends Exception {

    /**
     * 
     */
    private static final long serialVersionUID = 7910839845564333238L;
    
    /**
     * Constructor
     * 
     * @param message
     */
    public DatabaseException(String message) {
        super(message);
    }
}
