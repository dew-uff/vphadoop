package uff.dew.svp.db;


public class DatabaseFactory {
    
    private static Database databaseInstance;
    
    private DatabaseFactory() {
    }

//    public static void produceSingletonDatabaseObject(Properties prop) throws DatabaseException {
//        String type = prop.getProperty(Constants.DB_CONF_TYPE);
//        if (type == null) {
//            throw new DatabaseException("SGBDX type not specified in job configuration file");
//        }
//        
//        String hostname = prop.getProperty(Constants.DB_CONF_HOST);
//        if (hostname == null) {
//            throw new DatabaseException("SGBDX host not specified in job configuration file");
//        }
//        
//        String portstr = prop.getProperty(Constants.DB_CONF_PORT);
//        if (portstr == null) {
//            throw new DatabaseException("SGBDX port not specified in job configuration file");
//        }
//        int port = -1;
//        try {
//            port = Integer.parseInt(portstr);
//        }
//        catch (NumberFormatException e) {
//            throw new DatabaseException("SGBDX port specified is not a number");
//        }
//        
//        String username = prop.getProperty(Constants.DB_CONF_USERNAME);
//        if (username == null) {
//            throw new DatabaseException("SGBDX username not specified in job configuration file");
//        }
//
//        String password = prop.getProperty(Constants.DB_CONF_PASSWORD);
//        if (password == null) {
//            throw new DatabaseException("SGBDX password not specified in job configuration file");
//        }
//
//        String database = prop.getProperty(Constants.DB_CONF_DATABASE);
//        if (database == null) {
//            throw new DatabaseException("SGBDX database not specified in job configuration file");
//        }
//
//        if (type.equals(Constants.DB_TYPE_BASEX)) {
//            databaseInstance = new BaseXDatabase(hostname, port, username, password, database);
//        }
//        else if (type.equals(Constants.DB_TYPE_SEDNA)) {
//            databaseInstance = new SednaDatabase(hostname, port, username, password, database);
//        }
//        else {
//            throw new DatabaseException("Database type not recognized. Must be either SEDNA or BASEX.");
//        }
//    }
    
    public static void produceSingletonDatabaseObject(String hostname, int port, 
            String username, String password, String databaseName, String type) throws DatabaseException {
 
        if (type.equals(Constants.DB_TYPE_SEDNA)) {
            databaseInstance = new SednaDatabase(hostname, port, username, password, databaseName);            
        }
        else if (type.equals(Constants.DB_TYPE_BASEX)) {
            databaseInstance = new BaseXDatabase(hostname, port, username, password, databaseName);
        } else {
            throw new DatabaseException("Database type not recognized!");
        }
    }
    
    public static Database getSingletonDatabaseObject() {
    	return databaseInstance;
    }
}
