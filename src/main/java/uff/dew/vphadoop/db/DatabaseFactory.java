package uff.dew.vphadoop.db;

import org.apache.hadoop.conf.Configuration;

import uff.dew.vphadoop.VPConst;

public class DatabaseFactory {
    
    private static Database databaseInstance;
    
    private DatabaseFactory() {
    }

    public static void produceSingletonDatabaseObject(Configuration conf) throws Exception {
        String type = conf.get(VPConst.DB_CONF_TYPE);
        if (type == null) {
            throw new Exception("SGBDX type not specified in job configuration file");
        }
        
        String hostname = conf.get(VPConst.DB_CONF_HOST);
        if (hostname == null) {
            throw new Exception("SGBDX host not specified in job configuration file");
        }
        
        String portstr = conf.get(VPConst.DB_CONF_PORT);
        if (portstr == null) {
            throw new Exception("SGBDX port not specified in job configuration file");
        }
        int port = -1;
        try {
            port = Integer.parseInt(portstr);
        }
        catch (NumberFormatException e) {
            throw new Exception("SGBDX port specified is not a number");
        }
        
        String username = conf.get(VPConst.DB_CONF_USERNAME);
        if (username == null) {
            throw new Exception("SGBDX username not specified in job configuration file");
        }

        String password = conf.get(VPConst.DB_CONF_PASSWORD);
        if (password == null) {
            throw new Exception("SGBDX password not specified in job configuration file");
        }

        String database = conf.get(VPConst.DB_CONF_DATABASE);
        if (database == null) {
            throw new Exception("SGBDX database not specified in job configuration file");
        }

        if (type.equals(VPConst.DB_TYPE_BASEX)) {
            databaseInstance = new BaseXDatabase(hostname, port, username, password, database);
        }
        else if (type.equals(VPConst.DB_TYPE_SEDNA)) {
            databaseInstance = new SednaDatabase(hostname, port, username, password, database);
        }
        else {
            throw new Exception("Database type not recognized. Must be either SEDNA or BASEX.");
        }
    }
    
    public static Database getSingletonDatabaseObject() {
    	return databaseInstance;
    }
}
