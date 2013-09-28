package mediadorxml.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Properties;

public class Config {
	
	protected static Properties properties;
	
	protected Properties getProperties(){
		if (properties == null){
			properties = new Properties();
			try{
				//properties.load(new FileInputStream("config/partix.properties"));
				final URL url = this.getClass().getClassLoader().getResource("config/partix.properties");

				if (null == url) {
					//logger.warn("URL was null");
				}
				else{

			        String fname   = url.getFile();			        
			        //System.out.println(fname);
			        properties.load(new FileInputStream(URLDecoder.decode(fname, "UTF-8")));
				}			
			}
			catch (IOException e){
				// TODO Log
			}
		}
		return properties;
	}
	
	public static String getCatalogFile(){
		Config config = new Config();
		return config.getProperties().getProperty("CATALOGFILE", "catalog.xml");
	}
	
	public static FileInputStream getCatalogFileInputStream() throws FileNotFoundException, UnsupportedEncodingException{
		Config config = new Config();
		String catalogFile = config.getProperties().getProperty("CATALOGFILE", "catalog.xml").trim();
		final URL url = config.getClass().getClassLoader().getResource(catalogFile);
		if (null == url) {
			// TODO LOG
			//logger.warn("URL was null");
			return null;
		}
		else{

	        String fname   = url.getFile();			        
	        //System.out.println(fname);
	        return new FileInputStream(URLDecoder.decode(fname, "UTF-8"));
		}
	}
	
	public static int getCostEstimatorIOweight(){
		Config config = new Config();
		String w = config.getProperties().getProperty("COST_ESTIMATOR_IOWEIGHT", "1");
		try{
			return Integer.parseInt(w);
		}
		catch (Exception e){
			return 1;
		}
	}
}
