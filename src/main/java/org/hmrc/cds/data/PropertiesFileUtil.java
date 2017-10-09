package org.hmrc.cds.data;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;


public class PropertiesFileUtil {

    private static PropertiesConfiguration configuration = null;

    static
    {
        try{
            configuration = new PropertiesConfiguration("connection.properties");
        } catch (ConfigurationException ex) {
            ex.printStackTrace();
        }
    }

    public static synchronized String getProperty(final String key)
    {
        return (String)configuration.getProperty(key);
    }

}
