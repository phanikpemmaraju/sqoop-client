package org.hmrc.cds.data;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;


public class PropertiesFileUtil {

    private static PropertiesConfiguration configuration = null;
    private static String profile = System.getProperty("profile");

    static
    {
        try{
            configuration = new PropertiesConfiguration(PropertiesFileUtil.class.getClassLoader().getResource("profiles/"+profile+"/connection.properties"));
        } catch (ConfigurationException ex) {
            ex.printStackTrace();
        }
    }

    public static synchronized String getProperty(final String key)
    {
        return (String)configuration.getProperty(key);
    }

}
