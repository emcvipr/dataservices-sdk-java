package com.emc.vipr.services.s3;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.amazonaws.auth.BasicAWSCredentials;

/**
 * This class looks on the classpath for a file named viprs3.properties and uses it to
 * configure a connection to ViPR.  The supported properties are:
 * <dt>
 * <dl>vipr.access_key_id</dl><dd>(Required) The access key (user ID)</dd>
 * <dl>vipr.secret_key</dl><dd>(Required) The shared secret key</dd>
 * <dl>vipr.endpoint</dl><dd>(Required) The endpoint hostname or IP address of the ViPR
 * data service server to use</dd>
 * <dl>vipr.namespace</dl><dd>(Optional) The ViPR namespace to connect to.  Generally
 * this is required if your endpoint is an IP or not in the format of {namespace}.company.com</dd>
 * </dt>
 * @author cwikj
 *
 */
public class S3ClientFactory {
    public static final String VIPR_PROPERTIES_FILE = "/vipr.properties";
    
    public static final String PROP_ACCESS_KEY_ID = "vipr.access_key_id";
    public static final String PROP_SECRET_KEY = "vipr.secret_key";
    public static final String PROP_ENDPOINT = "vipr.endpoint";
    public static final String PROP_NAMESPACE = "vipr.namespace";
    
    public static Properties getProperties() throws FileNotFoundException, IOException {
        InputStream in = S3ClientFactory.class.getClassLoader().getResourceAsStream(VIPR_PROPERTIES_FILE);
        if(in == null) {
            throw new FileNotFoundException(VIPR_PROPERTIES_FILE);
        }
        
        Properties props = new Properties();
        props.load(in);
        in.close();
        
        return props;
    }
    
    public static ViPRS3Client getS3Client() {
        try {
            Properties props = getProperties();
            
            String accessKey = getPropertyNotEmpty(props, PROP_ACCESS_KEY_ID);
            String secretKey = getPropertyNotEmpty(props, PROP_SECRET_KEY);
            String endpoint = getPropertyNotEmpty(props, PROP_ENDPOINT);
            
            BasicAWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey);
            ViPRS3Client client = new ViPRS3Client(creds);
            client.setEndpoint(endpoint);
            
            String namespace = props.getProperty(PROP_NAMESPACE);
            if(namespace != null) {
               client.setNamespace(namespace);
            }
            
            return client;
        } catch (IOException e) {
            throw new RuntimeException("Could not load properties file", e);
        }
    }
    
    private static String getPropertyNotEmpty(Properties p, String key) {
        String value = p.getProperty(key);
        if(value == null || value.isEmpty()) {
            throw new RuntimeException(String.format("The property %s is required", key));
        }
        return value;
    }
}
