package com.emc.vipr.sample;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Properties;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3EncryptionClient;
import com.amazonaws.services.s3.model.EncryptionMaterials;
import com.emc.vipr.services.s3.NamespaceRequestHandler;
import com.emc.vipr.services.s3.ViPRS3Client;

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
    public static final String VIPR_PROPERTIES_FILE = "vipr.properties";
    
    public static final String PROP_ACCESS_KEY_ID = "vipr.access_key_id";
    public static final String PROP_SECRET_KEY = "vipr.secret_key";
    public static final String PROP_ENDPOINT = "vipr.endpoint";
    public static final String PROP_NAMESPACE = "vipr.namespace";
    public static final String PROP_PUBLIC_KEY = "vipr.encryption.publickey";
    public static final String PROP_PRIVATE_KEY = "vipr.encryption.privatekey";
    public static final String PROP_PROXY_HOST = "vipr.proxy.host";
    public static final String PROP_PROXY_PORT = "vipr.proxy.port";
    
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
            checkProxyConfig(client, props);
            
            return client;
        } catch (IOException e) {
            throw new RuntimeException("Could not load properties file", e);
        }
    }
    
    /**
     * Creates an EncryptionClient for testing.  Loads the public and private keys from
     * the properties file (not suitable for production).
     * @return
     * @throws IOException
     */
    public static AmazonS3EncryptionClient getEncryptionClient() throws IOException {
        Properties props = getProperties();
        
        String accessKey = getPropertyNotEmpty(props, PROP_ACCESS_KEY_ID);
        String secretKey = getPropertyNotEmpty(props, PROP_SECRET_KEY);
        String endpoint = getPropertyNotEmpty(props, PROP_ENDPOINT);
        String publicKey = getPropertyNotEmpty(props, PROP_PUBLIC_KEY);
        String privateKey = getPropertyNotEmpty(props, PROP_PRIVATE_KEY);
        
        byte[] pubKeyBytes = Base64.decodeBase64(publicKey.getBytes("US-ASCII"));
        byte[] privKeyBytes = Base64.decodeBase64(privateKey.getBytes("US-ASCII"));
        
        X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(pubKeyBytes);
        PKCS8EncodedKeySpec privKeySpec = new PKCS8EncodedKeySpec(privKeyBytes);
        
        PublicKey pubKey;
        PrivateKey privKey;
        try {
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            pubKey = keyFactory.generatePublic(pubKeySpec);
            privKey = keyFactory.generatePrivate(privKeySpec);
        } catch (GeneralSecurityException e) {
            throw new RuntimeException("Could not load key pair: " + e, e);
        }
        
        EncryptionMaterials keys = new EncryptionMaterials(new KeyPair(pubKey, privKey));
        
        BasicAWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3EncryptionClient client = new AmazonS3EncryptionClient(creds, keys);
        client.setEndpoint(endpoint);
        
        String namespace = props.getProperty(PROP_NAMESPACE);
        if(namespace != null) {
           NamespaceRequestHandler handler = new NamespaceRequestHandler(namespace);
           client.addRequestHandler(handler);
        }
        
        checkProxyConfig(client, props);
        
        return client;
    }
    
    private static void checkProxyConfig(AmazonS3Client client, Properties props) {
        String proxyHost = props.getProperty(PROP_PROXY_HOST);
        if(proxyHost != null && !proxyHost.isEmpty()) {
            int proxyPort = Integer.parseInt(props.getProperty(PROP_PROXY_PORT));
            ClientConfiguration config = new ClientConfiguration();
            config.setProxyHost(proxyHost);
            config.setProxyPort(proxyPort);
            client.setConfiguration(config);
        }        
    }
    
    static String getPropertyNotEmpty(Properties p, String key) {
        String value = p.getProperty(key);
        if(value == null || value.isEmpty()) {
            throw new RuntimeException(String.format("The property %s is required", key));
        }
        return value;
    }
    
    // Generates a RSA key pair for testing.
    public static void main(String[] args) {
        try {
            KeyPairGenerator keyGenerator = KeyPairGenerator.getInstance("RSA");
            keyGenerator.initialize(1024, new SecureRandom());
            KeyPair myKeyPair = keyGenerator.generateKeyPair();
            
            // Serialize.
            byte[] pubKeyBytes = myKeyPair.getPublic().getEncoded();
            byte[] privKeyBytes = myKeyPair.getPrivate().getEncoded();
            
            String pubKeyStr = new String(Base64.encodeBase64(pubKeyBytes, false), "US-ASCII");
            String privKeyStr = new String(Base64.encodeBase64(privKeyBytes, false), "US-ASCII");
            
            System.out.println("Public Key: " + pubKeyStr);
            System.out.println("Private Key: " + privKeyStr);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
