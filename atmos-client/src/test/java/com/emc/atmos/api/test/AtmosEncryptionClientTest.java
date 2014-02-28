package com.emc.atmos.api.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.Provider;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.emc.atmos.api.AtmosConfig;
import com.emc.atmos.api.ObjectId;
import com.emc.atmos.api.ObjectIdentifier;
import com.emc.atmos.api.ObjectPath;
import com.emc.atmos.api.Range;
import com.emc.atmos.api.bean.CreateObjectResponse;
import com.emc.atmos.api.bean.Metadata;
import com.emc.atmos.api.bean.ReadObjectResponse;
import com.emc.atmos.api.encryption.AtmosEncryptionClient;
import com.emc.atmos.api.encryption.CompressionConfig;
import com.emc.atmos.api.encryption.EncryptionConfig;
import com.emc.atmos.api.jersey.AtmosApiClient;
import com.emc.atmos.api.request.CreateObjectRequest;
import com.emc.atmos.api.request.ReadObjectRequest;
import com.emc.atmos.api.request.UpdateObjectRequest;
import com.emc.atmos.util.AtmosClientFactory;
import com.emc.atmos.util.RandomInputStream;
import com.emc.vipr.transform.TransformConstants;
import com.emc.vipr.transform.TransformConstants.CompressionMode;
import com.emc.vipr.transform.encryption.DoesNotNeedRekeyException;
import com.emc.vipr.transform.encryption.KeyUtils;
import com.emc.vipr.transform.util.CountingInputStream;

public class AtmosEncryptionClientTest {
    private static final Logger logger = LoggerFactory.getLogger(
            AtmosEncryptionClientTest.class);

    private Properties keyprops;
    private KeyPair masterKey;
    private KeyPair oldKey;
    private KeyStore keystore;
    private String keystorePassword = "viprviprvipr";
    private String keyAlias = "masterkey";
    private String oldKeyAlias = "oldkey";
    private String keystoreFile = "keystore.jks";
    protected Provider provider;
    protected List<ObjectIdentifier> cleanup = Collections.synchronizedList( new ArrayList<ObjectIdentifier>() );

    private AtmosConfig config;

    private AtmosApiClient api;

    @Before
    public void setUp() throws Exception {
        // Load some keys for manual mode
        keyprops = new Properties();
        keyprops.load(this.getClass().getClassLoader()
                .getResourceAsStream("keys.properties"));

        masterKey = KeyUtils.rsaKeyPairFromBase64(
                keyprops.getProperty("masterkey.public"),
                keyprops.getProperty("masterkey.private"));
        logger.debug("Master key sizes: public: {} private: {}", 
                ((RSAPublicKey)masterKey.getPublic()).getModulus().bitLength(),
                ((RSAPrivateKey)masterKey.getPrivate()).getModulus().bitLength());
        oldKey = KeyUtils.rsaKeyPairFromBase64(
                keyprops.getProperty("oldkey.public"),
                keyprops.getProperty("oldkey.private"));
        logger.debug("Old key sizes: public: {} private: {}", 
                ((RSAPublicKey)oldKey.getPublic()).getModulus().bitLength(),
                ((RSAPrivateKey)oldKey.getPrivate()).getModulus().bitLength());
        
        // Init keystore for keystore mode
        keystore = KeyStore.getInstance("jks");
        InputStream in = this.getClass().getClassLoader().getResourceAsStream(keystoreFile);
        if(in == null) {
            throw new FileNotFoundException(keystoreFile);
        }
        keystore.load(in, keystorePassword.toCharArray());
        logger.debug("Keystore Loaded");
        for(Enumeration<String> aliases = keystore.aliases(); aliases.hasMoreElements();) {
            logger.debug("Found key: {}", aliases.nextElement());            
        }

        // Initialize the AtmosClient
        config = AtmosClientFactory.getAtmosConfig();
        Assume.assumeTrue("Could not load Atmos configuration", config != null);
        config.setDisableSslValidation( false );
        config.setEnableExpect100Continue( false );
        config.setEnableRetry( false );
        api = new AtmosApiClient( config );
    }
    
    @After
    public void tearDown() {
        for (final ObjectIdentifier cleanItem : cleanup) {
            try {
                api.delete(cleanItem);
            } catch (Throwable t) {
                logger.info("Failed to delete " + cleanItem + ": "
                        + t.getMessage());
            }
        }
    }    
    
    private AtmosEncryptionClient getBasicEncryptionClient() throws Exception {
        Set<KeyPair> decryptionKeys = new HashSet<KeyPair>();
        decryptionKeys.add(oldKey);
        EncryptionConfig ec = new EncryptionConfig(masterKey, decryptionKeys, provider, 128);
        AtmosEncryptionClient eclient = new AtmosEncryptionClient(api, ec, null);

        return eclient;
    }
    
    private AtmosEncryptionClient getBasicEncryptionClient(KeyPair masterKey) throws Exception {
        Set<KeyPair> decryptionKeys = new HashSet<KeyPair>();
        EncryptionConfig ec = new EncryptionConfig(masterKey, decryptionKeys, provider, 128);
        AtmosEncryptionClient eclient = new AtmosEncryptionClient(api, ec, null);

        return eclient;
    }
    
    private AtmosEncryptionClient getBasicEncryptionClientWithCompression() throws Exception {
        Set<KeyPair> decryptionKeys = new HashSet<KeyPair>();
        decryptionKeys.add(oldKey);
        EncryptionConfig ec = new EncryptionConfig(masterKey, decryptionKeys, provider, 128);
        CompressionConfig cc = new CompressionConfig(CompressionMode.Deflate, 5);
        AtmosEncryptionClient eclient = new AtmosEncryptionClient(api, ec, cc);

        return eclient;
    }
    
    private AtmosEncryptionClient getCompressionClient() throws Exception {
        CompressionConfig cc = new CompressionConfig(CompressionMode.Deflate, 5);
        AtmosEncryptionClient eclient = new AtmosEncryptionClient(api, null, cc);

        return eclient;
    }
    
    private AtmosEncryptionClient getKeystoreEncryptionClient() throws Exception {
        return getKeystoreEncryptionClient(keyAlias);
    }
    
    private AtmosEncryptionClient getKeystoreEncryptionClient(String masterKeyAlias) throws Exception {
        EncryptionConfig ec = new EncryptionConfig(keystore, 
                keystorePassword.toCharArray(), masterKeyAlias, provider, 128);
        AtmosEncryptionClient eclient = new AtmosEncryptionClient(api, ec, null);
        
        return eclient;
    }

    // Test creating an encrypted object with basic keys
    @Test
    public void testCreateEncryptBasic() throws Exception {
        AtmosEncryptionClient eclient = getBasicEncryptionClient();
        String content = "Hello World!";
        
        ObjectId id = eclient.createObject(content, "text/plain");
        cleanup.add(id);
        
        // Read back and test
        ReadObjectRequest ror = new ReadObjectRequest();
        ror.setIdentifier(id);
        ReadObjectResponse<String> resp = eclient.readObject(ror, String.class);
        
        assertEquals("Content differs", content, resp.getObject());
        assertEquals("unencrypted size incorrect", "12",
                resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_UNENC_SIZE).getValue());
        assertEquals("encrypted size incorrect", "16", resp.getMetadata().getMetadata().get("size").getValue());
        assertEquals("unencrypted sha1 incorrect", "2ef7bde608ce5404e97d5f042f95f89f1c232871", 
                resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_UNENC_SHA1).getValue());
        assertEquals("master key ID incorrect", 
                KeyUtils.getRsaPublicKeyFingerprint((RSAPublicKey) masterKey.getPublic(), provider), 
                resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_KEY_ID).getValue());
        assertNotNull("IV null", resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_IV).getValue());
        assertNotNull("Object key", resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_OBJECT_KEY).getValue());
    }
    
    // Test creating an encrypted object with keystore keys
    @Test
    public void testCreateEncryptKeystore() throws Exception {
        AtmosEncryptionClient eclient = getKeystoreEncryptionClient();
        
        String content = "Hello World!";
        
        ObjectId id = eclient.createObject(content, "text/plain");
        cleanup.add(id);
        
        // Read back and test
        ReadObjectRequest ror = new ReadObjectRequest();
        ror.setIdentifier(id);
        ReadObjectResponse<String> resp = eclient.readObject(ror, String.class);
        
        assertEquals("Content differs", content, resp.getObject());
        assertEquals("unencrypted size incorrect", "12",
                resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_UNENC_SIZE).getValue());
        assertEquals("encrypted size incorrect", "16", 
                resp.getMetadata().getMetadata().get("size").getValue());
        assertEquals("unencrypted sha1 incorrect", "2ef7bde608ce5404e97d5f042f95f89f1c232871", 
                resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_UNENC_SHA1).getValue());
        assertEquals("master key ID incorrect", 
                KeyUtils.getRsaPublicKeyFingerprint((RSAPublicKey) keystore.getCertificate(keyAlias).getPublicKey(), provider), 
                resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_KEY_ID).getValue());
        assertNotNull("IV null", resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_IV).getValue());
        assertNotNull("Object key", resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_OBJECT_KEY).getValue());
    }
    
    // Test with namespace
    @Test
    public void testCreateEncryptOnPath() throws Exception {
        AtmosEncryptionClient eclient = getBasicEncryptionClient();
        String content = "Hello World!";
        
        ObjectId id = eclient.createObject(new ObjectPath("/enctest/" + rand8char()), 
                content, "text/plain");
        cleanup.add(id);
        
        // Read back and test
        ReadObjectRequest ror = new ReadObjectRequest();
        ror.setIdentifier(id);
        ReadObjectResponse<String> resp = eclient.readObject(ror, String.class);
        
        assertEquals("Content differs", content, resp.getObject());
        assertEquals("unencrypted size incorrect", "12",
                resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_UNENC_SIZE).getValue());
        assertEquals("encrypted size incorrect", "16", resp.getMetadata().getMetadata().get("size").getValue());
        assertEquals("unencrypted sha1 incorrect", "2ef7bde608ce5404e97d5f042f95f89f1c232871", 
                resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_UNENC_SHA1).getValue());
        assertEquals("master key ID incorrect", 
                KeyUtils.getRsaPublicKeyFingerprint((RSAPublicKey) masterKey.getPublic(), provider), 
                resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_KEY_ID).getValue());
        assertNotNull("IV null", resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_IV).getValue());
        assertNotNull("Object key", resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_OBJECT_KEY).getValue());
        
    }
    
    // Test a stream > 4MB.
    @Test
    public void testCreateEncryptStream() throws Exception {
        AtmosEncryptionClient eclient = getBasicEncryptionClient();

        int size = 5*1024*1024+13;
        RandomInputStream rs = new RandomInputStream(size);
        
        ObjectId id = eclient.createObject(rs, "text/plain");
        cleanup.add(id);
        
        // Read back and test
        ReadObjectRequest ror = new ReadObjectRequest();
        ror.setIdentifier(id);
        ReadObjectResponse<byte[]> resp = eclient.readObject(ror, byte[].class);
        
        // Make sure the checksum matches
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Digest = sha1.digest(resp.getObject());
        
        // Hex Encode it
        String sha1hex = KeyUtils.toHexPadded(sha1Digest);
        
        assertNotNull("Missing SHA1 meta", resp.getMetadata().getMetadata().get(
                TransformConstants.META_ENCRYPTION_UNENC_SHA1));
        assertEquals("SHA1 incorrect", sha1hex, resp.getMetadata().getMetadata().get(
                TransformConstants.META_ENCRYPTION_UNENC_SHA1).getValue());
        assertEquals("Stream length incorrect", size, Integer.parseInt(
                resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_UNENC_SIZE).getValue()));
        
    }
    
    // Test with smaller (odd) chunks
    @Test
    public void testCreateEncryptStreamSmallChunks() throws Exception {
        AtmosEncryptionClient eclient = getBasicEncryptionClient();
        eclient.setBufferSize(999999);
        
        // Get some data to encrypt.
        InputStream classin = this.getClass().getClassLoader()
                .getResourceAsStream("uncompressed.txt");

        ObjectId id = eclient.createObject(classin, "text/plain");
        cleanup.add(id);
        
        classin.close();
        
        // Read back and test
        ReadObjectRequest ror = new ReadObjectRequest();
        ror.setIdentifier(id);
        ReadObjectResponse<byte[]> resp = eclient.readObject(ror, byte[].class);
        Map<String, Metadata> objectData = resp.getMetadata().getMetadata();

        assertEquals("Uncompressed digest incorrect",
                "027e997e6b1dfc97b93eb28dc9a6804096d85873",
                objectData.get(TransformConstants.META_ENCRYPTION_UNENC_SHA1).getValue());
        assertEquals("Uncompressed size incorrect", 2516125,
                Long.parseLong(objectData
                        .get(TransformConstants.META_ENCRYPTION_UNENC_SIZE).getValue()));
        assertNotNull("Missing IV",
                objectData.get(TransformConstants.META_ENCRYPTION_IV).getValue());
        assertEquals("Incorrect master encryption key ID",
                KeyUtils.getRsaPublicKeyFingerprint((RSAPublicKey) masterKey
                        .getPublic(), provider),
                objectData.get(TransformConstants.META_ENCRYPTION_KEY_ID).getValue());
        assertNotNull("Missing object key",
                objectData.get(TransformConstants.META_ENCRYPTION_OBJECT_KEY).getValue());
        assertNotNull("Missing metadata signature",
                objectData.get(TransformConstants.META_ENCRYPTION_META_SIG).getValue());
    }
    
    // Test creating a compressed and encrypted object
    @Test
    public void testCreateEncryptCompress() throws Exception {
        AtmosEncryptionClient eclient = getBasicEncryptionClientWithCompression();
        
        // Get some data to encrypt.
        InputStream classin = this.getClass().getClassLoader()
                .getResourceAsStream("uncompressed.txt");
        CountingInputStream incount = new CountingInputStream(classin);

        ObjectId id = eclient.createObject(incount, "text/plain");
        cleanup.add(id);
        
        classin.close();
        
        long bytesSent = incount.getByteCount();
        assertEquals("Incorrect number of bytes sent", 2516125, bytesSent);
        
        // Read back and test
        ReadObjectRequest ror = new ReadObjectRequest();
        ror.setIdentifier(id);
        ReadObjectResponse<byte[]> resp = eclient.readObject(ror, byte[].class);
        Map<String, Metadata> objectData = resp.getMetadata().getMetadata();

        assertEquals("Uncompressed digest incorrect",
                "027e997e6b1dfc97b93eb28dc9a6804096d85873",
                objectData.get(TransformConstants.META_COMPRESSION_UNCOMP_SHA1).getValue());
        assertEquals("Transform mode incorrect", 
                "COMP:Deflate/5|ENC:AES/CBC/PKCS5Padding", 
                objectData.get(TransformConstants.META_TRANSFORM_MODE).getValue());
        assertEquals("Uncompressed size incorrect", 2516125,
                Long.parseLong(objectData
                        .get(TransformConstants.META_COMPRESSION_UNCOMP_SIZE).getValue()));
        assertNotNull("Missing IV",
                objectData.get(TransformConstants.META_ENCRYPTION_IV).getValue());
        assertEquals("Incorrect master encryption key ID",
                KeyUtils.getRsaPublicKeyFingerprint((RSAPublicKey) masterKey
                        .getPublic(), provider),
                objectData.get(TransformConstants.META_ENCRYPTION_KEY_ID).getValue());
        assertNotNull("Missing object key",
                objectData.get(TransformConstants.META_ENCRYPTION_OBJECT_KEY).getValue());
        assertNotNull("Missing metadata signature",
                objectData.get(TransformConstants.META_ENCRYPTION_META_SIG).getValue());
        assertTrue("Object not compressed", 
                bytesSent > Long.parseLong(objectData.get("size").getValue()));
    }
    
    // Test creating a compressed object (no encryption)
    @Test
    public void testCreateCompress() throws Exception {
        AtmosEncryptionClient eclient = getCompressionClient();
        
        // Get some data to encrypt.
        InputStream classin = this.getClass().getClassLoader()
                .getResourceAsStream("uncompressed.txt");
        CountingInputStream incount = new CountingInputStream(classin);

        ObjectId id = eclient.createObject(incount, "text/plain");
        cleanup.add(id);
        
        classin.close();
        
        long bytesSent = incount.getByteCount();
        assertEquals("Incorrect number of bytes sent", 2516125, bytesSent);
        
        // Read back and test
        ReadObjectRequest ror = new ReadObjectRequest();
        ror.setIdentifier(id);
        ReadObjectResponse<byte[]> resp = eclient.readObject(ror, byte[].class);
        Map<String, Metadata> objectData = resp.getMetadata().getMetadata();

        assertEquals("Uncompressed digest incorrect",
                "027e997e6b1dfc97b93eb28dc9a6804096d85873",
                objectData.get(TransformConstants.META_COMPRESSION_UNCOMP_SHA1).getValue());
        assertEquals("Uncompressed size incorrect", 2516125,
                Long.parseLong(objectData
                        .get(TransformConstants.META_COMPRESSION_UNCOMP_SIZE).getValue()));
        assertTrue("Object not compressed", 
                bytesSent > Long.parseLong(objectData.get("size").getValue()));
        
        // Encryption meta should not be present.
        assertNull("Should not have IV",
                objectData.get(TransformConstants.META_ENCRYPTION_IV));
        assertNull("Should not have key",
                objectData.get(TransformConstants.META_ENCRYPTION_OBJECT_KEY));
        assertNull("Should not have signature",
                objectData.get(TransformConstants.META_ENCRYPTION_META_SIG));
        
        // Check stream
        byte[] data = resp.getObject();
        assertEquals("Stream size incorrect", 2516125, data.length);
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] digest = sha1.digest(data);
        assertEquals("Stream digest incorrect", 
                "027e997e6b1dfc97b93eb28dc9a6804096d85873", KeyUtils.toHexPadded(digest));

    }
    
    // Test rekeying an object with basic keys
    @Test
    public void testRekeyBasic() throws Exception {
        AtmosEncryptionClient eclient = getBasicEncryptionClient(oldKey);
        
        // Create an object encrypted with the old key.
        String content = "Hello World!";
        
        ObjectId id = eclient.createObject(content, "text/plain");
        cleanup.add(id);
        
        // Rekey
        eclient = getBasicEncryptionClient();
        eclient.rekey(id);
        
        // Read back -- should now be encrypted with 'masterKey'
        ReadObjectRequest ror = new ReadObjectRequest();
        ror.setIdentifier(id);
        ReadObjectResponse<String> resp = eclient.readObject(ror, String.class);
        assertEquals("Content differs", content, resp.getObject());
        assertEquals("unencrypted size incorrect", "12",
                resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_UNENC_SIZE).getValue());
        assertEquals("encrypted size incorrect", "16", resp.getMetadata().getMetadata().get("size").getValue());
        assertEquals("unencrypted sha1 incorrect", "2ef7bde608ce5404e97d5f042f95f89f1c232871", 
                resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_UNENC_SHA1).getValue());
        assertEquals("master key ID incorrect", 
                KeyUtils.getRsaPublicKeyFingerprint((RSAPublicKey) masterKey.getPublic(), provider), 
                resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_KEY_ID).getValue());
        assertNotNull("IV null", resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_IV).getValue());
        assertNotNull("Object key", resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_OBJECT_KEY).getValue());
    }
    
    // Test rekeying an object with a keystore
    public void testRekeyKeystore() throws Exception {
        AtmosEncryptionClient eclient = getKeystoreEncryptionClient(oldKeyAlias);
        
        String content = "Hello World!";
        
        ObjectId id = eclient.createObject(content, "text/plain");
        cleanup.add(id);
        
        // Rekey
        eclient = getKeystoreEncryptionClient();
        eclient.rekey(id);
        
        // Read back and test
        ReadObjectRequest ror = new ReadObjectRequest();
        ror.setIdentifier(id);
        ReadObjectResponse<String> resp = eclient.readObject(ror, String.class);
        
        assertEquals("Content differs", content, resp.getObject());
        assertEquals("unencrypted size incorrect", "12",
                resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_UNENC_SIZE).getValue());
        assertEquals("encrypted size incorrect", "16", resp.getMetadata().getMetadata().get("size"));
        assertEquals("unencrypted sha1 incorrect", "2ef7bde608ce5404e97d5f042f95f89f1c232871", 
                resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_UNENC_SHA1).getValue());
        assertEquals("master key ID incorrect", 
                KeyUtils.getRsaPublicKeyFingerprint((RSAPublicKey) masterKey.getPublic(), provider), 
                resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_KEY_ID).getValue());
        assertNotNull("IV null", resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_IV).getValue());
        assertNotNull("Object key", resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_OBJECT_KEY).getValue());
        
    }
    
    // Test rekeying an object that does not need rekeying.
//    @Test(expected=DoesNotNeedRekeyException.class)
    public void testRekeyNoRekeyRequired() throws Exception {
        AtmosEncryptionClient eclient = getKeystoreEncryptionClient(oldKeyAlias);
        
        String content = "Hello World!";
        
        ObjectId id = eclient.createObject(content, "text/plain");
        cleanup.add(id);
        
        // Rekey -- should throw an exception that no rekey is needed.
        try {
            eclient.rekey(id);
            Assert.fail("DoesNotNeedRekeyException not thrown.");
        } catch(DoesNotNeedRekeyException e) {
            assertEquals("Wrong message", 
                    "Object is already using the current master key", e.getMessage());
        }
    }
    
    // Test rekeying an object that does not need rekeying because it wasn't compressed
    // in the first place.
    //@Test(expected=DoesNotNeedRekeyException.class)
    public void testRekeyNotEncrypted() throws Exception {
        AtmosEncryptionClient eclient = getCompressionClient();
        
        String content = "Hello World!";
        
        ObjectId id = eclient.createObject(content, "text/plain");
        cleanup.add(id);
        
        // Rekey -- should throw an exception that no rekey is needed.
        try {
            eclient.rekey(id);
            Assert.fail("DoesNotNeedRekeyException not thrown.");
        } catch(DoesNotNeedRekeyException e) {
            assertEquals("Wrong message", "Object was not rekeyed", e.getMessage());
        }
    }

    
    // Test partial read (should fail)
    @Test(expected=UnsupportedOperationException.class)
    public void testPartialRead() throws Exception {
        AtmosEncryptionClient eclient = getKeystoreEncryptionClient(oldKeyAlias);
        
        String content = "Hello World!";
        
        ObjectId id = eclient.createObject(content, "text/plain");
        cleanup.add(id);
        
        // Partial read
        eclient.readObject(id, new Range(1, 1), byte[].class);
    }
    
    // Test partial read (should fail)
    @Test(expected=UnsupportedOperationException.class)
    public void testPartialRead2() throws Exception {
        AtmosEncryptionClient eclient = getKeystoreEncryptionClient(oldKeyAlias);
        
        String content = "Hello World!";
        
        ObjectId id = eclient.createObject(content, "text/plain");
        cleanup.add(id);
        
        // Partial read
        ReadObjectRequest req = new ReadObjectRequest().identifier(id).ranges(new Range(1,1), new Range(2,2));
        eclient.readObject(req, byte[].class);
    }
    
    // Test object overwrite
    @Test
    public void testOverwriteObject() throws Exception {
        AtmosEncryptionClient eclient = getBasicEncryptionClient();
        
        String content = "Hello World!";
        
        ObjectId id = eclient.createObject(content, "text/plain");
        cleanup.add(id);
        
        // Overwrite
        content = "Hello Again";
        eclient.updateObject(id, content);
        
        // Read back and check.        
        ReadObjectRequest ror = new ReadObjectRequest();
        ror.setIdentifier(id);
        ReadObjectResponse<String> resp = eclient.readObject(ror, String.class);
        
        assertEquals("Content differs", content, resp.getObject());
        assertEquals("unencrypted size incorrect", "11",
                resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_UNENC_SIZE).getValue());
        assertEquals("encrypted size incorrect", "16", resp.getMetadata().getMetadata().get("size").getValue());
        assertEquals("unencrypted sha1 incorrect", "f18fd13626d3c9c76dc1386bb6d5b4d6a9f6d365", 
                resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_UNENC_SHA1).getValue());
        assertEquals("master key ID incorrect", 
                KeyUtils.getRsaPublicKeyFingerprint((RSAPublicKey) masterKey.getPublic(), provider), 
                resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_KEY_ID).getValue());
        assertNotNull("IV null", resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_IV).getValue());
        assertNotNull("Object key", resp.getMetadata().getMetadata().get(TransformConstants.META_ENCRYPTION_OBJECT_KEY).getValue());
    }
    
    // Test object overwrite and change from encrypted to compressed.
    @Test
    public void testOverwriteObjectCompressed() throws Exception {
        AtmosEncryptionClient eclient = getBasicEncryptionClient();
        
        String content = "Hello World!";
        Metadata[] meta = new Metadata[] { new Metadata("myname", "my value", false),
                new Metadata("listable", "", true)};
        CreateObjectRequest req = new CreateObjectRequest().content(content).contentType("text/plain").userMetadata(meta);
        
        CreateObjectResponse resp1 = eclient.createObject(req);
        ObjectId id = resp1.getObjectId();
        cleanup.add(id);
        
        // Overwrite
        eclient = getCompressionClient();
        
        // Get some data to encrypt.
        InputStream classin = this.getClass().getClassLoader()
                .getResourceAsStream("uncompressed.txt");
        CountingInputStream incount = new CountingInputStream(classin);

        eclient.updateObject(id, incount);
        cleanup.add(id);
        
        classin.close();
        
        long bytesSent = incount.getByteCount();
        assertEquals("Incorrect number of bytes sent", 2516125, bytesSent);
        
        // Read back and test
        ReadObjectRequest ror = new ReadObjectRequest();
        ror.setIdentifier(id);
        ReadObjectResponse<byte[]> resp2 = eclient.readObject(ror, byte[].class);
        Map<String, Metadata> objectData = resp2.getMetadata().getMetadata();

        assertEquals("Uncompressed digest incorrect",
                "027e997e6b1dfc97b93eb28dc9a6804096d85873",
                objectData.get(TransformConstants.META_COMPRESSION_UNCOMP_SHA1).getValue());
        assertEquals("Uncompressed size incorrect", 2516125,
                Long.parseLong(objectData
                        .get(TransformConstants.META_COMPRESSION_UNCOMP_SIZE).getValue()));
        assertTrue("Object not compressed", 
                bytesSent > Long.parseLong(objectData.get("size").getValue()));
        
        // Encryption meta should not be present.
        assertNull("Should not have IV",
                objectData.get(TransformConstants.META_ENCRYPTION_IV));
        assertNull("Should not have key",
                objectData.get(TransformConstants.META_ENCRYPTION_OBJECT_KEY));
        assertNull("Should not have signature",
                objectData.get(TransformConstants.META_ENCRYPTION_META_SIG));
        
        // Make sure our metadata tags are still there.
        for(Metadata m : meta) {
            Metadata mm = null;
            assertTrue("Missing metadata tag: " + m.getName(), 
                    (mm = objectData.get(m.getName())) != null);
            assertEquals("Metadata incorrect", m, mm);
            assertEquals("Metadata listable flag incorrect for " + m.getName(), 
                    m.isListable(), mm.isListable());
        }
        
        // Check stream
        byte[] data = resp2.getObject();
        assertEquals("Stream size incorrect", 2516125, data.length);
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] digest = sha1.digest(data);
        assertEquals("Stream digest incorrect", 
                "027e997e6b1dfc97b93eb28dc9a6804096d85873", KeyUtils.toHexPadded(digest));

    }
    
    // Test partial update (should fail)
    @Test(expected=UnsupportedOperationException.class)
    public void testObjectPartialOverwrite() throws Exception {
        AtmosEncryptionClient eclient = getBasicEncryptionClient();
        
        String content = "Hello World!";
        
        ObjectId id = eclient.createObject(content, "text/plain");
        cleanup.add(id);
        
        // Overwrite
        content = "Hello Again";
        eclient.updateObject(id, content, new Range(5,15));
    }
    
    // Test object append (should fail)
    @Test(expected=UnsupportedOperationException.class)
    public void testObjectAppend() throws Exception {
        AtmosEncryptionClient eclient = getBasicEncryptionClient();
        
        String content = "Hello World!";
        
        ObjectId id = eclient.createObject(content, "text/plain");
        cleanup.add(id);
        
        // Append
        content = "Hello Again";
        UpdateObjectRequest uor = new UpdateObjectRequest().identifier(id)
                .range(new Range(12,22)).content(content);
        eclient.updateObject(uor);
    }
    
    public static String rand8char() {
        Random r = new Random();
        StringBuilder sb = new StringBuilder( 8 );
        for ( int i = 0; i < 8; i++ ) {
            sb.append( (char) ('a' + r.nextInt( 26 )) );
        }
        return sb.toString();
    }


}
