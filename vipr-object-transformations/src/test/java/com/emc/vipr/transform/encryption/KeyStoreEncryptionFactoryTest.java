package com.emc.vipr.transform.encryption;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.InvalidKeyException;
import java.security.KeyStore;
import java.security.Provider;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.emc.vipr.transform.InputTransform;
import com.emc.vipr.transform.TransformConstants;


public class KeyStoreEncryptionFactoryTest {
    private static final Logger logger = LoggerFactory.getLogger(
            KeyStoreEncryptionFactoryTest.class);
    
    private KeyStore keystore;
    private String keystorePassword = "viprviprvipr";
    private String keyAlias = "masterkey";
    private String keystoreFile = "keystore.jks";
    protected Provider provider;

    @Before
    public void setUp() throws Exception {
        // Init keystore
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
    }

    @Test
    public void testRekey() throws Exception {
        KeyStoreEncryptionFactory factory = new KeyStoreEncryptionFactory(keystore, 
                "oldkey", keystorePassword.toCharArray(), provider);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("name1", "value1");
        metadata.put("name2", "value2");
        BasicEncryptionOutputTransform outTransform = factory
                .getOutputTransform(out, metadata);

        // Get some data to encrypt.
        InputStream classin = this.getClass().getClassLoader()
                .getResourceAsStream("uncompressed.txt");
        ByteArrayOutputStream classByteStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int c = 0;
        while ((c = classin.read(buffer)) != -1) {
            classByteStream.write(buffer, 0, c);
        }
        byte[] uncompressedData = classByteStream.toByteArray();
        classin.close();

        OutputStream encryptedStream = outTransform.getEncodedOutputStream();
        encryptedStream.write(uncompressedData);
        encryptedStream.close();
        
        byte[] encryptedObject = out.toByteArray();
        Map<String, String> objectMetadata = outTransform.getEncodedMetadata();
        
        // Now, rekey.
        factory.setMasterEncryptionKeyAlias(keyAlias);
        Map<String, String> objectMetadata2 = factory.rekey(objectMetadata);
        
        // Verify that the key ID and encrypted object key changed.
        assertNotEquals("Master key ID should have changed", 
                objectMetadata.get(TransformConstants.META_ENCRYPTION_KEY_ID),
                objectMetadata2.get(TransformConstants.META_ENCRYPTION_KEY_ID));
        assertNotEquals("Encrypted object key should have changed", 
                objectMetadata.get(TransformConstants.META_ENCRYPTION_OBJECT_KEY),
                objectMetadata2.get(TransformConstants.META_ENCRYPTION_OBJECT_KEY));
        
        // Decrypt with updated key
        ByteArrayInputStream encodedInput = new ByteArrayInputStream(encryptedObject);
        InputTransform inTransform = factory.getInputTransform(
                outTransform.getTransformConfig(), encodedInput, objectMetadata2);
        InputStream inStream = inTransform.getDecodedInputStream();
        
        ByteArrayOutputStream decodedOut = new ByteArrayOutputStream();
        while ((c = inStream.read(buffer)) != -1) {
            decodedOut.write(buffer, 0, c);
        }
        
        byte[] decodedData = decodedOut.toByteArray();
        
        assertArrayEquals("Decrypted output incorrect", uncompressedData, decodedData);
    }

    @Test
    public void testRekey256() throws Exception {
        KeyStoreEncryptionFactory factory = new KeyStoreEncryptionFactory(keystore, 
                "oldkey", keystorePassword.toCharArray(), provider);

        Assume.assumeTrue("256-bit AES is not supported", 
                KeyStoreEncryptionFactory.getMaxKeySize("AES") > 128);
        factory.setEncryptionSettings(TransformConstants.DEFAULT_ENCRYPTION_TRANSFORM, 
                256, provider);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("name1", "value1");
        metadata.put("name2", "value2");
        BasicEncryptionOutputTransform outTransform = factory
                .getOutputTransform(out, metadata);

        // Get some data to encrypt.
        InputStream classin = this.getClass().getClassLoader()
                .getResourceAsStream("uncompressed.txt");
        ByteArrayOutputStream classByteStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int c = 0;
        while ((c = classin.read(buffer)) != -1) {
            classByteStream.write(buffer, 0, c);
        }
        byte[] uncompressedData = classByteStream.toByteArray();
        classin.close();

        OutputStream encryptedStream = outTransform.getEncodedOutputStream();
        encryptedStream.write(uncompressedData);
        encryptedStream.close();
        
        byte[] encryptedObject = out.toByteArray();
        Map<String, String> objectMetadata = outTransform.getEncodedMetadata();
        
        // Now, rekey.
        factory.setMasterEncryptionKeyAlias(keyAlias);
        Map<String, String> objectMetadata2 = factory.rekey(objectMetadata);
        
        // Verify that the key ID and encrypted object key changed.
        assertNotEquals("Master key ID should have changed", 
                objectMetadata.get(TransformConstants.META_ENCRYPTION_KEY_ID),
                objectMetadata2.get(TransformConstants.META_ENCRYPTION_KEY_ID));
        assertNotEquals("Encrypted object key should have changed", 
                objectMetadata.get(TransformConstants.META_ENCRYPTION_OBJECT_KEY),
                objectMetadata2.get(TransformConstants.META_ENCRYPTION_OBJECT_KEY));
        
        // Decrypt with updated key
        ByteArrayInputStream encodedInput = new ByteArrayInputStream(encryptedObject);
        InputTransform inTransform = factory.getInputTransform(
                outTransform.getTransformConfig(), encodedInput, objectMetadata2);
        InputStream inStream = inTransform.getDecodedInputStream();
        
        ByteArrayOutputStream decodedOut = new ByteArrayOutputStream();
        while ((c = inStream.read(buffer)) != -1) {
            decodedOut.write(buffer, 0, c);
        }
        
        byte[] decodedData = decodedOut.toByteArray();
        
        assertArrayEquals("Decrypted output incorrect", uncompressedData, decodedData);
    }

    @Test
    public void testKeyStoreEncryptionFactory() throws Exception {
        // Should fail if key not found.
        try {
            new KeyStoreEncryptionFactory(keystore, 
                    "NoKey", keystorePassword.toCharArray(), provider);
            fail("Should not init with invalid key alias");
        } catch(InvalidKeyException e) {
            // OK
        }
    }

    @Test
    public void testGetOutputTransform() throws Exception {
        KeyStoreEncryptionFactory factory = new KeyStoreEncryptionFactory(keystore, 
                keyAlias, keystorePassword.toCharArray(), provider);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("name1", "value1");
        metadata.put("name2", "value2");
        BasicEncryptionOutputTransform outTransform = factory
                .getOutputTransform(out, metadata);

        // Get some data to encrypt.
        InputStream classin = this.getClass().getClassLoader()
                .getResourceAsStream("uncompressed.txt");
        ByteArrayOutputStream classByteStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int c = 0;
        while ((c = classin.read(buffer)) != -1) {
            classByteStream.write(buffer, 0, c);
        }
        byte[] uncompressedData = classByteStream.toByteArray();
        classin.close();

        OutputStream encryptedStream = outTransform.getEncodedOutputStream();
        encryptedStream.write(uncompressedData);

        // Should not allow this yet.
        try {
            outTransform.getEncodedMetadata();
            fail("Should not be able to get encoded metadata until stream is closed");
        } catch (IllegalStateException e) {
            // OK.
        }

        encryptedStream.close();

        Map<String, String> objectData = outTransform.getEncodedMetadata();

        assertEquals("Uncompressed digest incorrect",
                "027e997e6b1dfc97b93eb28dc9a6804096d85873",
                objectData.get(TransformConstants.META_ENCRYPTION_UNENC_SHA1));
        assertEquals("Uncompressed size incorrect", 2516125,
                Long.parseLong(objectData
                        .get(TransformConstants.META_ENCRYPTION_UNENC_SIZE)));
        assertNotNull("Missing IV",
                objectData.get(TransformConstants.META_ENCRYPTION_IV));
        assertEquals("Incorrect master encryption key ID",
                "e3a69d422e6d9008e3cdfcbea674ccd9ab4758c3",
                objectData.get(TransformConstants.META_ENCRYPTION_KEY_ID));
        assertNotNull("Missing object key",
                objectData.get(TransformConstants.META_ENCRYPTION_OBJECT_KEY));
        assertNotNull("Missing metadata signature",
                objectData.get(TransformConstants.META_ENCRYPTION_META_SIG));
        assertEquals("name1 incorrect", "value1", objectData.get("name1"));
        assertEquals("name2 incorrect", "value2", objectData.get("name2"));

        String transformConfig = outTransform.getTransformConfig();
        assertEquals("Transform config string incorrect",
                "ENC:AES/CBC/PKCS5Padding", transformConfig);

        logger.info("Encoded metadata: " + objectData);
    }

    @Test
    public void testGetInputTransform() throws Exception {
        Map<String, String> objectMetadata = new HashMap<String, String>();
        objectMetadata.put("x-emc-enc-object-key", "iyuQuDL9qsfZk2XqnRihfw8ejr+OcrsflXvYD1I5o/Bw+wZPkY4Fm6py8ng25K/iw6kO0zbqq5v5Ywkng0pgrUdHLyR6Aq/dD0vKTK46E6sHZKknlM7NSixR8qaieBwwc2QnhOzyFPIVWSgwo9TqhlPRlOjftRLwU6Nt056BGt5Lhrqn3DeQpTrZW8LDjcpTC1UZtVXe3v9pXB4JEz8M4iFjnFprHykmixlR35RWOw4tIVEbsbcXZwt9RhVsDHj8qnkH66S88y4IOuuU4JJeFMywFXLdDs+MlUrYrA/MvfZNs34WKLYcFICKuLoHoGZ/gReJPbKy64lhSM8gTtYf/Q==");
        objectMetadata.put("x-emc-enc-key-id", "e3a69d422e6d9008e3cdfcbea674ccd9ab4758c3");
        objectMetadata.put("x-emc-enc-unencrypted-size", "2516125");
        objectMetadata.put("x-emc-enc-unencrypted-sha1", "027e997e6b1dfc97b93eb28dc9a6804096d85873");
        objectMetadata.put("x-emc-enc-metadata-signature", "F5IG2SC20oFpjLCc+5aETIy25tjUSodNlpmkae/1g91gkCYtP6NG6aLMQLHwyu789LmSegPQ/flUwcqdDE8nCI9Y2SuVbQIE5wvyB7RXRNqDIBKOan4xiOS/G5BwzzPFs6uL3I0b5Ya/VrJYhnDiRMAC+6L5kDbEVesHkx77qqCxku/SSMzCJ2K7kX/MYKfJdNQgXsFMAZs1PEcJpW8viQVTEYR8YR7bx37y4/lIHBotmC7HtB0RWAIGDFcHrnASyqpyHCYnwYjiPqItWaZy7WxRVM+qkH7IMtJT2XCuuI6VFmNzu57LN8p5ROBKO4l0hTgfgHMOUbpmQwuanb6p9Q==");
        objectMetadata.put("x-emc-iv", "OCoTA8kO0A+ZKkoZKa7VIQ==");
        objectMetadata.put("name1", "value1");
        objectMetadata.put("name2", "value2");

        InputStream encryptedObject = this.getClass().getClassLoader()
                .getResourceAsStream("encrypted.txt.keystore.aes128");

        KeyStoreEncryptionFactory factory = new KeyStoreEncryptionFactory(keystore, 
                keyAlias, keystorePassword.toCharArray(), provider);

        // Load the transform.
        InputTransform inTransform = factory.getInputTransform("ENC:AES/CBC/PKCS5Padding", encryptedObject, objectMetadata);
        InputStream inStream = inTransform.getDecodedInputStream();
        
        byte[] buffer = new byte[4096];
        int c = 0;
        
        // Decrypt into a buffer
        ByteArrayOutputStream decryptedData = new ByteArrayOutputStream();
        while ((c = inStream.read(buffer)) != -1) {
            decryptedData.write(buffer, 0, c);
        }

        // Get original data to check.
        InputStream originalStream = this.getClass().getClassLoader()
                .getResourceAsStream("uncompressed.txt");
        ByteArrayOutputStream classByteStream = new ByteArrayOutputStream();
        while ((c = originalStream.read(buffer)) != -1) {
            classByteStream.write(buffer, 0, c);
        }
        byte[] originalData = classByteStream.toByteArray();
        originalStream.close();
        
        assertArrayEquals("Decrypted data incorrect", originalData, decryptedData.toByteArray());
    }
    
    /**
     * Test using a certificate that lacks the Subject Key Identifier.  We should have
     * to compute it manually.
     */
    @Test
    public void testGetInputTransformNoCertSKI() throws Exception {
        KeyStoreEncryptionFactory factory = new KeyStoreEncryptionFactory(keystore, 
                "masterkey2", keystorePassword.toCharArray(), provider);

        assertEquals("Wrong master key",  "masterkey2", 
                factory.getMasterEncryptionKeyAlias());
        
    }

}
