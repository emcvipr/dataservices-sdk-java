package com.emc.vipr.transform.encryption;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.interfaces.RSAPublicKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.emc.vipr.transform.InputTransform;
import com.emc.vipr.transform.TransformConstants;

public class BasicEncryptionTransformFactoryTest {
    private static final Logger logger = LoggerFactory
            .getLogger(BasicEncryptionTransformFactoryTest.class);

    private Properties keyprops;
    private KeyPair masterKey;
    private KeyPair oldKey;
    protected Provider provider;

    @Before
    public void setUp() throws Exception {
        // Load some keys.
        keyprops = new Properties();
        keyprops.load(this.getClass().getClassLoader()
                .getResourceAsStream("keys.properties"));

        masterKey = KeyUtils.rsaKeyPairFromBase64(
                keyprops.getProperty("masterkey.public"),
                keyprops.getProperty("masterkey.private"));
        oldKey = KeyUtils.rsaKeyPairFromBase64(
                keyprops.getProperty("oldkey.public"),
                keyprops.getProperty("oldkey.private"));
    }

    @Test
    public void testRejectSmallKey() throws Exception {
        // An RSA key < 1024 bits should be rejected as a master key.
        BasicEncryptionTransformFactory factory = new BasicEncryptionTransformFactory();
        factory.setCryptoProvider(provider);
        KeyPair smallKey = null;
        try {
            smallKey = KeyUtils.rsaKeyPairFromBase64(
                    keyprops.getProperty("smallkey.public"),
                    keyprops.getProperty("smallkey.private"));
        } catch (Exception e) {
            // Good!
            logger.info("Key was properly rejected by JVM: " + e);
            return;
        }

        try {
            factory.setMasterEncryptionKey(smallKey);
        } catch (Exception e) {
            // Good!
            logger.info("Key was properly rejected by factory: " + e);
            return;
        }

        fail("RSA key < 1024 bits should have been rejected by factory");
    }

    @Test
    public void testSetMasterEncryptionKey() throws Exception {
        BasicEncryptionTransformFactory factory = new BasicEncryptionTransformFactory();
        factory.setCryptoProvider(provider);
        factory.setMasterEncryptionKey(masterKey);
    }

    @Test
    public void testAddMasterDecryptionKey() throws Exception {
        BasicEncryptionTransformFactory factory = new BasicEncryptionTransformFactory();
        factory.setCryptoProvider(provider);
        factory.setMasterEncryptionKey(masterKey);
        factory.addMasterDecryptionKey(oldKey);
    }

    @Test
    public void testGetOutputTransformPush() throws Exception {
        BasicEncryptionTransformFactory factory = new BasicEncryptionTransformFactory();
        factory.setCryptoProvider(provider);
        factory.setMasterEncryptionKey(masterKey);
        factory.addMasterDecryptionKey(oldKey);
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
                KeyUtils.getRsaPublicKeyFingerprint((RSAPublicKey) masterKey
                        .getPublic(), provider),
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
    public void testGetOutputTransformPull() throws Exception {
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
        
        BasicEncryptionTransformFactory factory = new BasicEncryptionTransformFactory();
        factory.setCryptoProvider(provider);
        factory.setMasterEncryptionKey(masterKey);
        factory.addMasterDecryptionKey(oldKey);
        
        ByteArrayInputStream in = new ByteArrayInputStream(uncompressedData);

        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("name1", "value1");
        metadata.put("name2", "value2");
        BasicEncryptionOutputTransform outTransform = factory
                .getOutputTransform(in, metadata);

        InputStream encryptedStream = outTransform.getEncodedInputStream();
        while((c = encryptedStream.read(buffer)) != -1) {
            // discard
        }

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
                KeyUtils.getRsaPublicKeyFingerprint((RSAPublicKey) masterKey
                        .getPublic(), provider),
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

        logger.info("Encoded metadata: " + objectData);    }

    @Test
    public void testGetInputTransform() throws Exception {
        Map<String, String> objectMetadata = new HashMap<String, String>();
        objectMetadata.put("x-emc-enc-object-key", "holbhxpDq92g0dPRuqmtAt23KaNSm3JjKULdzadKdJyn3SINDSbaHnjmDU/Aa5pNSmq8ij+RWdkBlsrk9g6m/tjQm1gMPbeW+IhWJhInL0Mvsa+olZ+cLkztnKz/yHQ6vj9R0m8OboATweV1TTkRx9RFrr2nEBY7jKHNxd8JOJ/1I3gteuhsLKKE9oF2uS0UTVnCTZ3S6tf0W4P9D0PTW9LXQaA3KkFD+4tEbqZfC4ov88CLRAL72YC6KCF1LDZdqGzvqKf2j+92xIiy99+5LatVJPUebVucM8Equ6lAcETjNEsIwLPSNz2P9/saYI8XdLyZQDsOMg/32BbtVCVs7g==");
        objectMetadata.put("x-emc-enc-key-id", "000317457b5645b7b5c4daf4cf6780c05438effd");
        objectMetadata.put("x-emc-enc-unencrypted-size", "2516125");
        objectMetadata.put("x-emc-enc-unencrypted-sha1", "027e997e6b1dfc97b93eb28dc9a6804096d85873");
        objectMetadata.put("x-emc-enc-metadata-signature", "G98fTE0w8HzdzbqXv5x5AKl2/MudwrEIJ3nZciI1H9HQKg+i3Jmvi+/miEOeyMv8+lOVDj6CiBUMBpUsqrx46NODC+0MiA3L2JotW2DUJqfvfaTKtgbbFdSUYHshDDw3zZXcULX/flk5vjTYTICBSjedn9tg+VTA24ivk4IPexmPR7BKN+UmRZv6nPuvV1soGWg69K+5qv47lQf2rC4yO7FUXRJA10+nES1/8UmB3NylCwgI/a7UKu00o8kYABqgzVNbWgB4GjCqOtNcWJGSz8Xku3nWySetFLVs0wcwioZ3KyHIf+6p4XzbHx1ie4t9fhZuAYoOTPqDTu0o0QB/pg==");
        objectMetadata.put("x-emc-iv", "omQ2kgZauWkK58/m+Eichg==");
        objectMetadata.put("name1", "value1");
        objectMetadata.put("name2", "value2");

        InputStream encryptedObject = this.getClass().getClassLoader()
                .getResourceAsStream("encrypted.txt.aes128");

        BasicEncryptionTransformFactory factory = new BasicEncryptionTransformFactory();
        factory.setCryptoProvider(provider);
        factory.setMasterEncryptionKey(masterKey);

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
     * Test the rejection of a master KeyPair that's not an RSA key (e.g. a DSA
     * key)
     */
    @Test
    public void testRejectNonRsaMasterKey() throws Exception {
        KeyPairGenerator keyGenerator = KeyPairGenerator.getInstance("DSA");
        keyGenerator.initialize(512, new SecureRandom());
        KeyPair myKeyPair = keyGenerator.generateKeyPair();

        BasicEncryptionTransformFactory factory = new BasicEncryptionTransformFactory();
        factory.setCryptoProvider(provider);

        try {
            factory.setMasterEncryptionKey(myKeyPair);
        } catch (Exception e) {
            // Good!
            logger.info("DSA key was properly rejected by factory: " + e);
            return;
        }
        fail("DSA keys should not be allowed.");
    }

    /**
     * Test encrypting with one key, changing the master encryption key, then
     * decrypting. The old key should be found and used as the decryption key.
     */
    @Test
    public void testRekey() throws Exception {
        BasicEncryptionTransformFactory factory = new BasicEncryptionTransformFactory();
        factory.setCryptoProvider(provider);
        factory.setMasterEncryptionKey(oldKey);

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
        factory.setMasterEncryptionKey(masterKey);
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
    
    /**
     * Test encrypting with one key, changing the master encryption key, then
     * decrypting. The old key should be found and used as the decryption key.
     */
    @Test
    public void testRekey256() throws Exception {
        BasicEncryptionTransformFactory factory = new BasicEncryptionTransformFactory();
        factory.setCryptoProvider(provider);
        factory.setMasterEncryptionKey(oldKey);
        
        Assume.assumeTrue("256-bit AES is not supported", 
                BasicEncryptionTransformFactory.getMaxKeySize("AES") > 128);
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
        factory.setMasterEncryptionKey(masterKey);
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

}
