package com.emc.vipr.services.s3;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3EncryptionClient;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

/**
 * Test using the S3 Encryption Client with ViPR.
 */
public class S3EncryptionClientTest {
    AmazonS3EncryptionClient s3;
    
    private static final String TEST_BUCKET = "basic-s3-encryption-tests";

    @Before
    public void setUp() throws Exception{
        s3 = S3ClientFactory.getEncryptionClient();
        try {
            s3.createBucket(TEST_BUCKET);
        } catch(AmazonS3Exception e) {
            if(e.getStatusCode() == 409) {
                // Ignore; bucket exists;
            } else {
                throw e;
            }
        }
    }
    
    @Test
    public void testPutDeleteObjectEncrypted() throws Exception {
        String key = "testkey";
        String testString = "Hello World!";
        byte[] data = testString.getBytes();
        ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(data.length);
        om.setContentType("text/plain");
        
        s3.putObject(TEST_BUCKET, key, new ByteArrayInputStream(data), om);
        
        S3Object s3o = s3.getObject(TEST_BUCKET, key);
        InputStream in = s3o.getObjectContent();
        data = new byte[data.length];
        in.read(data);
        in.close();
        String outString = new String(data);
        
        assertEquals("String not equal", testString, outString);
        
        s3.deleteObject(TEST_BUCKET, key);

    }
}
