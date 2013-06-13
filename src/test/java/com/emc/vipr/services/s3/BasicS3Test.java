package com.emc.vipr.services.s3;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

/**
 * This class tests basic S3 functionality through the ViPRS3Client class.  This class
 * will look for a viprs3.properties file on the classpath and use it to configure the
 * connection to ViPR.
 */
public class BasicS3Test {
    ViPRS3Client vipr;
    
    private static final String TEST_BUCKET = "basic-s3-tests";

    @Before
    public void setUp() throws Exception {
        vipr = S3ClientFactory.getS3Client();
        try {
            vipr.createBucket(TEST_BUCKET);
        } catch(AmazonS3Exception e) {
            if(e.getStatusCode() == 409) {
                // Ignore; bucket exists;
            } else {
                throw e;
            }
        }
    }

    @Test
    public void testCreateDeleteBucket() {
        vipr.createBucket(TEST_BUCKET+"-1");
        vipr.deleteBucket(TEST_BUCKET+"-1");
    }

    @Test
    public void testPutDeleteObject() throws Exception {
        String key = "testkey";
        String testString = "Hello World!";
        byte[] data = testString.getBytes();
        ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(data.length);
        om.setContentType("text/plain");
        
        vipr.putObject(TEST_BUCKET, key, new ByteArrayInputStream(data), om);
        
        S3Object s3o = vipr.getObject(TEST_BUCKET, key);
        InputStream in = s3o.getObjectContent();
        data = new byte[data.length];
        in.read(data);
        in.close();
        String outString = new String(data);
        
        assertEquals("String not equal", testString, outString);
        
        vipr.deleteObject(TEST_BUCKET, key);
    }
    
    @Test
    public void testPutObjectMetadata() throws Exception {
        String key = "testkey";
        String testString = "Hello World!";
        byte[] data = testString.getBytes();
        ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(data.length);
        om.setContentType("text/plain");
        om.addUserMetadata("name1", "value1");
        om.addUserMetadata("name2", "value2");
        
        vipr.putObject(TEST_BUCKET, key, new ByteArrayInputStream(data), om);
        
        ObjectMetadata om2 = vipr.getObjectMetadata(TEST_BUCKET, key);
        Map<String,String> meta = om2.getUserMetadata();
        assertEquals("Metadata name1 incorrect on HEAD", "value1", meta.get("name1"));
        assertEquals("Metadata name2 incorrect on HEAD", "value2", meta.get("name2"));
        
        S3Object s3o = vipr.getObject(TEST_BUCKET, key);
        InputStream in = s3o.getObjectContent();
        data = new byte[data.length];
        in.read(data);
        in.close();
        String outString = new String(data);
        assertEquals("String not equal", testString, outString);
                
        meta = s3o.getObjectMetadata().getUserMetadata();
        assertEquals("Metadata name1 incorrect on GET", "value1", meta.get("name1"));
        assertEquals("Metadata name2 incorrect on GET", "value2", meta.get("name2"));
        
        vipr.deleteObject(TEST_BUCKET, key);
        
    }

}
