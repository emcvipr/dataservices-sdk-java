package com.emc.vipr.services.s3;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.emc.vipr.services.s3.model.UpdateObjectRequest;

public class UpdateTest {
    ViPRS3Client vipr;
    
    private static final String TEST_BUCKET = "update-tests";

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
    public void testUpdateRange() throws Exception {
        // Create base object.
        String key = "testkey";
        String testString = "Hello World!";
        byte[] data = testString.getBytes("US-ASCII");
        ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(data.length);
        om.setContentType("text/plain");
        
        vipr.putObject(TEST_BUCKET, key, new ByteArrayInputStream(data), om);

        // Update "World" to "Again"
        String updateString = "Again";
        data = updateString.getBytes("US-ASCII");
        om = new ObjectMetadata();
        om.setContentLength(data.length);
        UpdateObjectRequest r = new UpdateObjectRequest(TEST_BUCKET, key, 
                new ByteArrayInputStream(data), om).withUpdateRange(6, 10);
        vipr.updateObject(r);
        
        S3Object s3o = vipr.getObject(TEST_BUCKET, key);
        InputStream in = s3o.getObjectContent();
        data = new byte[255];
        int c = in.read(data);
        in.close();
        String outString = new String(data, 0, c, "US-ASCII");
        
        assertEquals("String not equal", "Hello Again!", outString);
    }
    
    @Test
    public void testUpdateStartOffset() throws Exception {
        // Create base object.
        String key = "testkey";
        String testString = "Hello World!";
        byte[] data = testString.getBytes("US-ASCII");
        ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(data.length);
        om.setContentType("text/plain");
        
        vipr.putObject(TEST_BUCKET, key, new ByteArrayInputStream(data), om);

        // Update "World" to "Again"
        String updateString = "Again";
        data = updateString.getBytes("US-ASCII");
        om = new ObjectMetadata();
        om.setContentLength(data.length);
        UpdateObjectRequest r = new UpdateObjectRequest(TEST_BUCKET, key, 
                new ByteArrayInputStream(data), om).withUpdateOffset(6);
        vipr.updateObject(r);
        
        S3Object s3o = vipr.getObject(TEST_BUCKET, key);
        InputStream in = s3o.getObjectContent();
        data = new byte[255];
        int c = in.read(data);
        in.close();
        String outString = new String(data, 0, c, "US-ASCII");
        
        assertEquals("String not equal", "Hello Again!", outString);
        
    }
        
    // Negative tests
    @Test
    public void testUpdateInvalidRange() throws Exception {
        // Create base object.
        String key = "testkey";
        String testString = "Hello World!";
        byte[] data = testString.getBytes("US-ASCII");
        ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(data.length);
        om.setContentType("text/plain");
        
        vipr.putObject(TEST_BUCKET, key, new ByteArrayInputStream(data), om);

        // Update "World" to "Again" but invalid range
        try {
            String updateString = "Again";
            data = updateString.getBytes("US-ASCII");
            om = new ObjectMetadata();
            om.setContentLength(data.length);
            UpdateObjectRequest r = new UpdateObjectRequest(TEST_BUCKET, key, 
                    new ByteArrayInputStream(data), om).withUpdateRange(15,6);
            vipr.updateObject(r);
            fail("Expected exception for invalid range.");
        } catch(AmazonS3Exception e) {
            assertEquals("Expected HTTP 416", 416, e.getStatusCode());
        }
    }
    
    @Test
    public void testUpdateRangeShortBody() throws Exception {
        // Create base object.
        String key = "testkey";
        String testString = "Hello World!";
        byte[] data = testString.getBytes("US-ASCII");
        ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(data.length);
        om.setContentType("text/plain");
        
        vipr.putObject(TEST_BUCKET, key, new ByteArrayInputStream(data), om);

        // Update with valid range but not enough bytes to fill it.
        try {
            String updateString = "Ag";
            data = updateString.getBytes("US-ASCII");
            om = new ObjectMetadata();
            om.setContentLength(data.length);
            UpdateObjectRequest r = new UpdateObjectRequest(TEST_BUCKET, key, 
                    new ByteArrayInputStream(data), om).withUpdateRange(6,10);
            vipr.updateObject(r);
            fail("Expected exception for invalid range.");
        } catch(AmazonS3Exception e) {
            assertEquals("Expected HTTP 416", 416, e.getStatusCode());
        }
        
    }
    
    @Test
    public void testUpdateRangeLongBody() throws Exception {
        // Create base object.
        String key = "testkey";
        String testString = "Hello World!";
        byte[] data = testString.getBytes("US-ASCII");
        ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(data.length);
        om.setContentType("text/plain");
        
        vipr.putObject(TEST_BUCKET, key, new ByteArrayInputStream(data), om);

        // Update with valid range but too many bytes to fill it.
        try {
            String updateString = "Again and again and again...";
            data = updateString.getBytes("US-ASCII");
            om = new ObjectMetadata();
            om.setContentLength(data.length);
            UpdateObjectRequest r = new UpdateObjectRequest(TEST_BUCKET, key, 
                    new ByteArrayInputStream(data), om).withUpdateRange(6,10);
            vipr.updateObject(r);
            fail("Expected exception for invalid range.");
        } catch(AmazonS3Exception e) {
            assertEquals("Expected HTTP 416", 416, e.getStatusCode());
        }
        
    }
    
//    @Test
    public void testUpdateStartOffsetBeyondEnd() throws Exception {
        // Create base object.
        String key = "testkey";
        String testString = "Hello World!";
        byte[] data = testString.getBytes("US-ASCII");
        ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(data.length);
        om.setContentType("text/plain");
        
        vipr.putObject(TEST_BUCKET, key, new ByteArrayInputStream(data), om);

        // Update but put start offset beyond the end of the object.
        try {
            String updateString = "Again";
            data = updateString.getBytes("US-ASCII");
            om = new ObjectMetadata();
            om.setContentLength(data.length);
            UpdateObjectRequest r = new UpdateObjectRequest(TEST_BUCKET, key, 
                    new ByteArrayInputStream(data), om).withUpdateOffset(15);
            vipr.updateObject(r);
            //fail("Expected exception for invalid range.");
        } catch(AmazonS3Exception e) {
            assertEquals("Expected HTTP 416", 416, e.getStatusCode());
        }
        S3Object s3o = vipr.getObject(TEST_BUCKET, key);
        InputStream in = s3o.getObjectContent();
        data = new byte[255];
        int c = in.read(data);
        in.close();
        String outString = new String(data, 0, c, "US-ASCII");
        
        assertEquals("String not equal", "Hello Again!", outString);
        
    }
 

}
