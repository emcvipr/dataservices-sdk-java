/*
 * Copyright 2013 EMC Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.emc.vipr.services.s3;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.emc.vipr.services.s3.model.UpdateObjectRequest;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class UpdateTest extends AbstractViPRS3Test {
    @Override
    protected String getTestBucketPrefix() {
        return "update-tests";
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
        
        s3.putObject(getTestBucket(), key, new ByteArrayInputStream(data), om);

        // Update "World" to "Again"
        String updateString = "Again";
        data = updateString.getBytes("US-ASCII");
        om = new ObjectMetadata();
        om.setContentLength(data.length);
        UpdateObjectRequest r = new UpdateObjectRequest(getTestBucket(), key, 
                new ByteArrayInputStream(data), om).withUpdateRange(6, 10);
        viprS3.updateObject(r);
        
        S3Object s3o = s3.getObject(getTestBucket(), key);
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
        
        s3.putObject(getTestBucket(), key, new ByteArrayInputStream(data), om);

        // Update "World" to "Again"
        String updateString = "Again";
        data = updateString.getBytes("US-ASCII");
        om = new ObjectMetadata();
        om.setContentLength(data.length);
        UpdateObjectRequest r = new UpdateObjectRequest(getTestBucket(), key, 
                new ByteArrayInputStream(data), om).withUpdateOffset(6);
        viprS3.updateObject(r);
        
        S3Object s3o = s3.getObject(getTestBucket(), key);
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
        
        s3.putObject(getTestBucket(), key, new ByteArrayInputStream(data), om);

        // Update "World" to "Again" but invalid range
        try {
            String updateString = "Again";
            data = updateString.getBytes("US-ASCII");
            om = new ObjectMetadata();
            om.setContentLength(data.length);
            UpdateObjectRequest r = new UpdateObjectRequest(getTestBucket(), key, 
                    new ByteArrayInputStream(data), om).withUpdateRange(15,6);
            viprS3.updateObject(r);
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
        
        s3.putObject(getTestBucket(), key, new ByteArrayInputStream(data), om);

        // Update with valid range but not enough bytes to fill it.
        try {
            String updateString = "Ag";
            data = updateString.getBytes("US-ASCII");
            om = new ObjectMetadata();
            om.setContentLength(data.length);
            UpdateObjectRequest r = new UpdateObjectRequest(getTestBucket(), key, 
                    new ByteArrayInputStream(data), om).withUpdateRange(6,10);
            viprS3.updateObject(r);
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
        
        s3.putObject(getTestBucket(), key, new ByteArrayInputStream(data), om);

        // Update with valid range but too many bytes to fill it.
        try {
            String updateString = "Again and again and again...";
            data = updateString.getBytes("US-ASCII");
            om = new ObjectMetadata();
            om.setContentLength(data.length);
            UpdateObjectRequest r = new UpdateObjectRequest(getTestBucket(), key, 
                    new ByteArrayInputStream(data), om).withUpdateRange(6,10);
            viprS3.updateObject(r);
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
        
        s3.putObject(getTestBucket(), key, new ByteArrayInputStream(data), om);

        // Update but put start offset beyond the end of the object.
        try {
            String updateString = "Again";
            data = updateString.getBytes("US-ASCII");
            om = new ObjectMetadata();
            om.setContentLength(data.length);
            UpdateObjectRequest r = new UpdateObjectRequest(getTestBucket(), key, 
                    new ByteArrayInputStream(data), om).withUpdateOffset(15);
            viprS3.updateObject(r);
            //fail("Expected exception for invalid range.");
        } catch(AmazonS3Exception e) {
            assertEquals("Expected HTTP 416", 416, e.getStatusCode());
        }
        S3Object s3o = s3.getObject(getTestBucket(), key);
        InputStream in = s3o.getObjectContent();
        data = new byte[255];
        int c = in.read(data);
        in.close();
        String outString = new String(data, 0, c, "US-ASCII");
        
        assertEquals("String not equal", "Hello Again!", outString);
    }
}
