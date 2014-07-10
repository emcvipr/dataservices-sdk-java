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

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.emc.vipr.services.s3.model.AppendObjectResult;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * Tests appending to objects using the ViPR S3 Extension methods.
 */
public class AppendTest extends AbstractViPRS3Test {
    @Override
    protected String getTestBucketPrefix() {
        return "append-tests";
    }

    /**
     * Basic append test.
     */
    @Test
    public void testAppend() throws Exception {
        String key = "testkey1";
        String testString = "Hello";
        String testString2 = " World!";
        byte[] data = testString.getBytes();
        ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(data.length);
        om.setContentType("text/plain");
        
        // Create Object
        s3.putObject(getTestBucket(), key, new ByteArrayInputStream(data), om);
        
        // Append Object
        int length1 = data.length;
        data = testString2.getBytes();
        om.setContentLength(data.length);
        AppendObjectResult appendRes = viprS3.appendObject(getTestBucket(), key, new ByteArrayInputStream(data), om);
        
        assertEquals("Append offset incorrect", length1, appendRes.getAppendOffset());
        
        // Check length
        ObjectMetadata om2 = s3.getObjectMetadata(getTestBucket(), key);
        assertEquals("Total size incorrect", length1+data.length, om2.getContentLength());
        
        // Read Back
        S3Object s3o = s3.getObject(getTestBucket(), key);
        InputStream in = s3o.getObjectContent();
        data = new byte[length1+data.length];
        in.read(data);
        in.close();
        String outString = new String(data);
        
        assertEquals("String not equal", testString+testString2, outString);
        
        s3.deleteObject(getTestBucket(), key);
    }
    
    /**
     * Tests appending to a zero byte object.
     */
    @Test
    public void testAppendEmptyObject() throws Exception {
        String key = "testkey2";
        String testString = "Hello World!";
        
        byte[] empty = new byte[0];
        ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(0);
        PutObjectRequest por = new PutObjectRequest(getTestBucket(), 
                key, new ByteArrayInputStream(empty), om);
        
        // Create Object
        s3.putObject(por);
        
        // Append Object
        om = new ObjectMetadata();
        byte[] data = testString.getBytes();
        om.setContentLength(data.length);
        AppendObjectResult appendRes = viprS3.appendObject(getTestBucket(), key,
                new ByteArrayInputStream(data), om);
        
        assertEquals("Append offset incorrect", 0, appendRes.getAppendOffset());
        
        // Check length
        ObjectMetadata om2 = s3.getObjectMetadata(getTestBucket(), key);
        assertEquals("Total size incorrect", data.length, om2.getContentLength());
        
        // Read Back
        S3Object s3o = s3.getObject(getTestBucket(), key);
        InputStream in = s3o.getObjectContent();
        data = new byte[data.length];
        in.read(data);
        in.close();
        String outString = new String(data);
        
        assertEquals("String not equal", testString, outString);
        
        s3.deleteObject(getTestBucket(), key);
        
    }
    
    /**
     * Tests appending to an object with parallel threads.  The order of appends is not
     * guaranteed, but the appends should all succeed and be atomic.
     */
    @Test
    public void testParallelAppends() throws Exception {
        String key = "parallel";
        int appends = 64;
        int threads = 8;
        
        // Create an empty object.
        byte[] empty = new byte[0];
        ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(0);
        PutObjectRequest por = new PutObjectRequest(getTestBucket(), 
                key, new ByteArrayInputStream(empty), om);
        s3.putObject(por);
        
        Set<ParallelAppend> ops = new HashSet<AppendTest.ParallelAppend>();
        
        for(int i=0; i<appends; i++) {
            ops.add(new ParallelAppend(getTestBucket(), key, ("chunk" + i).getBytes()));
        }

        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        List<Future<AppendObjectResult>> results = executorService.invokeAll(ops);

        // wait for tasks to complete.
        for(Future<AppendObjectResult> result : results) {
            try {
                result.get();
            } catch(ExecutionException e) {
                fail("Operation failed: " + e);
            }
        }
        
        // Check that all append offsets are unique.
        Set<Long> offsets = new HashSet<Long>();
        for(ParallelAppend p : ops) {
            long offset = p.result.getAppendOffset();
            if(offsets.contains(offset)) {
                fail("Duplicate append offset: " + offset);
            } else {
                offsets.add(offset);
            }
        }
        
        // Make sure everything is appended where it should be.
        for(ParallelAppend p : ops) {
            long offset = p.result.getAppendOffset();
            GetObjectRequest req = new GetObjectRequest(getTestBucket(), key);
            req.setRange(offset, offset + p.data.length - 1);
            S3Object o = s3.getObject(req);
            byte[] d = new byte[p.data.length];
            InputStream in = o.getObjectContent();
            in.read(d);
            in.close();
            String expected = new String(p.data);
            String actual = new String(d);
            assertEquals("Chunk data wrong", expected, actual);
        }
    }
    
    /**
     * Inner class used to execute the parallel appends.
     */
    class ParallelAppend implements Callable<AppendObjectResult> {
        private byte[] data;
        private String bucket;
        private String key;
        private AppendObjectResult result;
        
        public ParallelAppend(String bucket, String key, byte[] data) {
            this.bucket = bucket;
            this.key = key;
            this.data = data;
        }

        public AppendObjectResult call() throws Exception {
            ObjectMetadata om = new ObjectMetadata();
            om.setContentLength(data.length);
            result = viprS3.appendObject(bucket, key,
                    new ByteArrayInputStream(data), om);
            return result;
        }
    }
}
