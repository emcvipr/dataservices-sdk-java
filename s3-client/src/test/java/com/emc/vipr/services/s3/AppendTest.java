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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.emc.test.util.Concurrent;
import com.emc.test.util.ConcurrentJunitRunner;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.emc.vipr.services.s3.model.AppendObjectResult;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

/**
 * Tests appending to objects using the ViPR S3 Extension methods.
 */
@RunWith(ConcurrentJunitRunner.class)
@Concurrent
public class AppendTest {
    ViPRS3Client vipr;
    
    private static final String TEST_BUCKET = "append-tests";

    @Before
    public void setUp() throws Exception {
        vipr = S3ClientFactory.getS3Client();
        Assume.assumeTrue("Could not configure S3 connection", vipr != null);
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
        vipr.putObject(TEST_BUCKET, key, new ByteArrayInputStream(data), om);
        
        // Append Object
        int length1 = data.length;
        data = testString2.getBytes();
        om.setContentLength(data.length);
        AppendObjectResult appendRes = vipr.appendObject(TEST_BUCKET, key, new ByteArrayInputStream(data), om);
        
        assertEquals("Append offset incorrect", length1, appendRes.getAppendOffset());
        
        // Check length
        ObjectMetadata om2 = vipr.getObjectMetadata(TEST_BUCKET, key);
        assertEquals("Total size incorrect", length1+data.length, om2.getContentLength());
        
        // Read Back
        S3Object s3o = vipr.getObject(TEST_BUCKET, key);
        InputStream in = s3o.getObjectContent();
        data = new byte[length1+data.length];
        in.read(data);
        in.close();
        String outString = new String(data);
        
        assertEquals("String not equal", testString+testString2, outString);
        
        vipr.deleteObject(TEST_BUCKET, key);
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
        PutObjectRequest por = new PutObjectRequest(TEST_BUCKET, 
                key, new ByteArrayInputStream(empty), om);
        
        // Create Object
        vipr.putObject(por);
        
        // Append Object
        om = new ObjectMetadata();
        byte[] data = testString.getBytes();
        om.setContentLength(data.length);
        AppendObjectResult appendRes = vipr.appendObject(TEST_BUCKET, key, 
                new ByteArrayInputStream(data), om);
        
        assertEquals("Append offset incorrect", 0, appendRes.getAppendOffset());
        
        // Check length
        ObjectMetadata om2 = vipr.getObjectMetadata(TEST_BUCKET, key);
        assertEquals("Total size incorrect", data.length, om2.getContentLength());
        
        // Read Back
        S3Object s3o = vipr.getObject(TEST_BUCKET, key);
        InputStream in = s3o.getObjectContent();
        data = new byte[data.length];
        in.read(data);
        in.close();
        String outString = new String(data);
        
        assertEquals("String not equal", testString, outString);
        
        vipr.deleteObject(TEST_BUCKET, key);
        
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
        PutObjectRequest por = new PutObjectRequest(TEST_BUCKET, 
                key, new ByteArrayInputStream(empty), om);
        vipr.putObject(por);       
        
        Set<ParallelAppend> ops = new HashSet<AppendTest.ParallelAppend>();
        
        for(int i=0; i<appends; i++) {
            ops.add(new ParallelAppend(key, ("chunk" + i).getBytes()));
        }
        
        LinkedBlockingQueue<Runnable> transferQueue = new LinkedBlockingQueue<Runnable>(
                appends);
        ThreadPoolExecutor transferPool = new ThreadPoolExecutor(threads,
                threads, 15, TimeUnit.SECONDS, transferQueue);

        List<Future<AppendObjectResult>> results = transferPool.invokeAll(ops);
        transferPool.shutdown();
        transferPool.awaitTermination(2, TimeUnit.MINUTES);
        
        // Make sure all tasks completed successfully.
        for(Future<AppendObjectResult> result : results) {
            assertTrue("Operation did not complete", result.isDone());
            
            // Make sure none threw exception
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
            GetObjectRequest req = new GetObjectRequest(TEST_BUCKET, key);
            req.setRange(offset, offset + p.data.length - 1);
            S3Object o = vipr.getObject(req);
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
        private String key;
        private AppendObjectResult result;
        
        public ParallelAppend(String key, byte[] data) {
            this.key = key;
            this.data = data;
        }

        public AppendObjectResult call() throws Exception {
            ObjectMetadata om = new ObjectMetadata();
            om.setContentLength(data.length);
            result = vipr.appendObject(TEST_BUCKET, key, 
                    new ByteArrayInputStream(data), om);
            return result;
        }
        
    }

}
