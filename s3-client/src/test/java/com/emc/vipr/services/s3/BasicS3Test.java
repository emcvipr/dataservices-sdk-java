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

import static org.junit.Assert.*;

import java.io.*;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

import com.emc.test.util.Concurrent;
import com.emc.test.util.ConcurrentJunitRunner;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import org.junit.runner.RunWith;

/**
 * This class tests basic S3 functionality through the ViPRS3Client class.  This class
 * will look for a viprs3.properties file on the classpath and use it to configure the
 * connection to ViPR.
 */
@RunWith(ConcurrentJunitRunner.class)
@Concurrent
public class BasicS3Test {
    protected ViPRS3Client vipr;

    private static final String TEST_BUCKET = "basic-s3-tests";

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

    @Test
    public void testCreateDeleteBucket() {
        vipr.createBucket(TEST_BUCKET+"-1");
        vipr.deleteBucket(TEST_BUCKET+"-1");
    }

    @Test
    public void testPutDeleteObject() throws Exception {
        String key = "testkey1";
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
        String key = "testkey2";
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

    @Test
    public void testMultipartUpload() throws Exception {
        String key = "multipartKey";

        // write large file (must be a file to support parallel uploads)
        File tmpFile = File.createTempFile("random", "bin");
        tmpFile.deleteOnExit();
        int objectSize = 31 * 1000 * 1000; // 31M (not a power of 2)

        copyStream(new RandomInputStream(objectSize), new FileOutputStream(tmpFile));

        assertEquals("tmp file is not the right size", objectSize, tmpFile.length());

        ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10, 60, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(50));
        TransferManager tm = new TransferManager(vipr, executor);

        PutObjectRequest request = new PutObjectRequest(TEST_BUCKET, key, tmpFile);
        request.setMetadata(new ObjectMetadata());
        request.getMetadata().addUserMetadata("selector", "one");

        Upload upload = tm.upload(request);

        upload.waitForCompletion();

        S3Object object = vipr.getObject(TEST_BUCKET, key);

        int size = copyStream(object.getObjectContent(), null);
        assertEquals("Wrong object size", objectSize, size);

        vipr.deleteObject(TEST_BUCKET, key);
    }

    private int copyStream(InputStream is, OutputStream os) throws IOException {
        byte[] buffer = new byte[100 * 1024];
        int total = 0, read = is.read(buffer);
        while (read >= 0) {
            if (os != null)
                os.write(buffer, 0, read);
            total += read;
            read = is.read(buffer);
        }
        is.close();
        if (os != null)
            os.close();
        return total;
    }
}
