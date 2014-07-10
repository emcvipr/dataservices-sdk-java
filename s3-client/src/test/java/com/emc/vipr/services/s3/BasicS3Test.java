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

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import org.junit.Test;

import java.io.*;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * This class tests basic S3 functionality through the ViPRS3Client class.  This class
 * will look for a viprs3.properties file on the classpath and use it to configure the
 * connection to ViPR.
 */
public class BasicS3Test extends AbstractViPRS3Test {
    @Override
    protected String getTestBucketPrefix() {
        return "basic-s3-tests";
    }

    @Test
    public void testCreateDeleteBucket() {
        s3.createBucket(getTestBucket()+"-test");
        s3.deleteBucket(getTestBucket()+"-test");
    }

    @Test
    public void testPutDeleteObject() throws Exception {
        String key = "testkey1";
        String testString = "Hello World!";
        byte[] data = testString.getBytes();
        ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(data.length);
        om.setContentType("text/plain");

        s3.putObject(getTestBucket(), key, new ByteArrayInputStream(data), om);

        S3Object s3o = s3.getObject(getTestBucket(), key);
        InputStream in = s3o.getObjectContent();
        data = new byte[data.length];
        in.read(data);
        in.close();
        String outString = new String(data);

        assertEquals("String not equal", testString, outString);

        s3.deleteObject(getTestBucket(), key);
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

        s3.putObject(getTestBucket(), key, new ByteArrayInputStream(data), om);

        ObjectMetadata om2 = s3.getObjectMetadata(getTestBucket(), key);
        Map<String,String> meta = om2.getUserMetadata();
        assertEquals("Metadata name1 incorrect on HEAD", "value1", meta.get("name1"));
        assertEquals("Metadata name2 incorrect on HEAD", "value2", meta.get("name2"));

        S3Object s3o = s3.getObject(getTestBucket(), key);
        InputStream in = s3o.getObjectContent();
        data = new byte[data.length];
        in.read(data);
        in.close();
        String outString = new String(data);
        assertEquals("String not equal", testString, outString);

        meta = s3o.getObjectMetadata().getUserMetadata();
        assertEquals("Metadata name1 incorrect on GET", "value1", meta.get("name1"));
        assertEquals("Metadata name2 incorrect on GET", "value2", meta.get("name2"));

        s3.deleteObject(getTestBucket(), key);
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
        TransferManager tm = new TransferManager(s3, executor);

        PutObjectRequest request = new PutObjectRequest(getTestBucket(), key, tmpFile);
        request.setMetadata(new ObjectMetadata());
        request.getMetadata().addUserMetadata("selector", "one");

        Upload upload = tm.upload(request);

        upload.waitForCompletion();

        S3Object object = s3.getObject(getTestBucket(), key);

        int size = copyStream(object.getObjectContent(), null);
        assertEquals("Wrong object size", objectSize, size);

        s3.deleteObject(getTestBucket(), key);
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
