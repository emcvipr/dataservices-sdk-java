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

import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

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
        s3.createBucket(getTestBucket() + "-test");
        s3.deleteBucket(getTestBucket() + "-test");
    }

    @Test
    public void testListBuckets() {
        String testBucket1 = getTestBucket() + "-1", testBucket2 = getTestBucket() + "-2";

        s3.createBucket(testBucket1);
        s3.createBucket(testBucket2);

        List<Bucket> buckets = s3.listBuckets();
        assertNotNull("buckets is null", buckets);
        assertFalse("buckets is empty", buckets.isEmpty());
        assertTrue("buckets size is wrong", buckets.size() >= 3);

        int bucketIndex = Collections.binarySearch(buckets, new Bucket(getTestBucket()), bucketComparator);
        assertTrue("missing bucket " + getTestBucket(), bucketIndex >= 0);
        bucketIndex = Collections.binarySearch(buckets, new Bucket(testBucket1), bucketComparator);
        assertTrue("missing bucket " + testBucket1, bucketIndex >= 0);
        bucketIndex = Collections.binarySearch(buckets, new Bucket(testBucket2), bucketComparator);
        assertTrue("missing bucket " + testBucket2, bucketIndex >= 0);

        s3.deleteBucket(testBucket1);
        s3.deleteBucket(testBucket2);
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
        Map<String, String> meta = om2.getUserMetadata();
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

    @Test
    public void testFunkyKeys() throws Exception {
        String key1 = "foo!@#$%^&*()_-+=",
                key2 = "foo//bar",
                key3 = "foo bar/",
                key4 = "/foo/bar/baz space.txt";
        byte[] content = "Hello Funky Keys!".getBytes("UTF-8");

        ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(content.length);
        om.setContentType("text/plain");

        s3.putObject(getTestBucket(), key1, new ByteArrayInputStream(content), om);
        S3Object s3o = s3.getObject(getTestBucket(), key1);
        Assert.assertEquals(s3o.getKey(), key1);
        Assert.assertArrayEquals(key1 + " content different", readStream(s3o.getObjectContent()), content);
        s3.deleteObject(getTestBucket(), key1);

        s3.putObject(getTestBucket(), key2, new ByteArrayInputStream(content), om);
        s3o = s3.getObject(getTestBucket(), key2);
        Assert.assertEquals(s3o.getKey(), key2);
        Assert.assertArrayEquals(key2 + " content different", readStream(s3o.getObjectContent()), content);
        s3.deleteObject(getTestBucket(), key2);

        s3.putObject(getTestBucket(), key3, new ByteArrayInputStream(content), om);
        s3o = s3.getObject(getTestBucket(), key3);
        Assert.assertEquals(s3o.getKey(), key3);
        Assert.assertArrayEquals(key3 + " content different", readStream(s3o.getObjectContent()), content);
        s3.deleteObject(getTestBucket(), key3);

        s3.putObject(getTestBucket(), key4, new ByteArrayInputStream(content), om);
        s3o = s3.getObject(getTestBucket(), key4);
        Assert.assertEquals(s3o.getKey(), key4);
        Assert.assertArrayEquals(key4 + " content different", readStream(s3o.getObjectContent()), content);
        s3.deleteObject(getTestBucket(), key4);
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

    private byte[] readStream(InputStream is) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        copyStream(is, baos);
        return baos.toByteArray();
    }

    private Comparator<Bucket> bucketComparator = new Comparator<Bucket>() {
        @Override
        public int compare(Bucket o1, Bucket o2) {
            return o1.getName().compareTo(o2.getName());
        }
    };
}
