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
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.StringInputStream;
import com.emc.test.util.Concurrent;
import com.emc.test.util.ConcurrentJunitRunner;
import com.emc.vipr.services.s3.model.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

/*
 * Test the ViPR-specific file access feature for S3
 */
@RunWith(ConcurrentJunitRunner.class)
@Concurrent
public class FileAccessTest {
    private static Log log = LogFactory.getLog(FileAccessTest.class);

    private ViPRS3Client s3;

    @Before
    public void setUp() throws Exception {
        s3 = S3ClientFactory.getS3Client();
        Assume.assumeTrue("Could not configure S3 connection", s3 != null);
    }

    protected void createBucket(String bucketName) {
        try {
            s3.createBucket(bucketName);
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() != 409) { // ignore if bucket exists
                throw e;
            }
        }
    }

    @Ignore // blocked by CQ608849
    @Test
    public void testBasicReadOnly() throws Exception {
        String bucketName = "test.vipr-basic-read-only";
        String key = "basic-read-only.txt";
        String content = "Hello read-only!";

        try {
            createBucket(bucketName);
            StringInputStream ss = new StringInputStream(content);
            ObjectMetadata om = new ObjectMetadata();
            om.setContentLength(ss.available());
            s3.putObject(bucketName, key, ss, om);

            SetBucketFileAccessModeRequest request = new SetBucketFileAccessModeRequest();
            request.setBucketName(bucketName);
            request.setAccessMode(ViPRConstants.FileAccessMode.readOnly);
            request.setDuration(300); // seconds
            request.setHostList(Arrays.asList("10.6.143.99", "10.6.143.100")); // client IP(s)
            request.setUid("501"); // client's OS UID

            // change mode to read-only
            BucketFileAccessModeResult result = s3.setBucketFileAccessMode(request);

            assertNotNull("set access-mode result is null", result);
            assertTrue("wrong access mode", request.getAccessMode() == result.getAccessMode()
                    || result.getAccessMode().transitionsToTarget(request.getAccessMode()));
            assertTrue("wrong duration", request.getDuration() - result.getDuration() < 5);
            assertArrayEquals("wrong host list", request.getHostList().toArray(), result.getHostList().toArray());
            assertEquals("wrong user", request.getUid(), result.getUid());

            // wait until complete (change is asynchronous)
            waitForTransition(bucketName, ViPRConstants.FileAccessMode.readOnly, 90);

            // verify mode change
            BucketFileAccessModeResult result2 = s3.getBucketFileAccessMode(bucketName);

            assertEquals("wrong access mode", request.getAccessMode(), result2.getAccessMode());
            assertTrue("wrong duration", request.getDuration() > result2.getDuration());
            assertArrayEquals("wrong host list", request.getHostList().toArray(), result2.getHostList().toArray());
            assertEquals("wrong user", request.getUid(), result2.getUid());

            // get NFS details
            GetFileAccessRequest fileAccessRequest = new GetFileAccessRequest();
            fileAccessRequest.setBucketName(bucketName);
            GetFileAccessResult fileAccessResult = s3.getFileAccess(fileAccessRequest);

            // verify NFS details
            assertNotNull("fileaccess result is null", fileAccessResult);
            assertNotNull("mounts is null", fileAccessResult.getMountPoints());
            assertTrue("no mounts", fileAccessResult.getMountPoints().size() > 0);
            assertNotNull("objects is null", fileAccessResult.getObjects());
            assertEquals("wrong number of objects", 1, fileAccessResult.getObjects().size());

            // change mode back to disabled
            request = new SetBucketFileAccessModeRequest();
            request.setBucketName(bucketName);
            request.setAccessMode(ViPRConstants.FileAccessMode.disabled);
            s3.setBucketFileAccessMode(request);

            // wait until complete
            waitForTransition(bucketName, ViPRConstants.FileAccessMode.disabled, 90);

            // verify mode change
            fileAccessRequest = new GetFileAccessRequest();
            fileAccessRequest.setBucketName(bucketName);
            try {
                s3.getFileAccess(fileAccessRequest);
                fail("GET fileaccess should fail when access mode is disabled");
            } catch (AmazonS3Exception e) {
                if (!"FileAccessNotAllowed".equals(e.getErrorCode())) throw e;
            }

        } finally {
            cleanBucket(bucketName);
        }
    }

    @Test
    public void testReadWriteWindow() throws Exception {
        String bucketName = "test.vipr-fileaccess-window";
        String key1 = "test1.txt";
        String key2 = "test2.txt";
        String key3 = "test3.txt";
        String key4 = "test4.txt";
        String key5 = "test5.txt";
        String key6 = "test6.txt";
        String content = "Hello World!";
        String clientHost = "10.10.10.10";
        String clientUid = "501";
        long fileAccessDuration = 60 * 60; // seconds (1 hour)

        try {
            // create some objects
            s3.createBucket(bucketName);
            Set<String> keys = new TreeSet<String>();
            s3.putObject(bucketName, key1, new StringInputStream(content), null);
            s3.putObject(bucketName, key2, new StringInputStream(content), null);
            s3.putObject(bucketName, key3, new StringInputStream(content), null);
            s3.putObject(bucketName, key4, new StringInputStream(content), null);
            keys.addAll(Arrays.asList(key1, key2, key3, key4));

            SetBucketFileAccessModeRequest request = new SetBucketFileAccessModeRequest();
            request.setBucketName(bucketName);
            request.setAccessMode(ViPRConstants.FileAccessMode.readWrite);
            request.setDuration(fileAccessDuration); // seconds
            request.setHostList(Arrays.asList(clientHost)); // client IP(s)
            request.setUid(clientUid); // client's OS UID

            // change mode to read-write
            BucketFileAccessModeResult result = s3.setBucketFileAccessMode(request);

            assertNotNull("set access-mode result is null", result);
            assertTrue("wrong access mode", request.getAccessMode() == result.getAccessMode()
                    || result.getAccessMode().transitionsToTarget(request.getAccessMode()));
            assertTrue("wrong duration", request.getDuration() - result.getDuration() < 5);
            assertArrayEquals("wrong host list", new String[]{clientHost}, result.getHostList().toArray());
            assertEquals("wrong user", clientUid, result.getUid());

            // wait until complete (change is asynchronous)
            waitForTransition(bucketName, ViPRConstants.FileAccessMode.readWrite, 90);

            // verify mode change
            BucketFileAccessModeResult result2 = s3.getBucketFileAccessMode(bucketName);
            String tokenA = result2.getEndToken();

            assertEquals("wrong access mode", request.getAccessMode(), result2.getAccessMode());
            assertTrue("wrong duration", request.getDuration() > result2.getDuration());
            assertArrayEquals("wrong host list", new String[]{clientHost}, result2.getHostList().toArray());
            assertEquals("wrong user", clientUid, result2.getUid());

            // get NFS details
            GetFileAccessRequest fileAccessRequest = new GetFileAccessRequest();
            fileAccessRequest.setBucketName(bucketName);
            GetFileAccessResult fileAccessResult = s3.getFileAccess(fileAccessRequest);

            // verify NFS details
            assertNotNull("fileaccess result is null", fileAccessResult);
            assertNotNull("mounts is null", fileAccessResult.getMountPoints());
            assertTrue("no mounts", fileAccessResult.getMountPoints().size() > 0);
            assertNotNull("objects is null", fileAccessResult.getObjects());
            assertEquals("wrong number of objects", 4, fileAccessResult.getObjects().size());
            for (String key : keys) {
                boolean found = false;
                for (FileAccessObject object : fileAccessResult.getObjects()) {
                    if (key.equals(object.getName())) {
                        found = true;
                        break;
                    }
                }
                if (!found) fail("key " + key + " not found in export list");
            }

            // create more objects (part of a new workflow in the bucket)
            s3.putObject(bucketName, key5, new StringInputStream(content), null);
            s3.putObject(bucketName, key6, new StringInputStream(content), null);
            keys.addAll(Arrays.asList(key5, key6));

            request = new SetBucketFileAccessModeRequest();
            request.setBucketName(bucketName);
            request.setAccessMode(ViPRConstants.FileAccessMode.readWrite);
            request.setDuration(fileAccessDuration); // seconds
            request.setHostList(Arrays.asList(clientHost)); // client IP(s)
            request.setUid(clientUid); // client's OS UID
            request.setToken(tokenA); // end-token from last request

            // change mode to read-write using token
            result = s3.setBucketFileAccessMode(request);

            assertNotNull("set access-mode result is null", result);
            assertTrue("wrong access mode", request.getAccessMode() == result.getAccessMode()
                    || result.getAccessMode().transitionsToTarget(request.getAccessMode()));
            assertTrue("wrong duration", request.getDuration() - result.getDuration() < 5);
            assertArrayEquals("wrong host list", new String[]{clientHost}, result.getHostList().toArray());
            assertEquals("wrong user", clientUid, result.getUid());

            // wait until complete (change is asynchronous)
            waitForTransition(bucketName, ViPRConstants.FileAccessMode.readWrite, 90);

            // verify mode change
            result2 = s3.getBucketFileAccessMode(bucketName);
            String tokenB = result2.getEndToken();

            assertEquals("wrong access mode", request.getAccessMode(), result2.getAccessMode());
            assertTrue("wrong duration", request.getDuration() > result2.getDuration());
            assertArrayEquals("wrong host list", new String[]{clientHost}, result2.getHostList().toArray());
            assertEquals("wrong user", clientUid, result2.getUid());
            assertNotEquals("wrong token", tokenA, result2.getEndToken());

            // get NFS details
            fileAccessResult = s3.getFileAccess(fileAccessRequest);

            // verify NFS details
            assertNotNull("fileaccess result is null", fileAccessResult);
            assertNotNull("mounts is null", fileAccessResult.getMountPoints());
            assertTrue("no mounts", fileAccessResult.getMountPoints().size() > 0);
            assertNotNull("objects is null", fileAccessResult.getObjects());
            assertEquals("wrong number of objects", 6, fileAccessResult.getObjects().size());
            for (String key : keys) {
                boolean found = false;
                for (FileAccessObject object : fileAccessResult.getObjects()) {
                    if (key.equals(object.getName())) {
                        found = true;
                        break;
                    }
                }
                if (!found) fail("key " + key + " not found in export list");
            }

            request = new SetBucketFileAccessModeRequest();
            request.setBucketName(bucketName);
            request.setAccessMode(ViPRConstants.FileAccessMode.disabled);
            request.setToken(tokenA); // end-token from first request

            // change mode to disabled using token
            result = s3.setBucketFileAccessMode(request);

            assertNotNull("set access-mode result is null", result);
            assertTrue("wrong access mode", request.getAccessMode() == ViPRConstants.FileAccessMode.readWrite
                    || result.getAccessMode() == ViPRConstants.FileAccessMode.switchingToDisabled);

            // wait until complete (change is asynchronous)
            waitForTransition(bucketName, null, 90);

            // verify mode change
            result2 = s3.getBucketFileAccessMode(bucketName);
            keys.removeAll(Arrays.asList(key1, key2, key3, key4));

            // mode will still be read-write
            assertEquals("wrong access mode", ViPRConstants.FileAccessMode.readWrite, result2.getAccessMode());
            assertArrayEquals("wrong host list", new String[]{clientHost}, result2.getHostList().toArray());
            assertEquals("wrong user", clientUid, result2.getUid());
            assertEquals("wrong token", tokenB, result2.getEndToken());

            // get NFS details
            fileAccessResult = s3.getFileAccess(fileAccessRequest);

            // verify NFS details
            assertNotNull("fileaccess result is null", fileAccessResult);
            assertNotNull("mounts is null", fileAccessResult.getMountPoints());
            assertTrue("no mounts", fileAccessResult.getMountPoints().size() > 0);
            assertNotNull("objects is null", fileAccessResult.getObjects());
            assertEquals("wrong number of objects", 2, fileAccessResult.getObjects().size());
            for (String key : keys) {
                boolean found = false;
                for (FileAccessObject object : fileAccessResult.getObjects()) {
                    if (key.equals(object.getName())) {
                        found = true;
                        break;
                    }
                }
                if (!found) fail("key " + key + " not found in export list");
            }

            request = new SetBucketFileAccessModeRequest();
            request.setBucketName(bucketName);
            request.setAccessMode(ViPRConstants.FileAccessMode.disabled);
            request.setToken(tokenB); // end-token from second request

            // change mode to disabled using token
            result = s3.setBucketFileAccessMode(request);

            assertNotNull("set access-mode result is null", result);
            assertTrue("wrong access mode", request.getAccessMode() == ViPRConstants.FileAccessMode.disabled
                    || result.getAccessMode() == ViPRConstants.FileAccessMode.switchingToDisabled);

            // wait until complete (change is asynchronous)
            waitForTransition(bucketName, null, 90);

            // verify mode change
            result2 = s3.getBucketFileAccessMode(bucketName);

            // entire bucket should be disabled now
            assertEquals("wrong access mode", ViPRConstants.FileAccessMode.disabled, result2.getAccessMode());
        } finally {
            cleanBucket(bucketName);
        }
    }

    // XXX: unfortunately there is currently no good way to automate this test
    @Test
    public void testPreserveIngestPaths() throws Exception {
        String bucketName = "test.ingest";
        String key1 = "test1.txt";
        String key2 = "test2.txt";
        String key3 = "test3.txt";
        String key4 = "test4.txt";
        String content = "Hello World!";
        String clientHost = "10.10.10.10";
        String clientUid = "501";
        long fileAccessDuration = 60 * 60; // seconds (1 hour)

        try {
            // create some objects
            s3.createBucket(bucketName);
            Set<String> keys = new TreeSet<String>();
            s3.putObject(bucketName, key1, new StringInputStream(content), null);
            s3.putObject(bucketName, key2, new StringInputStream(content), null);
            s3.putObject(bucketName, key3, new StringInputStream(content), null);
            s3.putObject(bucketName, key4, new StringInputStream(content), null);
            keys.addAll(Arrays.asList(key1, key2, key3, key4));

            SetBucketFileAccessModeRequest requestReadOnly = new SetBucketFileAccessModeRequest();
            requestReadOnly.setBucketName(bucketName);
            requestReadOnly.setAccessMode(ViPRConstants.FileAccessMode.readOnly);
            requestReadOnly.setDuration(fileAccessDuration); // seconds
            requestReadOnly.setHostList(Arrays.asList(clientHost)); // client IP(s)
            requestReadOnly.setUid(clientUid); // client's OS UID

            // change mode to read-only
            // this is to ensure the feature is working properly without preserve-ingest-paths
            BucketFileAccessModeResult result = s3.setBucketFileAccessMode(requestReadOnly);

            assertNotNull("set access-mode result is null", result);
            assertTrue("wrong access mode", requestReadOnly.getAccessMode() == result.getAccessMode()
                    || result.getAccessMode().transitionsToTarget(requestReadOnly.getAccessMode()));
            assertTrue("wrong duration", requestReadOnly.getDuration() - result.getDuration() < 5);
            assertArrayEquals("wrong host list", new String[]{clientHost}, result.getHostList().toArray());
            assertEquals("wrong user", clientUid, result.getUid());

            // wait until complete (change is asynchronous)
            waitForTransition(bucketName, ViPRConstants.FileAccessMode.readOnly, 90);

            // verify mode change
            result = s3.getBucketFileAccessMode(bucketName);

            assertEquals("wrong access mode", requestReadOnly.getAccessMode(), result.getAccessMode());
            assertTrue("wrong duration", requestReadOnly.getDuration() > result.getDuration());
            assertArrayEquals("wrong host list", new String[]{clientHost}, result.getHostList().toArray());
            assertEquals("wrong user", clientUid, result.getUid());

            // get NFS details
            GetFileAccessRequest fileAccessRequest = new GetFileAccessRequest();
            fileAccessRequest.setBucketName(bucketName);
            GetFileAccessResult fileAccessResult = s3.getFileAccess(fileAccessRequest);

            // verify NFS details
            assertNotNull("fileaccess result is null", fileAccessResult);
            assertNotNull("mounts is null", fileAccessResult.getMountPoints());
            assertTrue("no mounts", fileAccessResult.getMountPoints().size() > 0);
            assertNotNull("objects is null", fileAccessResult.getObjects());
            assertEquals("wrong number of objects", 4, fileAccessResult.getObjects().size());
            for (String key : keys) {
                boolean found = false;
                for (FileAccessObject object : fileAccessResult.getObjects()) {
                    if (key.equals(object.getName())) {
                        found = true;
                        break;
                    }
                }
                if (!found) fail("key " + key + " not found in export list");
            }

            SetBucketFileAccessModeRequest requestDisabled = new SetBucketFileAccessModeRequest();
            requestDisabled.setBucketName(bucketName);
            requestDisabled.setAccessMode(ViPRConstants.FileAccessMode.disabled);

            // change mode to disabled
            s3.setBucketFileAccessMode(requestDisabled);

            waitForTransition(bucketName, ViPRConstants.FileAccessMode.disabled, 90);

            // now try to enable with preserve-ingest-paths
            // this should fail since the bucket was not ingested, but that's ok; we're only testing that the header
            // is being processed
            try {
                requestReadOnly.setPreserveIngestPaths(true);
                s3.setBucketFileAccessMode(requestReadOnly);
                fail("preserving ingest paths on a standard bucket should fail");
            } catch (AmazonS3Exception e) {
                if (!e.getErrorCode().equals("InvalidArgument")) throw e;
                // InvalidArgument expected
            }

        } finally {
            cleanBucket(bucketName);
        }
    }

    /**
     * waits until the target access mode is completely transitioned on the specified bucket.
     *
     * @param bucketName bucket name
     * @param targetMode target access mode to wait for (readOnly, readWrite, or disabled). Can be null if target mode
     *                   is unknown (if you're disabling a portion of the bucket and don't know if there
     *                   are still exported objects)
     * @param timeout    after the specified number of seconds, this method will throw a TimeoutException
     * @throws InterruptedException if interrupted while sleeping between GET intervals
     * @throws TimeoutException     if the specified timeout is reached before transition is complete
     */
    protected void waitForTransition(String bucketName, ViPRConstants.FileAccessMode targetMode, int timeout)
            throws InterruptedException, TimeoutException {
        if (targetMode != null && targetMode.isTransitionState())
            throw new IllegalArgumentException("Invalid target mode: " + targetMode);
        long start = System.currentTimeMillis(), interval = 500;
        timeout *= 1000;
        while (true) {
            // GET the current access mode
            BucketFileAccessModeResult result = s3.getBucketFileAccessMode(bucketName);

            if (targetMode == null) {
                if (!result.getAccessMode().isTransitionState()) {
                    return; // must be complete since the bucket is not in a transition state
                }
            } else {
                if (targetMode == result.getAccessMode()) {
                    return; // transition is complete
                }

                if (!result.getAccessMode().isTransitionState() || !result.getAccessMode().transitionsToTarget(targetMode))
                    throw new RuntimeException(String.format("Bucket %s in mode %s will never get to mode %s",
                            bucketName, result.getAccessMode(), targetMode));
            }

            // if we've reached our timeout
            long runTime = System.currentTimeMillis() - start;
            if (runTime >= timeout)
                throw new TimeoutException(String.format("Access mode transition for %s took longer than %d seconds",
                        bucketName, timeout / 1000));

            // transitioning; wait and query again
            long timeLeft = timeout - runTime;
            Thread.sleep(Math.min(timeLeft, interval));
        }
    }

    protected void cleanBucket(String bucketName) {
        try {
            for (S3ObjectSummary summary : s3.listObjects(bucketName).getObjectSummaries()) {
                s3.deleteObject(bucketName, summary.getKey());
            }
            s3.deleteBucket(bucketName);
        } catch (Exception e) {
            // don't cause tests to fail
            log.warn(String.format("Could not clean up bucket %s", bucketName), e);
        }
    }
}
