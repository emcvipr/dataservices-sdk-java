package com.emc.vipr.services.s3;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.StringInputStream;
import com.emc.vipr.services.s3.model.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

/*
 * Test the ViPR-specific file access feature for S3
 */
public class ViPRS3FileAccessTest {
    private static Log log = LogFactory.getLog(ViPRS3FileAccessTest.class);

    private ViPRS3Client s3;

    @Before
    public void setUp() throws Exception {
        s3 = S3ClientFactory.getS3Client();
    }

    @Test
    public void testBasicReadOnly() throws Exception {
        String bucketName = "test.vipr-fileaccess-basic-read-only";
        String key = "basic-read-only.txt";
        String content = "Hello read-only!";

        try {
            s3.createBucket(bucketName);
            s3.putObject(bucketName, key, new StringInputStream(content), null);

            SetBucketFileAccessModeRequest request = new SetBucketFileAccessModeRequest();
            request.setBucketName(bucketName);
            request.setAccessMode(ViPRConstants.FileAccessMode.ReadOnly);
            request.setAccessProtocol(ViPRConstants.FileAccessProtocol.NFS);
            request.setFileAccessDuration(300); // seconds
            request.setUser("arnetc"); // restrictions??

            // change mode to read-only
            SetBucketFileAccessModeResult result = s3.setBucketFileAccessMode(request);

            assertNotNull("set access-mode result is null", result);
            assertEquals("wrong access mode", request.getAccessMode(), result.getAccessMode());
            assertEquals("wrong duration", request.getFileAccessDuration(), result.getFileAccessDuration());
            assertArrayEquals("wrong host list", request.getHostList().toArray(), result.getHostList().toArray());
            assertEquals("wrong user", request.getUser(), result.getUser());

            // wait until complete (change is asynchronous)
            waitForTransition(bucketName, ViPRConstants.FileAccessMode.ReadOnly, 5000); // wait for up to 5 seconds

            // verify mode change
            GetFileAccessRequest fileAccessRequest = new GetFileAccessRequest();
            fileAccessRequest.setBucketName(bucketName);
            GetFileAccessResult fileAccessResult = s3.getFileAccess(fileAccessRequest);

            assertNotNull("fileaccess result is null", fileAccessResult);
            assertEquals("wrong access mode", request.getAccessMode(), fileAccessResult.getAccessMode());
            assertEquals("wrong protocol", request.getAccessProtocol(), fileAccessResult.getAccessProtocol());
            assertEquals("wrong duration", request.getFileAccessDuration(), fileAccessResult.getFileAccessDuration());
            assertArrayEquals("wrong host list", request.getHostList().toArray(), fileAccessResult.getHosts().toArray());
            assertEquals("wrong user", request.getUser(), fileAccessResult.getUser());

            // verify object details
            assertNotNull("mounts is null", fileAccessResult.getMountPoints());
            assertTrue("no mounts", fileAccessResult.getMountPoints().size() > 0);
            assertNotNull("objects is null", fileAccessResult.getObjects());
            assertEquals("wrong number of objects", 1, fileAccessResult.getObjects().size());
            assertEquals("wrong key", key, fileAccessResult.getObjects().get(0).getName());

            // change mode back to disabled
            request = new SetBucketFileAccessModeRequest();
            request.setBucketName(bucketName);
            request.setAccessMode(ViPRConstants.FileAccessMode.Disabled);
            s3.setBucketFileAccessMode(request);

            // wait until complete
            waitForTransition(bucketName, ViPRConstants.FileAccessMode.Disabled, 5000);

            // verify mode change
            fileAccessRequest = new GetFileAccessRequest();
            fileAccessRequest.setBucketName(bucketName);
            try {
                s3.getFileAccess(fileAccessRequest);
                fail("GET fileaccess should fail when access mode is Disabled");
            } catch (AmazonS3Exception e) {
                if (!e.getErrorCode().equals("FileAccessNotAllowed")) throw e;
            }

        } finally {
            cleanBucket(bucketName);
        }
    }

    /*
     * allowed transitions:
     * Disabled -> ReadOnly
     * Disabled -> ReadWrite
     * ReadOnly -> Disabled
     * SwitchToReadOnly -> Disabled (cancel read-only request)
     * ReadWrite -> Disabled
     * SwitchToReadWrite -> Disabled (cancel read-write request)
     */
    public void testAllTransitionsOnEmptyBucket() {

    }

    /**
     * waits until the target access mode is completely transitioned on the specified bucket.
     *
     * @param bucketName bucket name
     * @param accessMode target access mode to wait for (ReadOnly, ReadWrite, or Disabled)
     * @param timeout    after the specified number of milliseconds, this method will throw a TimeoutException
     * @throws InterruptedException if interrupted while sleeping between GET intervals
     * @throws TimeoutException     if the specified timeout is reached before transition is complete
     */
    protected void waitForTransition(String bucketName, ViPRConstants.FileAccessMode accessMode, int timeout) throws InterruptedException, TimeoutException {
        long start = System.currentTimeMillis(), interval = 500;
        while (true) {
            GetBucketFileAccessModeResult result = s3.getBucketFileAccessMode(bucketName);
            if (accessMode == result.getAccessMode()) return; // transition is complete

            // TODO: make FileAccessMode aware of transition states
            ViPRConstants.FileAccessMode transitionMode;
            switch (accessMode) {
                case ReadOnly:
                    transitionMode = ViPRConstants.FileAccessMode.SwitchingToReadOnly;
                    break;
                case ReadWrite:
                    transitionMode = ViPRConstants.FileAccessMode.SwitchingToReadWrite;
                    break;
                case Disabled:
                    transitionMode = ViPRConstants.FileAccessMode.SwitchingToDisabled;
                    break;
                default:
                    throw new IllegalArgumentException("Invalid target mode: " + accessMode);
            }

            if (transitionMode != result.getAccessMode())
                throw new RuntimeException(String.format("Expected transition mode %s for target mode %s, but got %s instead",
                        transitionMode, accessMode, result.getAccessMode()));

            // if we've reached our timeout
            long runTime = System.currentTimeMillis() - start;
            if (runTime >= timeout)
                throw new TimeoutException(String.format("Access mode transition for %s took longer than %dms",
                        bucketName, timeout));

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
