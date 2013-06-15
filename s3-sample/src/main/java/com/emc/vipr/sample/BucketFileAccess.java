package com.emc.vipr.sample;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import com.amazonaws.util.StringInputStream;
import com.emc.vipr.services.s3.ViPRS3Client;
import com.emc.vipr.services.s3.model.GetBucketFileAccessModeResult;
import com.emc.vipr.services.s3.model.GetFileAccessRequest;
import com.emc.vipr.services.s3.model.GetFileAccessResult;
import com.emc.vipr.services.s3.model.SetBucketFileAccessModeRequest;
import com.emc.vipr.services.s3.model.SetBucketFileAccessModeResult;
import com.emc.vipr.services.s3.model.ViPRConstants;

public class BucketFileAccess {
    private ViPRS3Client s3;

    public BucketFileAccess() {
        this.s3 = S3ClientFactory.getS3Client();
    }

    public void runSample() throws Exception {
        String bucketName = "temp.vipr-fileaccess";
        String key1 = "test1.txt";
        String key2 = "test2.txt";
        String content = "Hello World!";
        String clientHost = "10.10.10.10"; // change to a real client to test
        String clientUid = "501"; // change to your client uid to test
        long fileAccessDuration = 20; // seconds

        try {
            s3.createBucket(bucketName);
            s3.putObject(bucketName, key1, new StringInputStream(content), null);
            s3.putObject(bucketName, key2, new StringInputStream(content), null);

            SetBucketFileAccessModeRequest request = new SetBucketFileAccessModeRequest();
            request.setBucketName(bucketName);
            request.setAccessMode(ViPRConstants.FileAccessMode.ReadOnly);
            request.setAccessProtocol(ViPRConstants.FileAccessProtocol.NFS);
            request.setFileAccessDuration(fileAccessDuration);
            request.setHostList(Arrays.asList(clientHost));
            request.setUser(clientUid);

            // change mode to read-only
            SampleUtils.log(
                    "Changing access mode on bucket %s to NFS read-only",
                    bucketName);
            SetBucketFileAccessModeResult result = s3
                    .setBucketFileAccessMode(request);

            // this token can be used in the future to restrict mode changes
            // only to objects that have been created since this request was
            // made. For example, say you have a bucket workflow in which one
            // process requires filesystem access and after that process is
            // complete, the objects should once again be available via REST.
            // This token allows you to execute your workflow in batches. You
            // can turn on filesystem access using the previous token, which
            // will enable it only for new objects. Once that access expires,
            // those objects will be available again via REST. The next batch
            // will use the token returned from this batch and so on.
            String token = result.getToken();
            SampleUtils.log("Token to represent net-new objects: %s", token);

            // wait until complete (change is asynchronous)
            SampleUtils.log("Waiting for bucket mode to change...");
            waitForTransition(bucketName,
                    ViPRConstants.FileAccessMode.ReadOnly, 5000);
            SampleUtils
                    .log("Change complete; bucket is now in NFS read-only mode");

            // now the bucket should be accessible via NFS, let's get the
            // details
            GetFileAccessRequest fileAccessRequest = new GetFileAccessRequest();
            fileAccessRequest.setBucketName(bucketName);
            GetFileAccessResult fileAccessResult = s3
                    .getFileAccess(fileAccessRequest);

            // here are your mount points
            SampleUtils.log("NFS mount points:");
            for (String mountPoint : fileAccessResult.getMountPoints()) {
                SampleUtils.log("    %s", mountPoint);
            }

            // here are the paths to each object
            SampleUtils.log("paths to objects:");
            for (com.emc.vipr.services.s3.model.Object object : fileAccessResult
                    .getObjects()) {
                SampleUtils.log("    %s", object.getRelativePath());
            }

            // change mode back to disabled (must do this before going to
            // read-write)
            SampleUtils.log("Changing file access mode back to Disabled");
            request = new SetBucketFileAccessModeRequest();
            request.setBucketName(bucketName);
            request.setAccessMode(ViPRConstants.FileAccessMode.Disabled);
            s3.setBucketFileAccessMode(request);

            // wait until complete
            SampleUtils.log("Waiting for bucket mode to change...");
            waitForTransition(bucketName,
                    ViPRConstants.FileAccessMode.Disabled, 5000);
            SampleUtils
                    .log("Change complete; bucket file access is now Disabled");

            // now change to read-write
            SampleUtils.log("Changing file access mode to read-write");
            request = new SetBucketFileAccessModeRequest();
            request.setBucketName(bucketName);
            request.setAccessMode(ViPRConstants.FileAccessMode.ReadWrite);
            s3.setBucketFileAccessMode(request);

            SampleUtils.log("Waiting for bucket mode to change...");
            // wait until complete
            waitForTransition(bucketName,
                    ViPRConstants.FileAccessMode.ReadWrite, 5000);
            SampleUtils
                    .log("Change complete; bucket file access is now NFS read-write");

            // now the objects are writable via NFS

            // change mode back to disabled
            SampleUtils.log("Changing file access mode back to Disabled");
            request = new SetBucketFileAccessModeRequest();
            request.setBucketName(bucketName);
            request.setAccessMode(ViPRConstants.FileAccessMode.Disabled);
            s3.setBucketFileAccessMode(request);

            // wait until complete
            SampleUtils.log("Waiting for bucket mode to change...");
            waitForTransition(bucketName,
                    ViPRConstants.FileAccessMode.Disabled, 5000);
            SampleUtils
                    .log("Change complete; bucket file access is now Disabled");
        } finally {
            SampleUtils.cleanBucket(s3, bucketName);
        }
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        BucketFileAccess instance = new BucketFileAccess();
        instance.runSample();
    }

    /**
     * waits until the target access mode is completely transitioned on the
     * specified bucket.
     * 
     * @param bucketName
     *            bucket name
     * @param targetMode
     *            target access mode to wait for (ReadOnly, ReadWrite, or
     *            Disabled)
     * @param timeout
     *            after the specified number of milliseconds, this method will
     *            throw a TimeoutException
     * @throws InterruptedException
     *             if interrupted while sleeping between GET intervals
     * @throws TimeoutException
     *             if the specified timeout is reached before transition is
     *             complete
     */
    protected void waitForTransition(String bucketName,
            ViPRConstants.FileAccessMode targetMode, int timeout)
            throws InterruptedException, TimeoutException {
        long start = System.currentTimeMillis(), interval = 500;
        while (true) {
            // GET the current access mode
            GetBucketFileAccessModeResult result = s3
                    .getBucketFileAccessMode(bucketName);
            if (targetMode == result.getAccessMode())
                return; // transition is complete

            if (targetMode.isTransitionState())
                throw new IllegalArgumentException("Invalid target mode: "
                        + targetMode);

            if (!result.getAccessMode().isTransitionState()
                    || !result.getAccessMode().transitionsToTarget(targetMode))
                throw new RuntimeException(String.format(
                        "Bucket %s in mode %s will never get to mode %s",
                        bucketName, result.getAccessMode(), targetMode));

            // if we've reached our timeout
            long runTime = System.currentTimeMillis() - start;
            if (runTime >= timeout)
                throw new TimeoutException(String.format(
                        "Access mode transition for %s took longer than %dms",
                        bucketName, timeout));

            // transitioning; wait and query again
            long timeLeft = timeout - runTime;
            Thread.sleep(Math.min(timeLeft, interval));
        }
    }
}
