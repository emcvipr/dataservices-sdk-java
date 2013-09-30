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
package com.emc.vipr.sample;

import com.amazonaws.util.StringInputStream;
import com.emc.vipr.services.s3.ViPRS3Client;
import com.emc.vipr.services.s3.model.*;

import java.util.*;
import java.util.concurrent.TimeoutException;

/**
 * EMC ViPR data services includes an option to export S3 buckets and Swift
 * containers over NFS.  The set of objects exported in this manor is fixed and new
 * objects added via the REST protocol will not be visible in existing exports.
 * Conversely, you cannot create or delete objects in the exported mount points,
 * although when mounted read-write, you can modify existing objects.  While
 * objects are available over NFS, they are inaccessible via REST and vice versa.
 * <p/>
 * The set of objects to be exported is defined by a start-token and an end-token, which
 * represent specific points in time for the bucket. These tokens are returned from any
 * call ("get" or "set"). Only one token may be used in "set" calls with the following outcome:
 * <p/>
 * when setting mode to readOnly or readWrite and a token is included,
 * only enable access for objects *newer* than (created since) the specified token.
 * <p/>
 * when setting mode to disabled and a token is included, only disable
 * access for objects *older* than (created before) the specified token.
 * <p/>
 * note that the tokens are inclusive, so it is possible that start-token = end-token (this will
 * be true for the first time you enable file access).
 * <p/>
 * Leaving out the token in a request signifies that all objects in the bucket should be
 * affected by the operation.
 * <p/>
 * Using the "set" call to enable access for new objects and disable access for
 * old objects, you can effectively establish a sliding window (over time) of
 * objects available via NFS for a given bucket.
 */
public class BucketFileAccess {
    private ViPRS3Client s3;

    public BucketFileAccess() {
        this.s3 = S3ClientFactory.getS3Client();
    }

    public void runSample() throws Exception {
        String bucketName = "temp.vipr-fileaccess";
        String key1 = "test1.txt";
        String key2 = "test2.txt";
        String key3 = "test3.txt";
        String key4 = "test4.txt";
        String key5 = "test5.txt";
        String key6 = "test6.txt";
        String content = "Hello World!";
        String clientHost = "10.10.10.10"; // change to a real client to test
        String clientUid = "501"; // change to your client uid to test
        long fileAccessDuration = 60 * 60; // seconds (1 hour)

        try {
            // create some objects
            s3.createBucket(bucketName);
            s3.putObject(bucketName, key1, new StringInputStream(content), null);
            s3.putObject(bucketName, key2, new StringInputStream(content), null);
            s3.putObject(bucketName, key3, new StringInputStream(content), null);
            s3.putObject(bucketName, key4, new StringInputStream(content), null);

            // switch to NFS access for all objects in the bucket (this is the first export, so we don't have a token)
            SampleUtils.log("Enabling NFS access for all objects in bucket %s..", bucketName);
            String tokenA = exportNewObjects(bucketName, fileAccessDuration, Arrays.asList(clientHost),
                    clientUid, null);

            // this token is a bucket version. it represents a point in time. objects older than this token are now
            // exported via NFS. any objects created after this point in time will not be accessible via NFS, but *will*
            // be accessible via REST.
            SampleUtils.log("This export represents version %s of the bucket", tokenA);

            // here we get the exports and object paths
            Map<String, List<FileAccessObject>> mountMap = getNfsObjectMap(bucketName);

            // now we can mount the exports and process the objects in our workflow with some file-based tool
            SampleUtils.log("processing objects in version %s..", tokenA);
            for (String export : mountMap.keySet()) {
                SampleUtils.log("> %s", export);
                for (FileAccessObject object : mountMap.get(export)) {
                    // for this sample, "processing" just means dumping the objects to the log
                    SampleUtils.log("> > %s => %s", object.getName(), object.getRelativePath());
                }
            }

            // create more objects (part of a new workflow in the bucket)
            Set<String> workflowBKeys = new HashSet<String>();
            s3.putObject(bucketName, key5, new StringInputStream(content), null);
            workflowBKeys.add(key5);
            s3.putObject(bucketName, key6, new StringInputStream(content), null);
            workflowBKeys.add(key6);

            // now, switch to NFS access for the new objects using the end-token returned from the last call.
            // when enabling NFS access, all objects *newer* than the token are included in the NFS export.
            SampleUtils.log("Enabling NFS access for new objects since version %s..", tokenA);
            String tokenB = exportNewObjects(bucketName, fileAccessDuration, Arrays.asList(clientHost),
                    clientUid, tokenA);

            SampleUtils.log("Now the export represents version %s of the bucket", tokenB);

            // here we get the exports and object paths
            mountMap = getNfsObjectMap(bucketName);

            // now we can mount the exports and process the objects in our workflow with some file-based tool.
            // we'll have to cherry-pick the new objects because the GET call returns everything (even objects from
            // the last workflow).
            SampleUtils.log("processing only new objects by using a filter..");
            for (String export : mountMap.keySet()) {
                SampleUtils.log("> %s", export);
                for (FileAccessObject object : mountMap.get(export)) {
                    // filter:
                    if (!workflowBKeys.contains(object.getName())) continue;
                    // for this sample, "processing" just means dumping the objects to the log
                    SampleUtils.log("> > %s => %s", object.getName(), object.getRelativePath());
                }
            }

            // NFS access for a bucket is enabled only for objects between a starting and ending bucket version
            // (start-token and end-token). at this point in the sample, there are two simultaneous sequential NFS
            // "workflows" for the same bucket. you can have as many of these workflows as you want so long as they are
            // sequential and are therefore disabled in the same order they were enabled (there is only one sliding
            // window of access).

            // now let's assume the first workflow is complete.
            // switch mode back to disabled for those objects by passing end-token from first workflow.
            // this makes all objects in first workflow available via REST again.
            // when disabling NFS access, all objects *older* than the token are excluded from the NFS export.
            SampleUtils.log("Work complete on first set of objects, disabling file access for version %s..", tokenA);
            disableOldObjects(bucketName, tokenA);

            // now let's assume the second workflow is complete.
            // switch mode back to disabled for these objects by passing end-token from second workflow.
            // NOTE: setting mode to disabled *without* specifying a token will disable NFS access for *all* objects in
            // the bucket.
            SampleUtils.log("Work complete on second set of objects, disabling file access for version %s..", tokenB);
            disableOldObjects(bucketName, tokenB);

        } finally {
            SampleUtils.cleanBucket(s3, bucketName);
        }
    }

    public static void main(String[] args) throws Exception {
        BucketFileAccess instance = new BucketFileAccess();
        instance.runSample();
    }

    protected String exportNewObjects(String bucketName, long duration, List<String> hosts, String uid, String token)
            throws TimeoutException, InterruptedException {

        // create a request object and populate with parameters
        SetBucketFileAccessModeRequest request = new SetBucketFileAccessModeRequest();
        request.setBucketName(bucketName);
        request.setAccessMode(ViPRConstants.FileAccessMode.readWrite);
        request.setDuration(duration);
        request.setHostList(hosts);
        request.setUid(uid);
        request.setToken(token);

        // this makes the actual REST call
        BucketFileAccessModeResult result = s3.setBucketFileAccessMode(request);

        // this is the new end-token (last bucket version) of the NFS access window
        String endToken = result.getEndToken();

        // wait until change is complete (call is asynchronous)
        waitForTransition(bucketName, ViPRConstants.FileAccessMode.readWrite, 60); // max 1 minute

        return endToken;
    }

    protected void disableOldObjects(String bucketName, String token) throws TimeoutException, InterruptedException {

        // create a request object and populate with parameters
        SetBucketFileAccessModeRequest request = new SetBucketFileAccessModeRequest();
        request.setBucketName(bucketName);
        request.setAccessMode(ViPRConstants.FileAccessMode.disabled);
        request.setToken(token);

        // this makes the actual REST call
        s3.setBucketFileAccessMode(request);

        // wait until change is complete (call is asynchronous)
        waitForTransition(bucketName, null, 60); // max 1 minute
    }

    protected Map<String, List<FileAccessObject>> getNfsObjectMap(String bucketName) {

        // create a request object and populate with parameters
        GetFileAccessRequest fileAccessRequest = new GetFileAccessRequest();
        fileAccessRequest.setBucketName(bucketName);

        // this makes the actual REST call
        GetFileAccessResult fileAccessResult = s3.getFileAccess(fileAccessRequest);

        // the XML does not organize objects by export, so we'll create a map
        Map<String, List<FileAccessObject>> mountMap
                = new HashMap<String, List<FileAccessObject>>();
        for (FileAccessObject object : fileAccessResult.getObjects()) {
            List<FileAccessObject> objects = mountMap.get(object.getDeviceExport());
            if (objects == null) {
                objects = new ArrayList<FileAccessObject>();
                mountMap.put(object.getDeviceExport(), objects);
            }
            objects.add(object);
        }
        return mountMap;
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
        SampleUtils.log("Waiting for bucket mode to change...");
        long start = System.currentTimeMillis(), interval = 500;
        timeout *= 1000;
        while (true) {
            // GET the current access mode
            BucketFileAccessModeResult result = s3.getBucketFileAccessMode(bucketName);

            if (targetMode == null) {
                if (!result.getAccessMode().isTransitionState()) {
                    SampleUtils.log("Change complete; bucket is now in NFS %s mode", result.getAccessMode());
                    return; // must be complete since the bucket is not in a transition state
                }
            } else {
                if (targetMode == result.getAccessMode()) {
                    SampleUtils.log("Change complete; bucket is now in NFS %s mode", targetMode);
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
}
