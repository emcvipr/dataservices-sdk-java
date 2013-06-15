package com.emc.vipr.sample;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class SampleUtils {
    private static final Log log = LogFactory.getLog(SampleUtils.class);

    static void cleanBucket(AmazonS3 s3, String bucketName) {
        try {
            for (S3ObjectSummary summary : s3.listObjects(bucketName)
                    .getObjectSummaries()) {
                s3.deleteObject(bucketName, summary.getKey());
            }
            s3.deleteBucket(bucketName);
        } catch (Exception e) {
            // don't cause tests to fail
            log.warn(String.format("Could not clean up bucket %s", bucketName),
                    e);
        }
    }

    static void log(String msg, Object... params) {
        System.out.println(String.format(msg, params));
    }
}
