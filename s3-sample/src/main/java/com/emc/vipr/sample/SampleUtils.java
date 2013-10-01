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
