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

public class UpdateObject {
    private ViPRS3Client s3;

    public UpdateObject() {
        this.s3 = S3ClientFactory.getS3Client();
    }

    public void runSample() throws Exception {
        String bucketName = "test.vipr-update-object";
        String key = "temp1.txt";
        String content = "Hello World!";
        int worldIndex = content.indexOf("World");

        try {
            // first create an object
            SampleUtils.log("creating object %s/%s with content: %s",
                    bucketName, key, content);
            s3.createBucket(bucketName);
            s3.putObject(bucketName, key, new StringInputStream(content), null);

            // now let's update the object in the middle
            String content2 = "Universe!";
            SampleUtils.log(
                    "Updating object at offset %d with new content: %s",
                    worldIndex, content2);
            s3.updateObject(bucketName, key, new StringInputStream(content2),
                    null, worldIndex);

            content /* now */= "Hello Universe!";

            // now let's append to the object
            String content3 = " ... and Bob!!";
            SampleUtils
                    .log("Updating (appending) object at offset %d with new content: %s",
                            content.length(), content3);
            s3.updateObject(bucketName, key, new StringInputStream(content3),
                    null, content.length());

            // now let's create a sparse object by appending past the end of the
            // object
            String content4 = "hidden track!!!";
            SampleUtils
                    .log("Updating (sparse append) object at offset %d with new content: %s",
                            1000, content4);
            s3.updateObject(bucketName, key, new StringInputStream(content4),
                    null, 1000);

            // just for kicks, let's check the new size of the object
            int expected = 1000 + content4.length();
            long actual = s3.getObjectMetadata(bucketName, key)
                    .getContentLength();
            SampleUtils.log("Expected new size: %d, actual size: %d", expected,
                    actual);
        } finally {
            SampleUtils.cleanBucket(s3, bucketName);
        }
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        UpdateObject instance = new UpdateObject();
        instance.runSample();
    }
}
