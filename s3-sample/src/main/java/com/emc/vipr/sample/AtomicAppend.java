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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import com.amazonaws.util.StringInputStream;
import com.emc.vipr.services.s3.ViPRS3Client;
import com.emc.vipr.services.s3.model.AppendObjectResult;

public class AtomicAppend {
    private ViPRS3Client s3;

    public AtomicAppend() {
        this.s3 = S3ClientFactory.getS3Client();
    }

    public void runSample() throws Exception {
        String bucketName = "test.vipr-atomic-append";
        String key = "temp1.txt";
        String content = "Hello World!";

        try {
            // first create an object
            SampleUtils.log("Creating object %s/%s with content: %s",
                    bucketName, key, content);
            s3.createBucket(bucketName);
            s3.putObject(bucketName, key, new StringInputStream(content), null);

            // now we want to append to the object; we don't care exactly where
            // it is, as long as it's on the end
            String content2 = " ... and Universe!!";
            SampleUtils.log("Calling atomic append to append content: %s",
                    content2);
            AppendObjectResult result = s3.appendObject(bucketName, key,
                    new StringInputStream(content2), null);

            // returned to us is the offset at which our appended data was
            // written (this is the previous size of the object)
            long appendOffset = result.getAppendOffset();
            SampleUtils.log("Appended offset: %d", appendOffset);

            // just for kicks, lets check the final object
            InputStream input = s3.getObject(bucketName, key)
                    .getObjectContent();
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buffer = new byte[10240];
            int size = 0, read = input.read(buffer);
            while (read >= 0) {
                output.write(buffer, 0, read);
                size += read;
                read = input.read(buffer);
            }
            // check size
            SampleUtils.log("Expected new size: %d, actual size: %d",
                    (content.length() + content2.length()), size);

            // check content
            SampleUtils.log("Expected content: %s", content + content2);
            SampleUtils.log("Actual content  : %s",
                    new String(output.toByteArray()));
        } finally {
            SampleUtils.cleanBucket(s3, bucketName);
        }
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        AtomicAppend instance = new AtomicAppend();
        instance.runSample();
    }
}
