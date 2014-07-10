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
import com.amazonaws.services.s3.model.S3Object;
import org.junit.Assume;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;

/**
 * Test using the S3 Encryption Client with ViPR.
 */
public class S3EncryptionClientTest extends AbstractViPRS3Test {
    @Override
    protected String getTestBucketPrefix() {
        return "basic-s3-encryption-tests";
    }

    @Override
    protected void initS3() throws Exception{
        s3 = S3ClientFactory.getEncryptionClient();
        Assume.assumeTrue("Could not configure S3 connection", s3 != null);
    }
    
    @Test
    public void testPutDeleteObjectEncrypted() throws Exception {
        String key = "testkey";
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
}
