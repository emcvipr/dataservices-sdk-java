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

import org.junit.Assume;

public class NamespaceTest extends BasicS3Test {
    @Override
    protected String getTestBucketPrefix() {
        return "basic-s3-namespace-tests";
    }

    @Override
    protected void initS3() throws Exception {
        s3 = S3ClientFactory.getS3Client(true);
        Assume.assumeTrue("Could not configure S3 connection", s3 != null);
        viprS3 = (ViPRS3) s3;
    }
}
