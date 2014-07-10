/*
 * Copyright 2014 EMC Corporation. All Rights Reserved.
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

import com.emc.vipr.services.lib.ViprConfig;
import com.emc.vipr.services.s3.model.ViPRCreateBucketRequest;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class FileSystemEnabledTest extends AbstractViPRS3Test {
    @Override
    protected String getTestBucketPrefix() {
        return "fs-access-enabled-tests";
    }

    @Before
    public void checkEnabled() throws Exception {
        Assume.assumeFalse("false".equals(ViprConfig.getProperties().getProperty(ViprConfig.PROP_FILE_ACCESS_TESTS_ENABLED, "true")));
    }

    /**
     * Note, this does not currently test much.. just that the header is sent (via proxy sniffing).
     * There is no way to verify it had any effect and at this time we are not adding an HDFS client as a dependency.
     */
    @Test
    public void testFileSystemAccessEnabled() {
        ViPRCreateBucketRequest request = new ViPRCreateBucketRequest(getTestBucket());
        request.setFsAccessEnabled(true);
        s3.createBucket(request);

        s3.deleteBucket(getTestBucket());
    }
}
