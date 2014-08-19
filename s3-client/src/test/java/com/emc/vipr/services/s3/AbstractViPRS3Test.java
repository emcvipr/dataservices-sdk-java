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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.emc.test.util.Concurrent;
import com.emc.test.util.ConcurrentJunitRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

@RunWith(ConcurrentJunitRunner.class)
@Concurrent
public abstract class AbstractViPRS3Test {
    private static final Log l4j = LogFactory.getLog(AbstractViPRS3Test.class);
    private static AtomicInteger counter = new AtomicInteger(0);
    private ThreadLocal<String> testBucket = new ThreadLocal<String>();

    protected AmazonS3 s3;
    protected ViPRS3 viprS3;

    /**
     * Override to provide a different bucket prefix for each subclass.
     */
    protected String getTestBucketPrefix() {
        return "basic-s3-tests";
    }

    protected final String getTestBucket() {
        return testBucket.get();
    }

    @Before
    public final void setUp() throws Exception {
        initS3();
        testBucket.set(getTestBucketPrefix() + "-" + counter.incrementAndGet());
        createBucket();
    }

    protected void initS3() throws Exception {
        s3 = S3ClientFactory.getS3Client();
        Assume.assumeTrue("Could not configure S3 connection", s3 != null);
        viprS3 = (ViPRS3) s3;
    }

    protected void createBucket() throws Exception {
        try {
            s3.createBucket(getTestBucket());
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() != 409) { // Ignore 409 (bucket exists)
                throw e;
            }
        }
    }

    @After
    public final void tearDown() throws Exception {
        if (s3 == null) {
            return;
        }
        cleanUpBucket();
    }

    protected void cleanUpBucket() throws Exception {
        l4j.warn("cleaning up bucket " + getTestBucket());
        for (S3ObjectSummary summary : s3.listObjects(getTestBucket()).getObjectSummaries()) {
            s3.deleteObject(summary.getBucketName(), summary.getKey());
        }
        s3.deleteBucket(getTestBucket());
    }
}
