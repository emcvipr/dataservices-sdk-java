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
package com.emc.vipr.services.s3.smart;

import com.emc.vipr.ribbon.ViPRDataServicesServerList;
import com.emc.vipr.services.s3.BasicS3Test;
import com.emc.vipr.services.s3.S3ClientFactory;
import com.emc.vipr.services.s3.ViPRS3;
import com.emc.vipr.services.s3.ViPRS3Client;
import com.netflix.client.AbstractLoadBalancerAwareClient;
import com.netflix.client.ClientFactory;
import com.netflix.loadbalancer.LoadBalancerStats;
import com.netflix.loadbalancer.ZoneAwareLoadBalancer;
import org.junit.After;
import org.junit.Assume;

public class BasicS3SmartTest extends BasicS3Test {
    private static int localInstanceCounter = 0;

    @Override
    protected String getTestBucketPrefix() {
        return "basic-s3-smart-tests";
    }

    @Override
    protected void initS3() throws Exception {
        s3 = S3ClientFactory.getSmartS3Client(false);
        Assume.assumeTrue("Could not configure S3 connection", s3 != null);
        viprS3 = (ViPRS3) s3;

        // force polling for all data service nodes
        getTestBucket(); // make sure our instanceCounter is accurate
        AbstractLoadBalancerAwareClient client = (AbstractLoadBalancerAwareClient) ClientFactory.getNamedClient("ViPR.SmartHttpClient_" + ++localInstanceCounter);
        ZoneAwareLoadBalancer lb = (ZoneAwareLoadBalancer) client.getLoadBalancer();
        ViPRDataServicesServerList serverList = (ViPRDataServicesServerList) lb.getServerListImpl();
        serverList.getUpdatedListOfServers();
    }

    @After
    public void dumpLBStats() throws Exception {
        if(s3 == null) {
            return;
        }
        LoadBalancerStats lbStats = ((ViPRS3Client) viprS3).getLoadBalancerStats();
        System.out.println(lbStats);
    }
}
