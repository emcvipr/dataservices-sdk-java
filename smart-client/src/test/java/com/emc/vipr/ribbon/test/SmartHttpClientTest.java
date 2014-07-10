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
package com.emc.vipr.ribbon.test;

import com.emc.vipr.ribbon.SmartClientConfig;
import com.emc.vipr.ribbon.SmartHttpClient;
import com.netflix.loadbalancer.BaseLoadBalancer;
import com.netflix.loadbalancer.LoadBalancerStats;
import com.netflix.loadbalancer.Server;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.*;

public class SmartHttpClientTest {
    @Test
    public void testRibbonClient() throws Exception {
        int totalRequests = 20;
        String vipAddress = "www.foo.com";
        String serverString = "www.wikipedia.org:80,www.time.gov:80,www.bing.com:80,api.atmosonline.com:80";
        List<Server> servers = SmartClientConfig.parseServerList(serverString);

        String clientName = "testRibbonClient";
        SmartHttpClient client = new SmartHttpClient(clientName,
                new SmartClientConfig().withInitialNodes(servers.toArray(new Server[servers.size()]))
                        .withVipAddresses(vipAddress).withPollProtocol("http")
        );

        // fire off a number of requests
        for (int i = 0; i < totalRequests; i++) {
            HttpEntity e = client.execute(new HttpGet("http://" + vipAddress + "/")).getEntity();
            if (e != null) readAndClose(e.getContent());
        }
        BaseLoadBalancer lb = (BaseLoadBalancer) client.getLoadBalancer();
        LoadBalancerStats lbStats = lb.getLoadBalancerStats();
        assertNull("No requests should go to " + vipAddress, lbStats.getServerStats().get(new Server(vipAddress, 80)));
        int requestCount = 0;
        for (Server server : servers) {
            requestCount += lbStats.getSingleServerStat(server).getTotalRequestsCount();
            assertTrue("At least one request should go to " + server, lbStats.getSingleServerStat(server).getTotalRequestsCount() > 0);
        }
        assertEquals("Total requests should be " + totalRequests, totalRequests, requestCount);
    }

    private void readAndClose(InputStream is) throws IOException {
        byte[] buffer = new byte[4096];
        int read = 0;
        while (read >= 0) {
            read = is.read(buffer);
        }
        is.close();
    }
}
