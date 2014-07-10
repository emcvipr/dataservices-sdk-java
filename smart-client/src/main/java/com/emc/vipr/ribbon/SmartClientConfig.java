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
package com.emc.vipr.ribbon;

import com.netflix.loadbalancer.Server;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Houses configuration for the smart client.
 */
public class SmartClientConfig {
    public static int DEFAULT_POLL_INTERVAL = 120; // seconds
    public static int DEFAULT_TIMEOUT = 5000; // ms

    private String vipAddresses;
    private List<Server> initialNodes;
    private String user;
    private String secret;
    private String pollProtocol;
    private int pollInterval = DEFAULT_POLL_INTERVAL;
    private int timeout = DEFAULT_TIMEOUT;

    public String getVipAddresses() {
        return vipAddresses;
    }

    /**
     * Set the comma-separated list of VIP addresses (hosts and ports) for load balancing. When a request uses one of
     * these addresses, it will be replaced by the load balancer with a selected host (data services node) from the LB's
     * host list.
     */
    public void setVipAddresses(String vipAddresses) {
        this.vipAddresses = vipAddresses;
    }

    public List<Server> getInitialNodes() {
        return initialNodes;
    }

    public String getInitialNodesString() {
        if (initialNodes == null) return "";
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < initialNodes.size(); i++) {
            if (i > 0) str.append(",");
            str.append(initialNodes.get(i));
        }
        return str.toString();
    }

    /**
     * Set the initial list of data services nodes in the ViPR cluster. These nodes will be queried at regular intervals
     * to get the full current list of active nodes.
     */
    public void setInitialNodes(List<Server> initialNodes) {
        this.initialNodes = initialNodes;
    }

    public String getUser() {
        return user;
    }

    /**
     * Set the user to use when querying for the full list of active nodes
     */
    public void setUser(String user) {
        this.user = user;
    }

    public String getSecret() {
        return secret;
    }

    /**
     * Set the user's secret key when querying nodes
     */
    public void setSecret(String secret) {
        this.secret = secret;
    }

    public String getPollProtocol() {
        return pollProtocol;
    }

    /**
     * Sets the protocol to use when polling for active nodes (http or https).
     */
    public void setPollProtocol(String pollProtocol) {
        this.pollProtocol = pollProtocol;
    }

    public int getPollInterval() {
        return pollInterval;
    }

    /**
     * Set the interval in seconds to wait between queries for active nodes. Defaults to 120 seconds (2 minutes).
     */
    public void setPollInterval(int pollInterval) {
        this.pollInterval = pollInterval;
    }

    public int getTimeout() {
        return timeout;
    }

    /**
     * Set the timeout threshold (in milliseconds) when querying for active nodes. Defaults to 5000ms (5 seconds).
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    /**
     * @see #setVipAddresses(String)
     */
    public SmartClientConfig withVipAddresses(String vipAddresses) {
        setVipAddresses(vipAddresses);
        return this;
    }

    /**
     * @see #setInitialNodes(java.util.List)
     */
    public SmartClientConfig withInitialNodes(Server... initialNodes) {
        setInitialNodes(Arrays.asList(initialNodes));
        return this;
    }

    /**
     * @see #setInitialNodes(java.util.List)
     */
    public SmartClientConfig withInitialNodes(String listString) {
        setInitialNodes(parseServerList(listString));
        return this;
    }

    /**
     * @see #setInitialNodes(java.util.List)
     */
    public SmartClientConfig withInitialNode(Server initialNode) {
        setInitialNodes(Arrays.asList(initialNode));
        return this;
    }

    /**
     * @see #setUser(String)
     */
    public SmartClientConfig withUsername(String username) {
        setUser(username);
        return this;
    }

    /**
     * @see #setSecret(String)
     */
    public SmartClientConfig withSecret(String secret) {
        setSecret(secret);
        return this;
    }

    /**
     * @see #setPollProtocol(String)
     */
    public SmartClientConfig withPollProtocol(String pollProtocol) {
        setPollProtocol(pollProtocol);
        return this;
    }

    /**
     * @see #setPollInterval(int)
     */
    public SmartClientConfig withPollInterval(int pollInterval) {
        setPollInterval(pollInterval);
        return this;
    }

    /**
     * @see #setTimeout(int)
     */
    public SmartClientConfig withTimeout(int timeout) {
        setTimeout(timeout);
        return this;
    }

    public static List<Server> parseServerList(String listString) {
        List<Server> serverList = new ArrayList<Server>();
        for (String serverStr : listString.split(",")) {
            String[] parts = serverStr.split(":");
            if (parts.length != 2)
                throw new IllegalArgumentException("Invalid server (must be in host:port format): " + serverStr);
            serverList.add(new Server(parts[0], Integer.parseInt(parts[1])));
        }
        return serverList;
    }
}
