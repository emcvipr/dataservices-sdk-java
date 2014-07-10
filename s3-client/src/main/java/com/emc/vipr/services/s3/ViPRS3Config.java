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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.emc.vipr.ribbon.SmartClientConfig;
import com.netflix.loadbalancer.Server;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * class to house configuration elements for a ViPR S3 client.
 */
public class ViPRS3Config {
    private Protocol protocol = Protocol.HTTP;
    private List<URI> s3Endpoints;
    private String vipHost = "s3.amazonaws.com";
    private AWSCredentialsProvider credentialsProvider;
    private ClientConfiguration clientConfiguration;
    private int pollInterval; // seconds
    private int timeout; // ms

    public Protocol getProtocol() {
        return protocol;
    }

    /**
     * Set the protocol that will be used for connections (HTTP or HTTPS)
     */
    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public List<URI> getS3Endpoints() {
        return s3Endpoints;
    }

    /**
     * Set the initial set of S3 data endpoints (data services nodes). This list of nodes will automatically be updated
     * by the client.
     */
    public void setS3Endpoints(List<URI> s3Endpoints) {
        this.s3Endpoints = s3Endpoints;
    }

    public String getVipHost() {
        return vipHost;
    }

    public AWSCredentialsProvider getCredentialsProvider() {
        return credentialsProvider;
    }

    /**
     * Set the S3 credentials (access key and secret key)
     */
    public void setCredentialsProvider(AWSCredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
    }

    public synchronized ClientConfiguration getClientConfiguration() {
        if (clientConfiguration == null) {
            clientConfiguration = new ClientConfiguration();
        }
        return clientConfiguration;
    }

    /**
     * Set specific AWS client configuration parameters
     */
    public void setClientConfiguration(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = clientConfiguration;
    }

    public int getPollInterval() {
        return pollInterval;
    }

    /**
     * Set the delay in seconds between polls to discover data nodes
     */
    public void setPollInterval(int pollInterval) {
        this.pollInterval = pollInterval;
    }

    public int getTimeout() {
        return timeout;
    }

    /**
     * Set the timeout for discovery polls. If a timeout is reached, other servers will be tried until the list
     * is exhausted, in which case the data node list will not be updated until the next poll interval.
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public ViPRS3Config withProtocol(Protocol protocol) {
        setProtocol(protocol);
        return this;
    }

    public ViPRS3Config withS3Endpoints(URI... s3Endpoints) {
        setS3Endpoints(Arrays.asList(s3Endpoints));
        return this;
    }

    public ViPRS3Config withS3Endpoints(String s3EndpointsString) {
        try {
            List<URI> endpoints = new ArrayList<URI>();
            String[] uris = s3EndpointsString.split(",");
            for (String uri : uris) {
                endpoints.add(new URI(uri.trim()));
            }
            setS3Endpoints(endpoints);
            return this;
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid endpoint URL", e);
        }
    }

    public ViPRS3Config withCredentialsProvider(AWSCredentialsProvider credentialsProvider) {
        setCredentialsProvider(credentialsProvider);
        return this;
    }

    public ViPRS3Config withClientConfiguration(ClientConfiguration clientConfiguration) {
        setClientConfiguration(clientConfiguration);
        return this;
    }

    public ViPRS3Config withPollInterval(int pollInterval) {
        setPollInterval(pollInterval);
        return this;
    }

    public ViPRS3Config withTimeout(int timeout) {
        setTimeout(timeout);
        return this;
    }

    public SmartClientConfig toSmartClientConfig() {
        SmartClientConfig smartConfig = new SmartClientConfig().withInitialNodes(toServers(s3Endpoints)).withVipAddresses(vipHost)
                .withPollProtocol(protocol.toString())
                .withUsername(credentialsProvider.getCredentials().getAWSAccessKeyId())
                .withSecret(credentialsProvider.getCredentials().getAWSSecretKey());
        if (pollInterval > 0) smartConfig.setPollInterval(pollInterval);
        if (timeout > 0) smartConfig.setTimeout(timeout);
        return smartConfig;
    }

    protected Server[] toServers(List<URI> uris) {
        List<Server> servers = new ArrayList<Server>();
        for (URI uri : uris) {
            int port = uri.getPort();
            if (port == -1) port = (uri.getScheme().equalsIgnoreCase("https") ? 443 : 80);
            servers.add(new Server(uri.getHost(), port));
        }
        return servers.toArray(new Server[servers.size()]);
    }
}
