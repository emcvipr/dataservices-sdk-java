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

import com.emc.vipr.ribbon.bean.ListDataNode;
import com.netflix.client.config.IClientConfig;
import com.netflix.loadbalancer.AbstractServerList;
import com.netflix.loadbalancer.Server;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.*;

public class ViPRDataServicesServerList extends AbstractServerList<Server> {
    private static final Logger logger = LoggerFactory.getLogger(ViPRDataServicesServerList.class);

    protected final SimpleDateFormat rfc822DateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
    protected Unmarshaller unmarshaller;

    private String protocol;
    private List<Server> nodeList;
    private int port;
    private String user;
    private String secret;
    private HttpClient httpClient;
    private int requestCounter = 0;

    public ViPRDataServicesServerList() {
        rfc822DateFormat.setTimeZone(new SimpleTimeZone(0, "GMT"));
        try {
            unmarshaller = JAXBContext.newInstance(ListDataNode.class).createUnmarshaller();
        } catch (JAXBException e) {
            throw new RuntimeException("can't create unmarshaller", e);
        }
        PoolingClientConnectionManager cm = new PoolingClientConnectionManager();
        cm.setDefaultMaxPerRoute(10);
        httpClient = new DefaultHttpClient(cm);
    }

    @Override
    public void initWithNiwsConfig(IClientConfig clientConfig) {
        protocol = clientConfig.getPropertyAsString(SmartClientConfigKey.ViPRDataServicesProtocol, "").toLowerCase();
        if (!Arrays.asList("http", "https").contains(protocol))
            throw new IllegalArgumentException("Invalid protocol: " + protocol);

        String nodeStr = clientConfig.getPropertyAsString(SmartClientConfigKey.ViPRDataServicesInitialNodes, "");
        if (nodeStr.trim().length() == 0)
            throw new IllegalStateException("No servers configured in smartConfig or NIWS config");
        setNodeList(SmartClientConfig.parseServerList(nodeStr));

        // pull the port from the initial node list (it will not be returned by the list-data-nodes call)
        port = getNodeList().get(0).getPort();

        user = clientConfig.getPropertyAsString(SmartClientConfigKey.ViPRDataServicesUser, null);

        secret = clientConfig.getPropertyAsString(SmartClientConfigKey.ViPRDataServicesUserSecret, null);

        int timeout = clientConfig.getPropertyAsInteger(SmartClientConfigKey.ViPRDataServicesTimeout, SmartClientConfig.DEFAULT_TIMEOUT);
        HttpConnectionParams.setConnectionTimeout(httpClient.getParams(), timeout);
        HttpConnectionParams.setSoTimeout(httpClient.getParams(), timeout);

        if (logger.isDebugEnabled()) {
            logger.debug("Configured node enumeration");
            logger.debug("--- protocol: " + protocol);
            logger.debug("--- nodeList: " + getNodeList());
            logger.debug("--- user: " + user);
            logger.debug("--- secret: " + secret);
            logger.debug("--- timeout: " + timeout);
            logger.debug("--- httpClient: " + (httpClient != null));
        }
    }

    @Override
    public List<Server> getInitialListOfServers() {
        return getNodeList();
    }

    @Override
    public List<Server> getUpdatedListOfServers() {
        return pollForServers();
    }

    protected List<Server> pollForServers() {
        try {
            List<Server> activeNodeList = getNodeList();
            int activeNodeCount = activeNodeList.size();
            Server server = null;
            List<String> hosts = null;
            String path = "/?endpoint";

            // we want to try a different node on failure until we try every active node (HttpClient will auto-retry
            // 500s and some IOEs), but we don't want to start with the same node each time.
            for (int i = 0; i < activeNodeCount; i++) {
                try {
                    // get next server in the list (trying to distribute this call among active nodes)
                    // note: the extra modulus logic is there just in case requestCounter wraps around to a negative value
                    server = activeNodeList.get((requestCounter++ % activeNodeCount + activeNodeCount) % activeNodeCount);

                    HttpGet request = new HttpGet(protocol + "://" + server + path);
                    logger.debug("endpoint query attempt #" + (i + 1) + ": trying " + server);

                    // format date
                    String rfcDate;
                    synchronized (rfc822DateFormat) {
                        rfcDate = rfc822DateFormat.format(new Date());
                    }

                    // generate signature
                    String canonicalString = "GET\n\n\n" + rfcDate + "\n" + path;
                    String signature = getSignature(canonicalString, secret);

                    // add date and auth headers
                    request.addHeader("Date", rfcDate);
                    request.addHeader("Authorization", "AWS " + user + ":" + signature);

                    // send request
                    HttpResponse response = httpClient.execute(request);
                    if (response.getStatusLine().getStatusCode() > 299) {
                        EntityUtils.consumeQuietly(response.getEntity());
                        throw new RuntimeException("received error response: " + response.getStatusLine());
                    }

                    logger.debug("received success response: " + response.getStatusLine());
                    hosts = parseResponse(response);
                    break;
                } catch (Exception e) {
                    logger.warn("error polling for endpoints on " + server, e);
                }
            }

            if (hosts == null)
                throw new RuntimeException("Exhausted all nodes; no response available");

            List<Server> updatedNodeList = new ArrayList<Server>();
            for (String host : hosts) {
                updatedNodeList.add(new Server(host, port));
            }
            setNodeList(updatedNodeList);
        } catch (Exception e) {
            logger.warn("Unable to poll for servers", e);
        }
        return getNodeList();
    }

    protected String getSignature(String canonicalString, String secret) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA1");
        mac.init(new SecretKeySpec(secret.getBytes("UTF-8"), "HmacSHA1"));
        String signature = new String(Base64.encodeBase64(mac.doFinal(canonicalString.getBytes("UTF-8"))));
        logger.debug("canonicalString:\n" + canonicalString);
        logger.debug("signature:\n" + signature);
        return signature;
    }

    @SuppressWarnings("unchecked")
    protected List<String> parseResponse(HttpResponse response) throws IOException, JAXBException {
        InputStream contentStream = response.getEntity().getContent();
        try {
            ListDataNode listDataNode = (ListDataNode) unmarshaller.unmarshal(contentStream);

            List<String> hosts = new ArrayList<String>();
            for (String host : listDataNode.getDataNodes()) {
                hosts.add(host.trim());
            }
            return hosts;
        } finally {
            try {
                contentStream.close();
            } catch (RuntimeException e) {
                logger.warn("error closing HTTP content stream", e);
            }
        }
    }

    protected List<Server> getNodeList() {
        return nodeList;
    }

    protected synchronized void setNodeList(List<Server> nodeList) {
        this.nodeList = Collections.unmodifiableList(nodeList);
    }
}
