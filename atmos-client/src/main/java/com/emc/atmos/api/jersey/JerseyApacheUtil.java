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
package com.emc.atmos.api.jersey;

import com.emc.atmos.AtmosException;
import com.emc.atmos.api.AtmosConfig;
import com.emc.util.SslUtil;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.client.apache4.ApacheHttpClient4;
import com.sun.jersey.client.apache4.config.ApacheHttpClient4Config;
import com.sun.jersey.client.apache4.config.DefaultApacheHttpClient4Config;
import org.apache.http.client.params.AllClientPNames;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.AbstractHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.HttpParams;
import org.apache.http.params.SyncBasicHttpParams;

import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

public class JerseyApacheUtil {
    private static Pattern BOOLEAN_PATTERN = Pattern.compile("([tT][rR][uU][eE]|[fF][aA][lL][sS][eE])");
    private static Pattern DOUBLE_PATTERN = Pattern.compile("[0-9]*\\.[0-9]+");
    private static Pattern INT_PATTERN = Pattern.compile("[0-9]+");
    private static Set<String> LONG_PARAMETERS = new TreeSet<String>(Arrays.asList(
            "http.conn-manager.timeout"));

    public static Client createApacheClient(AtmosConfig config,
                                            boolean useExpect100Continue,
                                            List<Class<MessageBodyReader<?>>> readers,
                                            List<Class<MessageBodyWriter<?>>> writers) {
        try {
            ClientConfig clientConfig = new DefaultApacheHttpClient4Config();

            // make sure the apache client is thread-safe
            PoolingClientConnectionManager connectionManager = new PoolingClientConnectionManager();
            // Increase max total connection to 200
            connectionManager.setMaxTotal(200);
            // Increase default max connection per route to 200
            connectionManager.setDefaultMaxPerRoute(200);
            clientConfig.getProperties().put(DefaultApacheHttpClient4Config.PROPERTY_CONNECTION_MANAGER,
                    connectionManager);

            // register an open trust manager to allow SSL connections to servers with self-signed certificates
            if (config.isDisableSslValidation()) {
                connectionManager.getSchemeRegistry().register(
                        new Scheme("https", 443,
                                new SSLSocketFactory(SslUtil.createGullibleSslContext(),
                                        SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER)));
            }

            // set proxy configuration
            // first look in config
            URI proxyUri = config.getProxyUri();
            // then check system props
            if (proxyUri == null) {
                String host = System.getProperty("http.proxyHost");
                String portStr = System.getProperty("http.proxyPort");
                int port = (portStr != null) ? Integer.parseInt(portStr) : -1;
                if (host != null && host.length() > 0)
                    proxyUri = new URI("http", null, host, port, null, null, null);
            }
            if (proxyUri != null)
                clientConfig.getProperties().put(ApacheHttpClient4Config.PROPERTY_PROXY_URI, proxyUri);

            // set proxy auth
            // first config
            String proxyUser = config.getProxyUser(), proxyPassword = config.getProxyPassword();
            // then system props
            if (proxyUser == null) {
                proxyUser = System.getProperty("http.proxyUser");
                proxyPassword = System.getProperty("http.proxyPassword");
            }
            if (proxyUser != null && proxyUser.length() > 0) {
                clientConfig.getProperties().put(ApacheHttpClient4Config.PROPERTY_PROXY_USERNAME, proxyUser);
                clientConfig.getProperties().put(ApacheHttpClient4Config.PROPERTY_PROXY_PASSWORD, proxyPassword);
            }

            // specify whether to use Expect: 100-continue
            HttpParams httpParams = new SyncBasicHttpParams();
            DefaultHttpClient.setDefaultHttpParams(httpParams);
            httpParams.setBooleanParameter(AllClientPNames.USE_EXPECT_CONTINUE, useExpect100Continue);
            clientConfig.getProperties().put(DefaultApacheHttpClient4Config.PROPERTY_HTTP_PARAMS, httpParams);

            // pick up other configuration from system props
            for (String prop : System.getProperties().stringPropertyNames()) {
                if (prop.startsWith("http.")) {
                    // because AbstractHttpParams uses casts instead of parsing string values, we must know the
                    // parameter types ahead of time. I'm going to guess them in lieu of maintaining a map. however,
                    // we still need to define Long typed parameters so they may be differentiated from Integer typed
                    // parameters.
                    String value = System.getProperty(prop);
                    if (LONG_PARAMETERS.contains(prop))
                        httpParams.setLongParameter(prop, Long.parseLong(value.substring(0, value.length() - 1)));
                    else if (BOOLEAN_PATTERN.matcher(value).matches())
                        httpParams.setBooleanParameter(prop, Boolean.parseBoolean(value));
                    else if (DOUBLE_PATTERN.matcher(value).matches())
                        httpParams.setDoubleParameter(prop, Double.parseDouble(value));
                    else if (INT_PATTERN.matcher(value).matches())
                        httpParams.setIntParameter(prop, Integer.parseInt(value));
                    else httpParams.setParameter(prop, System.getProperty(prop));
                }
            }

            JerseyUtil.addHandlers(clientConfig, readers, writers);

            // create the client
            ApacheHttpClient4 client = ApacheHttpClient4.create(clientConfig);

            // do not use Apache's retry handler
            ((AbstractHttpClient) client.getClientHandler().getHttpClient()).setHttpRequestRetryHandler(
                    new DefaultHttpRequestRetryHandler(0, false));

            JerseyUtil.addFilters(client, config);

            return client;
        } catch (Exception e) {
            throw new AtmosException("Error configuring REST client", e);
        }
    }

}
