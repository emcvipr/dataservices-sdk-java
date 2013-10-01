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
package com.emc.atmos.util;

import com.emc.atmos.api.AtmosApi;
import com.emc.atmos.api.AtmosConfig;
import com.emc.atmos.api.jersey.AtmosApiClient;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * This class looks on the classpath for a file named atmos.properties and uses it to
 * configure a connection to Atmos.  The supported properties are:
 * <dt>
 * <dl>atmos.uid</dl><dd>(Required) The full token ID</dd>
 * <dl>atmos.secret</dl><dd>(Required) The shared secret key</dd>
 * <dl>atmos.endpoints</dl><dd>(Required) Comma-delimited list of endpoint URIs</dd>
 * <dl>atmos.proxyUrl</dl><dd>(Optional) proxy to use</dd>
 * </dt>
 *
 * @author cwikj
 */
public class AtmosClientFactory {
    public static final String ATMOS_PROPERTIES_FILE = "atmos.properties";

    public static final String PROP_UID = "atmos.uid";
    public static final String PROP_SECRET = "atmos.secret";
    public static final String PROP_ENDPOINTS = "atmos.endpoints";
    public static final String PROP_PROXY = "atmos.proxyUrl";

    private static Properties properties;

    /**
     * Locates and loads the properties file for the test configuration.  This file can
     * reside in one of two places: somewhere in the CLASSPATH or in the user's home
     * directory.
     *
     * @return the contents of the properties file as a {@link java.util.Properties} object.
     * @throws java.io.FileNotFoundException if the file was not found
     * @throws java.io.IOException           if there was an error reading the file.
     */
    public static synchronized Properties getProperties() throws IOException {
        if (properties != null) return properties;

        InputStream in = AtmosClientFactory.class.getClassLoader().getResourceAsStream(ATMOS_PROPERTIES_FILE);
        if (in == null) {
            // Check in home directory
            File homeProps = new File(System.getProperty("user.home") + File.separator + ATMOS_PROPERTIES_FILE);
            if (homeProps.exists()) {
                in = new FileInputStream(homeProps);
            } else {
                throw new FileNotFoundException(ATMOS_PROPERTIES_FILE);
            }
        }

        properties = new Properties();
        properties.load(in);
        in.close();

        return properties;
    }

    public static AtmosApi getAtmosClient() {
        return new AtmosApiClient(getAtmosConfig());
    }

    public static AtmosConfig getAtmosConfig() {
        try {
            Properties props = getProperties();

            String uid = getPropertyNotEmpty(props, PROP_UID);
            String secret = getPropertyNotEmpty(props, PROP_SECRET);
            String endpoints = getPropertyNotEmpty(props, PROP_ENDPOINTS);
            String proxyUrl = props.getProperty(PROP_PROXY);

            List<URI> endpointUris = new ArrayList<URI>();
            for (String endpoint : endpoints.split(",")) {
                endpointUris.add(new URI(endpoint));
            }

            AtmosConfig config = new AtmosConfig(uid, secret, endpointUris.toArray(new URI[endpointUris.size()]));
            if (proxyUrl != null) config.setProxyUri(new URI(proxyUrl));

            return config;
        } catch (IOException e) {
            throw new RuntimeException("Could not load properties file", e);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Invalid endpoint or proxy URI", e);
        }
    }

    private static String getPropertyNotEmpty(Properties p, String key) {
        String value = p.getProperty(key);
        if (value == null || value.isEmpty()) {
            throw new RuntimeException(String.format("The property %s is required", key));
        }
        return value;
    }
}
