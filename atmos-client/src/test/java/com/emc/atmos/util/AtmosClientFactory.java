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
import com.emc.vipr.services.lib.ViprConfig;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

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
    private static final Logger l4j = Logger.getLogger(AtmosClientFactory.class);

    public static AtmosApi getAtmosClient() {
        AtmosConfig config = getAtmosConfig();
        if(config == null) {
            return null;
        }
        return new AtmosApiClient(getAtmosConfig());
    }

    public static AtmosConfig getAtmosConfig() {
        try {
            Properties props = ViprConfig.getProperties();

            String uid = ViprConfig.getPropertyNotEmpty(props, ViprConfig.PROP_ATMOS_UID);
            String secret = ViprConfig.getPropertyNotEmpty(props, ViprConfig.PROP_ATMOS_SECRET);
            String endpoints = ViprConfig.getPropertyNotEmpty(props, ViprConfig.PROP_ATMOS_ENDPOINTS);
            String proxyUrl = ViprConfig.getProxyUri(props);

            List<URI> endpointUris = new ArrayList<URI>();
            for (String endpoint : endpoints.split(",")) {
                endpointUris.add(new URI(endpoint));
            }

            AtmosConfig config = new AtmosConfig(uid, secret, endpointUris.toArray(new URI[endpointUris.size()]));
            if (proxyUrl != null) config.setProxyUri(new URI(proxyUrl));

            return config;
        } catch (IOException e) {
            l4j.info("Could not load properties file: " + e);
            return null;
        } catch (URISyntaxException e) {
           l4j.info("Invalid endpoint or proxy URI: " + e);
           return null;
        }
    }
    
    public static boolean atmosIsVipr() {
        try {
            Properties props = ViprConfig.getProperties();
            
            return Boolean.parseBoolean(props.getProperty(ViprConfig.PROP_ATMOS_IS_VIPR, "false"));
            
        } catch (IOException e) {
            l4j.info("Could not load properties file: " + e);
            return false;
        } 
    }

}
