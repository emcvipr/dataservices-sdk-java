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
package com.emc.atmos.api.jersey;

import com.emc.atmos.api.AtmosConfig;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.List;

public class ConfigProxySelector extends ProxySelector {
    private AtmosConfig config;

    public ConfigProxySelector(AtmosConfig config) {
        this.config = config;
    }

    /**
     * Very simple implementation that returns NO_PROXY for localhost and the configured proxy for anything else (no
     * nonProxyHosts-like support).
     */
    @Override
    public List<Proxy> select(URI uri) {
        List<Proxy> proxies = new ArrayList<Proxy>(1);

        if ("127.0.0.1".equals(uri.getHost()) || "localhost".equals(uri.getHost()) || config.getProxyUri() == null) {
            proxies.add(Proxy.NO_PROXY);
        } else {
            URI proxyUri = config.getProxyUri();
            SocketAddress address = InetSocketAddress.createUnresolved(proxyUri.getHost(), proxyUri.getPort());
            proxies.add(new Proxy(Proxy.Type.HTTP, address));
        }

        return proxies;
    }

    @Override
    public void connectFailed(URI uri, SocketAddress socketAddress, IOException e) {
        // ignored
    }
}
