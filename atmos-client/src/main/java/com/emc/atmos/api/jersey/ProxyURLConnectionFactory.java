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

import com.sun.jersey.client.urlconnection.HttpURLConnectionFactory;

import java.io.IOException;
import java.net.*;

public class ProxyURLConnectionFactory implements HttpURLConnectionFactory {
    private Proxy proxy;
    private PasswordAuthentication authentication;

    public ProxyURLConnectionFactory( URI proxyUri, String proxyUser, String proxyPassword ) {
        this.proxy = new Proxy( Proxy.Type.HTTP, new InetSocketAddress( proxyUri.getHost(), proxyUri.getPort() ) );
        if ( proxyUser != null ) {
            this.authentication = new PasswordAuthentication( proxyUser,
                                                              proxyPassword == null
                                                              ? new char[]{}
                                                              : proxyPassword.toCharArray() );
            Authenticator.setDefault( new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    return authentication;
                }
            } );
        }
    }

    @Override
    public HttpURLConnection getHttpURLConnection( URL url ) throws IOException {
        return (HttpURLConnection) url.openConnection( proxy );
    }
}
