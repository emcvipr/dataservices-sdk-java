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

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.filter.ClientFilter;
import org.apache.commons.codec.binary.Base64;

import java.io.UnsupportedEncodingException;

public class ProxyAuthFilter extends ClientFilter {
    private String proxyUser, proxyPassword;

    public ProxyAuthFilter( String proxyUser, String proxyPassword ) {
        this.proxyUser = proxyUser;
        this.proxyPassword = proxyPassword;
    }

    @Override
    public ClientResponse handle( ClientRequest request ) throws ClientHandlerException {
        handleProxyAuth( request );

        return getNext().handle( request );
    }

    protected void handleProxyAuth( ClientRequest request ) {
        if ( proxyUser != null && proxyUser.length() > 0 ) {
            String userPass = proxyUser + ":" + ((proxyPassword == null) ? "null" : proxyPassword);

            String userPass64;
            try {
                userPass64 = Base64.encodeBase64String( userPass.getBytes( "UTF-8" ) );
            } catch ( UnsupportedEncodingException e ) {
                userPass64 = Base64.encodeBase64String( userPass.getBytes() );
            }

            request.getHeaders().putSingle( "Proxy-Authorization", "Basic " + userPass64 );
        }
    }
}
