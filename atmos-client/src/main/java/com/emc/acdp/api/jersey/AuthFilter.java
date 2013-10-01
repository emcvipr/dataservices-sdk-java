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
package com.emc.acdp.api.jersey;

import com.emc.acdp.AcdpConfig;
import com.emc.acdp.AcdpException;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.filter.ClientFilter;
import com.sun.jersey.core.util.MultivaluedMapImpl;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import java.net.URI;
import java.net.URISyntaxException;

public class AuthFilter extends ClientFilter {
    private static final String PARAM_SESSION_TOKEN = "cdp_session";
    private static final String PARAM_USER_ID = "cdp-identity-id";
    private static final String PARAM_PASSWORD = "cdp-password";

    private AcdpConfig config;

    public AuthFilter( AcdpConfig config ) {
        this.config = config;
    }

    @Override
    public ClientResponse handle( ClientRequest request ) throws ClientHandlerException {
        if ( !config.isSecureRequest( request.getURI().getPath(), request.getMethod() ) )
            return getNext().handle( request );

        if ( config.getSessionToken() == null ) {

            // must login
            login( request );
        } else {
            attachSessionToken( request );
        }

        ClientResponse response;
        try {
            response = getNext().handle( request );

            // if unauthorized, try one more time after logging in
        } catch ( AcdpException e ) {
            if ( e.getHttpCode() == 401 ) {
                login( request );

                response = getNext().handle( (request) );
            } else {
                throw e;
            }
        }

        return response;
    }

    private void attachSessionToken( ClientRequest request ) {

        // append session token to query in URI
        String uriStr = request.getURI().toString();

        URI uri = request.getURI();
        if ( uri.getQuery() != null && uri.getQuery().length() > 0 )
            uriStr += "&";
        else
            uriStr += "?";

        uriStr += PARAM_SESSION_TOKEN + "=" + config.getSessionToken();

        try {
            request.setURI( new URI( uriStr ) );
        } catch ( URISyntaxException e ) {
            throw new RuntimeException( e );
        }
    }

    private void login( ClientRequest request ) {

        // hold existing request configuration (we can't create a new request here)
        String holdMethod = request.getMethod();
        URI holdUri = request.getURI();
        Object holdEntity = request.getEntity();
        Object holdType = request.getHeaders().getFirst( HttpHeaders.CONTENT_TYPE );

        // login
        MultivaluedMap<String, String> params = new MultivaluedMapImpl();
        params.putSingle( PARAM_USER_ID, config.getUsername() );
        params.putSingle( PARAM_PASSWORD, config.getPassword() );

        request.setMethod( HttpMethod.POST );
        request.setURI( request.getURI().resolve( config.getLoginPath() ) );
        request.setEntity( params );
        request.getHeaders().putSingle( HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_TYPE );

        ClientResponse response = getNext().handle( request );

        // get token from response
        String token = response.getEntity( String.class );
        //String token = new java.util.Scanner( response.getEntityInputStream(), "UTF-8" ).useDelimiter( "\\A" ).next();
        config.setSessionToken( token );

        // reset initial request configuration
        request.setMethod( holdMethod );
        request.setURI( holdUri );
        request.setEntity( holdEntity );
        request.getHeaders().putSingle( HttpHeaders.CONTENT_TYPE, holdType );

        attachSessionToken( request );
    }
}
