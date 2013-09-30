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

import com.emc.acdp.AcdpException;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.filter.ClientFilter;
import org.apache.log4j.Logger;
import org.jdom.Document;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;

public class ErrorFilter extends ClientFilter {
    private static final Logger log = Logger.getLogger( ErrorFilter.class );

    @Override
    public ClientResponse handle( ClientRequest clientRequest ) throws ClientHandlerException {
        ClientResponse response = getNext().handle( clientRequest );

        if ( response.getStatus() > 299 ) {

            // JAXB will expect a namespace if we try to unmarshall, but some error responses don't include
            // a namespace. In lieu of writing a SAXFilter to apply a default namespace in-line, this works just as well.
            SAXBuilder sb = new SAXBuilder();

            Document d = null;
            try {
                d = sb.build( response.getEntityInputStream() );
            } catch ( Exception e ) {
                throw new AcdpException( response.getClientResponseStatus().getReasonPhrase(), response.getStatus() );
            }

            String code = d.getRootElement().getChildText( "code" );
            if ( code == null )
                code = d.getRootElement()
                        .getChildText( "code", Namespace.getNamespace( "http://cdp.emc.com/services/rest/model" ) );
            String message = d.getRootElement().getChildText( "message" );
            if ( message == null )
                message = d.getRootElement()
                           .getChildText( "message",
                                          Namespace.getNamespace( "http://cdp.emc.com/services/rest/model" ) );

            if ( code == null && message == null ) {
                // not an error from CDP
                throw new AcdpException( response.getClientResponseStatus().getReasonPhrase(), response.getStatus() );
            }

            log.debug( "Error: " + code + " message: " + message );
            throw new AcdpException( message, response.getStatus(), code );
        }

        return response;
    }
}
