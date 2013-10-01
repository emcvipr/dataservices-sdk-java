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

import com.emc.acdp.api.AcdpMgmtApi;
import com.emc.acdp.api.AcdpMgmtConfig;
import com.emc.cdp.services.rest.model.Identity;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

public class AcdpMgmtApiClient implements AcdpMgmtApi {
    private AcdpMgmtConfig config;
    private Client client;

    public AcdpMgmtApiClient( AcdpMgmtConfig config ) {
        this.config = config;
        this.client = JerseyUtil.createClient( config );
    }

    /**
     * Note that this constructor cannot disable SSL validation, so that configuration option is ignored here. You are
     * responsible for configuring the client with any proxy, ssl or other options prior to calling this constructor.
     */
    public AcdpMgmtApiClient( AcdpMgmtConfig config, Client client ) {
        this.config = config;
        JerseyUtil.configureClient( client, config );
        this.client = client;
    }

    @Override
    public void createIdentity( Identity identity ) {
        WebResource resource = client.resource( getMgmtUri() + "/identities" );
        resource.type( MediaType.TEXT_XML ).post( identity );
    }

    @Override
    public void createAccount( String serviceId ) {
        WebResource.Builder builder = client.resource( getMgmtUri() + "/accounts" ).getRequestBuilder();

        MultivaluedMap<String, String> params = new MultivaluedMapImpl();
        params.putSingle( "serviceId", serviceId );

        builder.type( MediaType.APPLICATION_FORM_URLENCODED ).post( params );
    }

    private String getMgmtUri() {
        return config.getBaseUri() + "/cdp-rest/v1";
    }
}
