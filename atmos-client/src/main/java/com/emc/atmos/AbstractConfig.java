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
package com.emc.atmos;

import org.apache.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Holds configuration parameters common between Atmos data and mgmt APIs.
 * <p/>
 * If multiple endpoints are provided, they will be balanced (round-robin style) between threads and each thread will
 * be assigned its own endpoint to avoid MDS sync issues.
 */
public class AbstractConfig {
    private static final Logger l4j = Logger.getLogger( AbstractConfig.class );

    protected String context;
    protected URI[] endpoints;
    protected boolean disableSslValidation = false;
    protected int resolveCount = 0;
    protected LoadBalancingAlgorithm loadBalancingAlgorithm = new RoundRobinAlgorithm();
    protected ThreadLocal<URI> threadEndpoint = new ThreadLocal<URI>();

    public AbstractConfig( String context, URI... endpoints ) {
        this.context = context;
        this.endpoints = endpoints;
    }

    /**
     * Resolves a path relative to the API context. The returned URI will be of the format
     * scheme://host[:port]/context/relativePath?query. The scheme, host and port (endpoint) to use is delegated to the
     * configured loadBalancingAlgorithm to balance load across multiple endpoints.
     */
    public URI resolvePath( String relativePath, String query ) {
        String path = relativePath;

        // make sure we have a root path
        if ( path.length() == 0 || path.charAt( 0 ) != '/' ) path = '/' + path;

        // don't add the context if it's already there
        if ( !path.startsWith( context ) ) path = context + path;

        URI endpoint = loadBalancingAlgorithm.getNextEndpoint( endpoints );

        try {
            URI uri = new URI( endpoint.getScheme(), null, endpoint.getHost(), endpoint.getPort(),
                               path, query, null );
            l4j.debug( "raw path & query: " + path + "?" + query );
            l4j.debug( "encoded URI: " + uri );
            return uri;
        } catch ( URISyntaxException e ) {
            throw new RuntimeException( "Invalid URI syntax", e );
        }
    }

    /**
     * Returns the base API context (i.e. "/rest" for the Atmos data API).
     */
    public String getContext() {
        return context;
    }

    /**
     * Sets the base API context (i.e. "/rest" for the Atmos data API).
     */
    public void setContext( String context ) {
        this.context = context;
    }

    /**
     * Returns whether SSL validation should be disabled (allowing self-signed certificates to be used for https
     * requests).
     */
    public boolean isDisableSslValidation() {
        return disableSslValidation;
    }

    /**
     * Sets whether SSL validation should be disabled (allowing self-signed certificates to be used for https
     * requests).
     */
    public void setDisableSslValidation( boolean disableSslValidation ) {
        this.disableSslValidation = disableSslValidation;
    }

    /**
     * Returns the configured endpoints.
     */
    public URI[] getEndpoints() {
        return endpoints;
    }

    /**
     * Sets the configured endpoints. These URIs should be of the format scheme://host[:port]. They should not contain
     * a path or query.
     */
    public void setEndpoints( URI[] endpoints ) {
        this.endpoints = endpoints;
    }

    /**
     * Returns the load balancing algorithm implementation used to distribute requests between multiple endpoints.
     */
    public LoadBalancingAlgorithm getLoadBalancingAlgorithm() {
        return loadBalancingAlgorithm;
    }

    /**
     * Sets the load balancing algorithm implementation used to distribute requests between multiple endpoints.
     */
    public void setLoadBalancingAlgorithm( LoadBalancingAlgorithm loadBalancingAlgorithm ) {
        this.loadBalancingAlgorithm = loadBalancingAlgorithm;
    }
}
