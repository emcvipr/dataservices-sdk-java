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

import java.net.URI;

/**
 * This implementation will tie a specific endpoint to each thread to avoid MDS sync issues. However, multiple threads
 * will be distributed between the configured endpoints.
 */
public class StickyThreadAlgorithm implements LoadBalancingAlgorithm {
    protected ThreadLocal<URI> threadEndpoint = new ThreadLocal<URI>();
    protected int callCount = 0;

    @Override
    public URI getNextEndpoint( URI[] endpoints ) {
        // tie the endpoint to the current thread to eliminate MDS sync issues when using multiple endpoints
        URI endpoint = threadEndpoint.get();
        if ( endpoint == null ) {
            endpoint = endpoints[callCount++ % endpoints.length];
            threadEndpoint.set( endpoint );
        }
        return endpoint;
    }
}
