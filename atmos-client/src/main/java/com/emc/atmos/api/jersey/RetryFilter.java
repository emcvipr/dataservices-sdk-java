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

import com.emc.atmos.AtmosException;
import com.emc.atmos.api.AtmosConfig;
import com.emc.atmos.api.RestUtil;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.filter.ClientFilter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;

public class RetryFilter extends ClientFilter {
    private static final int ATMOS_1040_DELAY_MS = 300;

    private static final Logger log = Logger.getLogger( RetryFilter.class );

    private AtmosConfig config;

    public RetryFilter( AtmosConfig config ) {
        this.config = config;
    }

    @Override
    public ClientResponse handle( ClientRequest clientRequest ) throws ClientHandlerException {
        int retryCount = 0;
        InputStream entityStream = null;
        if ( clientRequest.getEntity() instanceof InputStream ) entityStream = (InputStream) clientRequest.getEntity();
        while ( true ) {
            try {
                // if using an InputStream, mark the stream so we can rewind it in case of an error
                if ( entityStream != null && entityStream.markSupported() )
                    entityStream.mark( config.getRetryBufferSize() );

                return getNext().handle( clientRequest );
            } catch ( RuntimeException orig ) {
                Throwable t = orig;

                // in this case, the exception was wrapped by Jersey
                if ( t instanceof ClientHandlerException ) t = t.getCause();

                if ( t instanceof AtmosException ) {
                    AtmosException ae = (AtmosException) t;

                    // retry all 50x errors
                    if ( ae.getHttpCode() < 500 ) throw orig;

                    // add small delay to Atmos code 1040 (server busy)
                    if ( ae.getErrorCode() == 1040 ) {
                        try {
                            Thread.sleep( ATMOS_1040_DELAY_MS );
                        } catch ( InterruptedException e ) {
                            log.warn( "Interrupted while waiting after a 1040 response: " + e.getMessage() );
                        }
                    }

                    // retry all IO exceptions unless wschecksum is enabled (can't overwrite data in this case)
                } else if ( !(t instanceof IOException)
                            || clientRequest.getHeaders().getFirst( RestUtil.XHEADER_WSCHECKSUM ) != null ) throw orig;

                // only retry maxRetries times
                if ( ++retryCount > config.getMaxRetries() ) throw orig;

                // attempt to reset InputStream if it has been read from
                if ( entityStream != null ) {
                    if ( !(entityStream instanceof MeasuredInputStream)
                         || ((MeasuredInputStream) entityStream).getRead() > 0 ) {
                        try {
                            if ( !entityStream.markSupported() ) throw new IOException( "Mark is not supported" );
                            entityStream.reset();
                        } catch ( IOException e ) {
                            log.warn( "Could not reset entity stream for retry: " + e.getMessage() );
                            throw orig;
                        }
                    }
                }

                log.info( "Error received in response (" + t + "), retrying..." );

                // wait for retry delay
                if ( config.getRetryDelayMillis() > 0 ) {
                    try {
                        Thread.sleep( config.getRetryDelayMillis() );
                    } catch ( InterruptedException e ) {
                        log.warn( "Interrupted while waiting to retry: " + e.getMessage() );
                    }
                }
            }
        }
    }
}
