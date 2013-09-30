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
package com.emc.atmos.api;

import com.emc.atmos.AbstractConfig;
import org.apache.commons.codec.binary.Base64;

import java.io.UnsupportedEncodingException;
import java.net.URI;

/**
 * Holds configuration parameters for an AtmosApi instance.
 */
public class AtmosConfig extends AbstractConfig {
    public static final String DEFAULT_CONTEXT = "/rest";

    private String tokenId;
    private byte[] secretKey;
    private long serverClockSkew;
    private boolean enableRetry = true;
    private int retryDelayMillis = 0;
    private int maxRetries = 2;
    private int retryBufferSize = 1048576; // 1MB default
    private boolean enableExpect100Continue = true;
    private URI proxyUri;
    private String proxyUser;
    private String proxyPassword;

    /**
     * Creates a new instance with default parameters. tokenId, secretKey and at least one endpoint must be provided
     * separately. Intended for use in IoC containers such as Spring.
     */
    public AtmosConfig() {
        super( DEFAULT_CONTEXT );
    }

    /**
     * Creates a new instance with default parameters and the specified tokenId, secretKey and endpoint(s). This is the
     * minimum configuration necessary for an AtmosApi instance. The secretKey here should be base64 encoded.
     */
    public AtmosConfig( String tokenId, String secretKey, URI... endpoints ) {
        super( DEFAULT_CONTEXT, endpoints );
        setSecretKey( secretKey );
        this.tokenId = tokenId;
    }

    /**
     * Returns whether the Expect: 100-continue header is enabled for object write requests
     */
    public boolean isEnableExpect100Continue() {
        return enableExpect100Continue;
    }

    /**
     * Sets whether the Expect: 100-continue header should be used for object write requests. When enabled, all object
     * create and update requests will post the headers first (including Expect: 100-continue) and wait for a 100
     * Continue response before sending object data. This will allow Atmos to determine if the request is valid before
     * sending a potentially large payload over the network.
     * Default is true.
     */
    public void setEnableExpect100Continue( boolean enableExpect100Continue ) {
        this.enableExpect100Continue = enableExpect100Continue;
    }

    /**
     * Returns whether 500 errors and IOExceptions should be automatically retried.
     */
    public boolean isEnableRetry() {
        return enableRetry;
    }

    /**
     * Sets whether 500 errors and IOExceptions should be automatically retried. When enabled, these requests will be
     * retried up to maxRetries times with a retryDelayMillis ms delay between retry attempts.
     * Default is true.
     */
    public void setEnableRetry( boolean enableRetry ) {
        this.enableRetry = enableRetry;
    }

    /**
     * Returns the maximum number of retry attempts before bubbling the exception back to calling code.
     */
    public int getMaxRetries() {
        return maxRetries;
    }

    /**
     * Sets the maximum number of retry attempts before bubbling the exception back to calling code.
     * Default is 2 retries.
     */
    public void setMaxRetries( int maxRetries ) {
        this.maxRetries = maxRetries;
    }

    /**
     * Returns the buffer size used for non-repeatable object content (input streams) when retry is enabled.
     */
    public int getRetryBufferSize() {
        return retryBufferSize;
    }

    /**
     * Sets the buffer size used for non-repeatable object content (input streams) when retry is enabled. Once the
     * amount of data transferred surpasses this number, the request can no longer be retried. The implementation may
     * be
     * as simple as calling inputStream.mark( bufferSize ), so input streams that do not support mark might not be
     * retried.
     * Default is 1MB.
     */
    public void setRetryBufferSize( int retryBufferSize ) {
        this.retryBufferSize = retryBufferSize;
    }

    /**
     * Returns the delay in milliseconds to wait between retry attempts.
     */
    public int getRetryDelayMillis() {
        return retryDelayMillis;
    }

    /**
     * Sets the delay in milliseconds to wait between retry attempts.
     * Default is 0 (no wait).
     */
    public void setRetryDelayMillis( int retryDelayMillis ) {
        this.retryDelayMillis = retryDelayMillis;
    }

    /**
     * Returns the secret key as a byte array.
     */
    public byte[] getSecretKey() {
        return secretKey;
    }

    /**
     * Sets the secret key as a byte array.
     */
    public void setSecretKey( byte[] secretKey ) {
        this.secretKey = secretKey;
    }

    /**
     * Set the secret key from a base64 encoded string (this is the format typically provided to users)
     */
    public void setSecretKey( String secretKey ) {
        try {
            this.secretKey = Base64.decodeBase64( secretKey.getBytes( "UTF-8" ) );
        } catch ( UnsupportedEncodingException e ) {
            throw new RuntimeException( "UTF-8 encoding isn't supported on this system", e ); // unrecoverable
        }
    }

    /**
     * Returns the estimated clock skew between the local machine and the Atmos cloud (if set).
     */
    public long getServerClockSkew() {
        return serverClockSkew;
    }

    /**
     * Sets the estimated clock skew between the local machine and the Atmos cloud. This number can be calculated by
     * calling {@link com.emc.atmos.api.AtmosApi#calculateServerClockSkew()}. Implementations should set this
     * automatically on their local config instances when this method is called.
     */
    public void setServerClockSkew( long serverClockSkew ) {
        this.serverClockSkew = serverClockSkew;
    }

    /**
     * Returns the full Atmos token ID
     */
    public String getTokenId() {
        return tokenId;
    }

    /**
     * Sets the full Atmos token ID
     */
    public void setTokenId( String tokenId ) {
        this.tokenId = tokenId;
    }

    /**
     * Returns the HTTP proxy used for communication to Atmos
     */
    public URI getProxyUri() {
        return proxyUri;
    }

    /**
     * Sets the HTTP proxy used for communication to Atmos
     */
    public void setProxyUri( URI proxyUri ) {
        this.proxyUri = proxyUri;
    }

    /**
     * Gets the username to use for the HTTP proxy
     */
    public String getProxyUser() {
        return proxyUser;
    }

    /**
     * Sets the username to use for the HTTP proxy
     */
    public void setProxyUser( String proxyUser ) {
        this.proxyUser = proxyUser;
    }

    /**
     * Gets the password to use for the HTTP proxy
     */
    public String getProxyPassword() {
        return proxyPassword;
    }

    /**
     * Sets the password to use for the HTTP proxy
     */
    public void setProxyPassword( String proxyPassword ) {
        this.proxyPassword = proxyPassword;
    }
}
