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
package com.emc.acdp;

public abstract class AcdpConfig {
    private String proto;
    private boolean disableSslValidation = false;
    private String host;
    private int port;
    private String username;
    private String password;
    private String sessionToken;

    public AcdpConfig() {
        this( "http", null, 80, null, null );
    }

    public AcdpConfig( String proto, String host, int port, String username, String password ) {
        this.proto = proto;
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public abstract String getLoginPath();

    public abstract boolean isSecureRequest( String path, String method );

    public String getProto() {
        return proto;
    }

    public void setProto( String proto ) {
        this.proto = proto;
    }

    public boolean isDisableSslValidation() {
        return disableSslValidation;
    }

    public void setDisableSslValidation( boolean disableSslValidation ) {
        this.disableSslValidation = disableSslValidation;
    }

    public String getHost() {
        return host;
    }

    public void setHost( String host ) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort( int port ) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername( String username ) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword( String password ) {
        this.password = password;
    }

    public String getSessionToken() {
        return sessionToken;
    }

    public void setSessionToken( String sessionToken ) {
        this.sessionToken = sessionToken;
    }

    public String getBaseUri() {
        String url = proto + "://" + host;
        if ( port > 0 ) url += ":" + port;
        return url;
    }
}
