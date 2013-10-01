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
package com.emc.atmos.sync.monitor;

public class DirectoryMonitorBean {
    private int atmosPort;
    private String atmosHost;
    private String atmosUid;
    private String atmosSecret;
    private String atmosDirectory;
    private String localDirectory;
    private boolean recursive;

    public DirectoryMonitorBean() {
    }

    public int getAtmosPort() {
        return atmosPort;
    }

    public void setAtmosPort( final int atmosPort ) {
        this.atmosPort = atmosPort;
    }

    public String getAtmosHost() {
        return atmosHost;
    }

    public void setAtmosHost( final String atmosHost ) {
        this.atmosHost = atmosHost;
    }

    public String getAtmosUid() {
        return atmosUid;
    }

    public void setAtmosUid( final String atmosUid ) {
        this.atmosUid = atmosUid;
    }

    public String getAtmosSecret() {
        return atmosSecret;
    }

    public void setAtmosSecret( final String atmosSecret ) {
        this.atmosSecret = atmosSecret;
    }

    public String getAtmosDirectory() {
        return atmosDirectory;
    }

    public void setAtmosDirectory( final String atmosDirectory ) {
        this.atmosDirectory = atmosDirectory;
        if ( !this.atmosDirectory.startsWith( "/" ) ) this.atmosDirectory = "/" + this.atmosDirectory;
        if ( !this.atmosDirectory.endsWith( "/" ) ) this.atmosDirectory += "/";
    }

    public String getLocalDirectory() {
        return localDirectory;
    }

    public void setLocalDirectory( String localDirectory ) {
        this.localDirectory = localDirectory;
    }

    public boolean isRecursive() {
        return recursive;
    }

    public void setRecursive( boolean recursive ) {
        this.recursive = recursive;
    }
}