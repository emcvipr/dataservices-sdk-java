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
package com.emc.atmos.mgmt;

import com.emc.atmos.AbstractConfig;

import java.net.URI;
import java.util.List;
import java.util.Map;

public abstract class AtmosMgmtConfig extends AbstractConfig {
    private static final String DEFAULT_CONTEXT = "/sysmgmt";

    private String username;
    private String password;

    public AtmosMgmtConfig() {
        super( DEFAULT_CONTEXT );
    }

    public AtmosMgmtConfig( String username, String password, URI... endpoints ) {
        super( DEFAULT_CONTEXT, endpoints );
        this.username = username;
        this.password = password;
    }

    public abstract Map<String, List<Object>> getAuthenticationHeaders();

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
}
