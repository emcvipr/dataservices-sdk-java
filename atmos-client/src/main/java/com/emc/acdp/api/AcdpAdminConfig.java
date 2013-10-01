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
package com.emc.acdp.api;

import com.emc.acdp.AcdpConfig;

public class AcdpAdminConfig extends AcdpConfig {
    public AcdpAdminConfig() {
    }

    public AcdpAdminConfig( String proto, String host, int port, String username, String password ) {
        super( proto, host, port, username, password );
    }

    @Override
    public String getLoginPath() {
        return "/cdp-rest/v1/admin/login";
    }

    @Override
    public boolean isSecureRequest( String path, String method ) {
        return true;
    }
}
