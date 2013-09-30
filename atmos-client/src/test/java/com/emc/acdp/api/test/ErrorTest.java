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
package com.emc.acdp.api.test;

import com.emc.acdp.AcdpException;
import com.emc.acdp.api.AcdpAdminConfig;
import com.emc.acdp.api.jersey.AcdpAdminApiClient;
import com.emc.util.PropertiesUtil;

import junit.framework.Assert;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

public class ErrorTest {
    private static final String DO_NOT_CREATE_THIS_ACCOUNT = "delete_this_account_immediately";

    AcdpAdminApiClient admin;

    @Before
    public void setUp() throws Exception {
        try {
            admin = new AcdpAdminApiClient( loadAdminConfig( "acdp.properties" ) );
        } catch(Exception e) {
            Assume.assumeNoException("Loading acdp.properties failed", e);
        }

    }

    @Test
    public void testErrorParsing() {
        try {
            admin.getAccount( DO_NOT_CREATE_THIS_ACCOUNT );
            Assert.fail( "Test account should not exist, but does!" );
        } catch ( AcdpException e ) {
            Assert.assertNotNull( "ACDP code is null", e.getAcdpCode() );
        }
    }

    private AcdpAdminConfig loadAdminConfig( String fileName ) throws URISyntaxException {
        URI endpoint = new URI( PropertiesUtil.getRequiredProperty(fileName, "acdp.admin.endpoint") );
        String username = PropertiesUtil.getRequiredProperty(fileName, "acdp.admin.username");
        String password = PropertiesUtil.getRequiredProperty(fileName, "acdp.admin.password");

        return new AcdpAdminConfig( endpoint.getScheme(), endpoint.getHost(), endpoint.getPort(), username, password );
    }
}
