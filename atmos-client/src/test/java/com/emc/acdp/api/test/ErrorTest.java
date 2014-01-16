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

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.emc.acdp.AcdpException;
import com.emc.acdp.api.jersey.AcdpAdminApiClient;

public class ErrorTest {
    private static final String DO_NOT_CREATE_THIS_ACCOUNT = "delete_this_account_immediately";

    AcdpAdminApiClient admin;

    @Before
    public void setUp() throws Exception {
        try {
            admin = new AcdpAdminApiClient( AcdpTestUtil.loadAdminConfig() );
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

}
