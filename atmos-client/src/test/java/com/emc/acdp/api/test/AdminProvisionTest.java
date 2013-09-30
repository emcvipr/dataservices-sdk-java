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

import com.emc.acdp.api.AcdpAdminApi;
import com.emc.acdp.api.AcdpAdminConfig;
import com.emc.acdp.api.jersey.AcdpAdminApiClient;
import com.emc.cdp.services.rest.model.Account;
import com.emc.esu.api.EsuException;
import com.emc.util.PropertiesUtil;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.util.Random;

/**
 * This test case tests provisioning an ACDP account from front-to-back using
 * the ACDP Admin API.
 * Note: Requires CDP 1.1.2+ for addAccountAssignee.
 *
 * @author cwikj
 */
public class AdminProvisionTest {
    private AcdpAdminConfig config;

    public AdminProvisionTest() throws Exception {
        try {
            config = loadAdminConfig( "acdp.properties" );
        } catch(Exception e) {
            Assume.assumeNoException("Loading acdp.properties failed", e);
        }
    }

    @Test
    public void testProvisionSequence() {
        // Step 0: Login
        AcdpAdminApi api = new AcdpAdminApiClient( config );

        // Step 1: Create an Account
        Account acct = new Account();
        acct.setName( "Testcase Account" );
        acct.setDescription( "This account was created through JUnit" );
        acct.setType( "web" );

        String accountId = api.createAccount( acct );
        Assert.assertNotNull( "Empty AccountID", accountId );

        // Step 2: Create an Identity and assign it as the account admin
        String firstName = rand8char();
        String lastName = rand8char();
        String email = generateEmail();
        String password = rand8char() + "!";
        String role = "account_manager";
        String identityId = email;

        api.addAccountAssignee( accountId, identityId, password, firstName, lastName, email, role );

        // Step 3: Create a subscription for the account
        String subscriptionId = api.createSubscription( accountId, "storageservice" );
        Assert.assertNotNull( "Subscription ID is null!", subscriptionId );

        // Now do the reverse

        // Step 4: Delete the account subscription
        api.deleteSubscription( accountId, subscriptionId );

        // Step 5: Unassign the identity
        api.unassignAccountIdentity( accountId, identityId );

        // Step 6: Delete the identity
        api.deleteIdentity( identityId );

        // Step 7: Delete the account
        api.deleteAccount( accountId );

    }

    @Test
    public void testAssignIdentityError() {
        // Step 0: Login
        AcdpAdminApi api = new AcdpAdminApiClient( config );

        // Step 1: Create an Account
        Account acct = new Account();
        acct.setName( "Testcase Account" );
        acct.setDescription( "This account was created through JUnit" );
        acct.setType( "web" );

        String accountId = api.createAccount( acct );
        Assert.assertNotNull( "Empty AccountID", accountId );

        // Step 2: Create an Identity and assign it as the account admin
        String firstName = rand8char();
        String lastName = rand8char();
        String email = generateEmail();
        String password = rand8char() + "!";
        String role = "account_manager";
        String identityId = email;

        api.addAccountAssignee( accountId, identityId, password, firstName, lastName, email, role );

        try {
            // Do it again, should get error.
            api.addAccountAssignee( accountId, identityId, password, firstName, lastName, email, role );
            Assert.fail( "Expected Exception" );
        } catch ( EsuException e ) {
            Assert.assertEquals( "HTTP code wrong", 409, e.getHttpCode() );
            String msg = MessageFormat.format(
                    "The identity \"{0}\" is already assigned to an account",
                    identityId );
            Assert.assertEquals( "Error message incorrect", msg, e.getMessage() );
        }

        // Cleanup

        // Step 5: Unassign the identity
        api.unassignAccountIdentity( accountId, identityId );

        // Step 6: Delete the identity
        api.deleteIdentity( identityId );

        // Step 7: Delete the account
        api.deleteAccount( accountId );

    }

    private String generateEmail() {
        return rand8char() + "@" + rand8char() + ".com";
    }

    protected String rand8char() {
        Random r = new Random();
        StringBuffer sb = new StringBuffer( 8 );
        for ( int i = 0; i < 8; i++ ) {
            sb.append( (char) ('a' + r.nextInt( 26 )) );
        }
        return sb.toString();
    }

    private AcdpAdminConfig loadAdminConfig( String fileName ) throws URISyntaxException {
        URI endpoint = new URI( PropertiesUtil.getRequiredProperty(fileName, "acdp.admin.endpoint") );
        String username = PropertiesUtil.getRequiredProperty(fileName, "acdp.admin.username");
        String password = PropertiesUtil.getRequiredProperty(fileName, "acdp.admin.password");

        return new AcdpAdminConfig( endpoint.getScheme(), endpoint.getHost(), endpoint.getPort(), username, password );
    }
}
