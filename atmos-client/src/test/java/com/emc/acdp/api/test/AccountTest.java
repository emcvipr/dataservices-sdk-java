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
import com.emc.cdp.services.rest.model.Attribute;
import com.emc.cdp.services.rest.model.ObjectFactory;
import com.emc.util.PropertiesUtil;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Random;

/**
 * @author cwikj
 */
public class AccountTest {
    private static final Logger l4j = Logger.getLogger( AccountTest.class );

    AcdpAdminApi acdp;
    AcdpAdminConfig config;

    public AccountTest() throws Exception {
        try {
            config = loadAdminConfig( "acdp.properties" );
        } catch(Exception e) {
            Assume.assumeNoException("Loading acdp.properties failed", e);
        }
    }

    @Before
    public void setUp() {
        acdp = new AcdpAdminApiClient( config );

        // Create the identity if it doesn't exist.

    }

    //@Test
    public void testCreateDeleteAccount() {
        ObjectFactory of = new ObjectFactory();
        Account acct = of.createAccount();
        acct.setName( "name1" );
        acct.setType( "web" );
        String accountId = acdp.createAccount( acct );
        l4j.debug( "Created account " + accountId );

        acdp.deleteAccount( accountId );
    }

    //@Test
    public void testAccountCustomAttributes() {
        ObjectFactory of = new ObjectFactory();
        Account acct = of.createAccount();
        acct.setName( "name1" );
        acct.setType( "direct" );
        Attribute attr1 = of.createAttribute();
        attr1.setName( "myAccountType" );
        attr1.setValue( "enterprise" );
        Attribute attr2 = of.createAttribute();
        attr2.setName( "policyState" );
        attr2.setValue( "DR" );
        acct.getAttributes().add( attr1 );
        acct.getAttributes().add( attr2 );
        String accountId = acdp.createAccount( acct );

        Account acct2 = acdp.getAccount( accountId );

        Assert.assertEquals( "Account name wrong", acct.getName(), acct2.getName() );
        Assert.assertEquals( "Account type wrong", acct.getType(), acct2.getType() );
        validateAttribute( acct2.getAttributes(), attr1 );
        validateAttribute( acct2.getAttributes(), attr2 );

    }

    private void validateAttribute( List<Attribute> attributes, Attribute attr ) {
        for ( Attribute a : attributes ) {
            if ( a.getName().equals( attr.getName() ) ) {
                Assert.assertEquals( "Attribute " + a.getName() + " does not match", attr.getValue(), a.getValue() );
                return;
            }
        }
        Assert.fail( "Attribute " + attr.getName() + " does not exist" );
    }

    //@Test
    public void testCreateDeleteAccountObj() throws Exception {
        // Create Account
        ObjectFactory of = new ObjectFactory();
        Account acct = of.createAccount();
        acct.setName( "name1" );
        acct.setType( "web" );

        String accountId = acdp.createAccount( acct );
        Assert.assertNotNull( "Account ID null", accountId );
        l4j.debug( "Created account " + accountId );

        // Delete account
        acdp.deleteAccount( accountId );
    }

    //@Test
    public void testAssignAccountAdmin() throws Exception {
        // Create Account
        ObjectFactory of = new ObjectFactory();
        Account acct = of.createAccount();
        acct.setName( "name1" );
        acct.setType( "web" );

        String accountId = acdp.createAccount( acct );
        Assert.assertNotNull( "Account ID null", accountId );
        l4j.debug( "Created account " + accountId );

        // Assign the account admin
        // CreateAccountInvitationRequest cair = new
        // CreateAccountInvitationRequest(
        // r2.getAccountId(), accountAdmin, "account_manager",
        // r1.getAdminSessionId());
        // cair.setEndpoint(acdpEndpoint);
        // CreateAccountInvitationResponse r3 = cair.call();
        // Assert.assertTrue("Create account invite failed: " +
        // r3.getErrorMessage(),
        // r3.isSuccessful());
        // Assert.assertNotNull("Account invite ID null", r3.getInvitationId());
        // l4j.debug("Created account invite " + r3.getInvitationId());
        acdp.addAccountAssignee( accountId,
                                 rand8char(),
                                 rand8char() + "!",
                                 rand8char(),
                                 rand8char(),
                                 generateEmail(),
                                 "account_manager" );

//        EditAccountIdentityRequest eair = new EditAccountIdentityRequest(
//                r2.getAccountId(), accountAdmin, "account_manager",
//                r1.getAdminSessionId());
//        eair.setEndpoint(acdpEndpoint);
//        AcdpResponse r3 = eair.call();
//        Assert.assertTrue(
//                "Edit account idenitity failed: " + r3.getErrorMessage(),
//                r3.isSuccessful());

        // Delete account
        acdp.deleteAccount( accountId );
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
