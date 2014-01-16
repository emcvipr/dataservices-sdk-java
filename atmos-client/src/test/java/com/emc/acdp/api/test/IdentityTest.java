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

import java.util.Random;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.emc.acdp.api.jersey.AcdpAdminApiClient;
import com.emc.acdp.api.jersey.AcdpMgmtApiClient;
import com.emc.cdp.services.rest.model.Identity;
import com.emc.cdp.services.rest.model.IdentityList;
import com.emc.cdp.services.rest.model.ObjectFactory;
import com.emc.cdp.services.rest.model.Profile;

/**
 * @author cwikj
 */
public class IdentityTest {
    AcdpMgmtApiClient mgmt;
    AcdpAdminApiClient admin;

    @Before
    public void setUp() throws Exception {
        try {
            mgmt = new AcdpMgmtApiClient( AcdpTestUtil.loadMgmtConfig() );
            admin = new AcdpAdminApiClient( AcdpTestUtil.loadAdminConfig() );
        } catch(Exception e) {
            Assume.assumeNoException("Loading acdp.properties failed", e);
        }

    }

    @Test
    public void testCreateIdentityNullProfile() {
        ObjectFactory of = new ObjectFactory();
        Identity ident = of.createIdentity();
        ident.setId( generateEmail() );
        ident.setPassword( rand8char() + "!" );
        try {
            mgmt.createIdentity( ident );
            Assert.fail( "Should have failed with profile required" );
        } catch ( Exception e ) {
            // OK
        }
    }

    @Test
    public void testListAllIdentities() {
        IdentityList list = admin.listIdentities( true, true, 1, 1000 );
        Assert.assertNotNull( list );
        Assert.assertTrue( list.getTotalResults() > 0 );
    }

    @Test
    public void testCreateIdentityPartialProfile() {
        ObjectFactory of = new ObjectFactory();
        Identity ident = of.createIdentity();
        ident.setId( rand8char() );
        ident.setPassword( rand8char() + "!" );
        Profile p = of.createProfile();
        p.setFirstName( rand8char() );
        p.setLastName( rand8char() );
        p.setEmail( generateEmail() );
        ident.setProfile( p );
        mgmt.createIdentity( ident );
    }

    @Test
    public void testCreateIdentityNoPassword() {
        ObjectFactory of = new ObjectFactory();
        Identity ident = of.createIdentity();
        ident.setId( generateEmail() );
        try {
            mgmt.createIdentity( ident );
            Assert.fail( "Should have failed with password required" );
        } catch ( Exception e ) {
            // OK
        }
    }

    @Test
    public void testCreateGetIdentity() {
        ObjectFactory of = new ObjectFactory();
        Identity ident = of.createIdentity();

        String email = generateEmail();
        String id = rand8char();
        String firstName = rand8char();
        String lastName = rand8char();

        ident.setId( id );
        ident.setPassword( rand8char() + "!" );
        Profile p = of.createProfile();
        p.setFirstName( firstName );
        p.setLastName( lastName );
        p.setEmail( email );
        ident.setProfile( p );
        mgmt.createIdentity( ident );

        Identity ident2 = admin.getIdentity( id );

        Assert.assertNotNull( "Identity read back null", ident2 );
        Assert.assertEquals( "ID mismatch", id, ident2.getId() );
        Assert.assertEquals( "Name mismatch", firstName, ident2.getProfile()
                                                               .getFirstName() );
        Assert.assertEquals( "Last name mismatch", lastName, ident2.getProfile()
                                                                   .getLastName() );
        Assert.assertEquals( "email mismatch", email, ident2.getProfile()
                                                            .getEmail() );
    }

    @Test
    public void testGetIdentity() {
        String id = "christopher.arnett@emc.com";
        Identity ident = admin.getIdentity( id );
        Assert.assertNotNull( "Identity is null", ident );
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
}
