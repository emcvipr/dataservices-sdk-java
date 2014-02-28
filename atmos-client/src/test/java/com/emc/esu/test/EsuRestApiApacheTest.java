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
/**
 *
 */
package com.emc.esu.test;

import java.net.URI;
import java.util.Properties;

import com.emc.test.util.Concurrent;
import com.emc.test.util.ConcurrentJunitRunner;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.emc.atmos.util.AtmosClientFactory;
import com.emc.esu.api.EsuApi;
import com.emc.esu.api.EsuException;
import com.emc.esu.api.rest.EsuRestApiApache;
import com.emc.vipr.services.lib.ViprConfig;

import org.junit.runner.RunWith;

/**
 * @author jason
 *
 */
@SuppressWarnings("deprecation")
@RunWith(ConcurrentJunitRunner.class)
@Concurrent
public class EsuRestApiApacheTest extends EsuApiTest {
    /**
     * UID to run tests with.  Set in properties file or -Datmos.uid.
     */
    private String uid2;

    /**
     * Shared secret for UID.  Set in atmos.properties or -Datmos.secret
     */
    private String secret;

    /**
     * Hostname or IP of ESU server.  Set in atmos.properties or -Datmos.host
     */
    private String host;

    /**
     * Port of ESU server (usually 80 or 443). Set in atmos.properties or -Datmos.port
     */
    private int port = 80;

    public EsuRestApiApacheTest() {
        try {
            Properties p = ViprConfig.getProperties();
            uid2 = ViprConfig.getPropertyNotEmpty(p, ViprConfig.PROP_ATMOS_UID);
            secret = ViprConfig.getPropertyNotEmpty(p, ViprConfig.PROP_ATMOS_SECRET);
            URI u = new URI(ViprConfig.getPropertyNotEmpty(p, ViprConfig.PROP_ATMOS_ENDPOINTS).split(",")[0].trim());
            host = u.getHost();
            port = u.getPort();
            if(port == -1) {
                if("http".equals(u.getScheme())) {
                    port = 80;
                } else if("https".equals(u.getScheme())) {
                    port = 443;
                }
            }
            isVipr = AtmosClientFactory.atmosIsVipr();
        } catch(Exception e) {
            Assume.assumeNoException("Could not load Atmos configuration", e);
        }
    }

    /**
     * @see junit.framework.TestCase#setUp()
     */
    @Before
    public void setUp() throws Exception {
        esu = new EsuRestApiApache( host, port, uid2, secret );
        ((EsuRestApiApache) esu).setUnicodeEnabled( true );
        uid = uid2;
    }



    //
    // TESTS START HERE
    //




    /**
     * Test handling signature failures.  Should throw an exception with
     * error code 1032.
     */
    @Test
    public void testSignatureFailure() throws Exception {
       EsuApi tempEsu = esu;
       try {
           // Fiddle with the secret key
           esu = new EsuRestApiApache( host, port, uid, secret.toUpperCase() );
           //test = new EsuApiTest( esu );
           testCreateEmptyObject();
           Assert.fail( "Expected exception to be thrown" );
       } catch( EsuException e ) {
           Assert.assertEquals( "Expected error code 1032 for signature failure",
                   1032, e.getAtmosCode() );
       } finally {
           esu = tempEsu;
       }
    }

    /**
     * Test general HTTP errors by generating a 404.
     */
    @Test
    public void testFourOhFour() throws Exception {
        try {
            // Fiddle with the context
            ((EsuRestApiApache)esu).setContext( "/restttttttttt" );
            testCreateEmptyObject();
            Assert.fail( "Expected exception to be thrown" );
        } catch( EsuException e ) {
            Assert.assertEquals( "Expected error code 404 for bad context root",
                    404, e.getHttpCode() );
        }

    }

    @Test
    public void testServerOffset() throws Exception {
    	long offset = ((EsuRestApiApache)esu).calculateServerOffset();
    	l4j.info("Server offset: " + offset + " milliseconds");
    }

}
