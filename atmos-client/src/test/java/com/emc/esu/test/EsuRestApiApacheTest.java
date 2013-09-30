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

import com.emc.atmos.util.AtmosClientFactory;
import com.emc.esu.api.EsuApi;
import com.emc.esu.api.EsuException;
import com.emc.esu.api.rest.EsuRestApiApache;
import com.emc.util.PropertiesUtil;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author jason
 *
 */
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
    	uid2 = PropertiesUtil.getRequiredProperty(AtmosClientFactory.ATMOS_PROPERTIES_FILE, "atmos.uid");
    	secret = PropertiesUtil.getRequiredProperty(AtmosClientFactory.ATMOS_PROPERTIES_FILE, "atmos.secret");
    	host = PropertiesUtil.getRequiredProperty(AtmosClientFactory.ATMOS_PROPERTIES_FILE, "atmos.host");
    	port = Integer.parseInt( PropertiesUtil.getRequiredProperty(AtmosClientFactory.ATMOS_PROPERTIES_FILE, "atmos.port") );
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
