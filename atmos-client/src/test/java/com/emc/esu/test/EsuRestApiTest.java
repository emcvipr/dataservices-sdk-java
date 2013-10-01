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
package com.emc.esu.test;

import com.emc.atmos.util.AtmosClientFactory;
import com.emc.esu.api.EsuApi;
import com.emc.esu.api.EsuException;
import com.emc.esu.api.rest.EsuRestApi;
import com.emc.esu.api.rest.LBEsuRestApi;
import com.emc.esu.sysmgmt.SysMgmtApi;
import com.emc.util.PropertiesUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class EsuRestApiTest extends EsuApiTest {
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
     * Comma-delimited list of access nodes.  Set in atmos.properties or -Datmos.hosts
     */
    private String hosts;

    /**
     * Port of ESU server (usually 80 or 443). Set in atmos.properties or -Datmos.port
     */
    private int port = 80;

    public EsuRestApiTest() {
    	super();

        uid2 = PropertiesUtil.getRequiredProperty( AtmosClientFactory.ATMOS_PROPERTIES_FILE, "atmos.uid" );
        secret = PropertiesUtil.getRequiredProperty( AtmosClientFactory.ATMOS_PROPERTIES_FILE, "atmos.secret" );
        host = PropertiesUtil.getRequiredProperty( AtmosClientFactory.ATMOS_PROPERTIES_FILE, "atmos.host" );
        port = Integer.parseInt( PropertiesUtil.getRequiredProperty( AtmosClientFactory.ATMOS_PROPERTIES_FILE, "atmos.port" ) );
        hosts = PropertiesUtil.getProperty(AtmosClientFactory.ATMOS_PROPERTIES_FILE, "atmos.hosts");
    }

    /**
     * @see junit.framework.TestCase#setUp()
     */
    @Before
    public void setUp() throws Exception {
        esu = new LBEsuRestApi( Arrays.asList( (hosts == null) ? new String[]{host} : hosts.split( "," ) ),
                                port, uid2, secret );
        esu.setUnicodeEnabled(true);
        uid = uid2;
        SysMgmtApi.disableCertificateValidation();
    }


    /**
     * Test handling signature failures.  Should throw an exception with
     * error code 1032.
     */
    @Test
    public void testSignatureFailure() throws Exception {
       EsuApi tempEsu = esu;
       try {
           // Fiddle with the secret key
           esu = new EsuRestApi( host, port, uid, secret.toUpperCase() );
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
            ((EsuRestApi)esu).setContext( "/restttttttttt" );
            testCreateEmptyObject();
            Assert.fail( "Expected exception to be thrown" );
        } catch( EsuException e ) {
            Assert.assertEquals( "Expected error code 404 for bad context root",
                    404, e.getHttpCode() );
        }

    }

    @Test
    public void testServerOffset() throws Exception {
    	long offset = ((EsuRestApi)esu).calculateServerOffset();
    	l4j.info("Server offset: " + offset + " milliseconds");
    }

    /**
     * NOTE: This method does not actually test that the custom headers are sent over the wire. Run tcpmon or wireshark
     * to verify
     */
    @Test
    public void testCustomHeaders() throws Exception {
        Map<String, String> customHeaders = new HashMap<String, String>();
        customHeaders.put( "myCustomHeader", "Hello World!" );
        ((EsuRestApi) this.esu).setCustomHeaders( customHeaders );
        this.esu.getServiceInformation();
    }
}
