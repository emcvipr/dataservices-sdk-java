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
package com.emc.atmos.api.test;

import com.emc.atmos.api.AtmosConfig;
import com.emc.atmos.api.jersey.EsuApiJerseyAdapter;
import com.emc.atmos.util.AtmosClientFactory;
import com.emc.esu.api.EsuException;
import com.emc.esu.api.rest.AbstractEsuRestApi;
import com.emc.esu.test.EsuApiTest;
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Test;

public class EsuApiJerseyAdapterTest extends EsuApiTest {
    private AtmosConfig config;

    public EsuApiJerseyAdapterTest() throws Exception {
        config = AtmosClientFactory.getAtmosConfig();
        uid = config.getTokenId();
        config.setDisableSslValidation( true );
        config.setEnableExpect100Continue( false );
        config.setEnableRetry( false );
        esu = new EsuApiJerseyAdapter( config );
    }

    /**
     * Test handling signature failures.  Should throw an exception with
     * error code 1032.
     */
    @Test
    public void testSignatureFailure() throws Exception {
        byte[] goodSecret = config.getSecretKey();
        String secretStr = new String( Base64.encodeBase64( goodSecret ), "UTF-8" );
        byte[] badSecret = Base64.decodeBase64( secretStr.toUpperCase().getBytes( "UTF-8" ) );
        try {
            // Fiddle with the secret key
            config.setSecretKey( badSecret );
            testCreateEmptyObject();
            Assert.fail( "Expected exception to be thrown" );
        } catch ( EsuException e ) {
            Assert.assertEquals( "Expected error code 1032 for signature failure",
                                 1032, e.getAtmosCode() );
        } finally {
            config.setSecretKey( goodSecret );
        }
    }

    /**
     * Test general HTTP errors by generating a 404.
     */
    @Test
    public void testFourOhFour() throws Exception {
        String goodContext = config.getContext();
        try {
            // Fiddle with the context
            config.setContext( "/restttttttttt" );
            testCreateEmptyObject();
            Assert.fail( "Expected exception to be thrown" );
        } catch ( EsuException e ) {
            Assert.assertEquals( "Expected error code 404 for bad context root",
                                 404, e.getHttpCode() );
        } finally {
            config.setContext( goodContext );
        }
    }

    @Test
    public void testServerOffset() throws Exception {
        long offset = ((AbstractEsuRestApi) esu).calculateServerOffset();
        l4j.info( "Server offset: " + offset + " milliseconds" );
    }
}
