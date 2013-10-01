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
package com.emc.atmos.api.jersey;

import com.emc.atmos.AtmosException;
import com.emc.atmos.api.AtmosConfig;
import com.emc.atmos.api.jersey.provider.*;
import com.emc.util.SslUtil;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import com.sun.jersey.client.urlconnection.HttpURLConnectionFactory;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import com.sun.jersey.core.impl.provider.entity.ByteArrayProvider;
import com.sun.jersey.core.impl.provider.entity.FileProvider;
import com.sun.jersey.core.impl.provider.entity.StringProvider;
import com.sun.jersey.core.impl.provider.entity.XMLRootElementProvider;

import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import java.util.List;

public class JerseyUtil {
    public static Client createClient( AtmosConfig config,
                                       List<Class<MessageBodyReader<?>>> readers,
                                       List<Class<MessageBodyWriter<?>>> writers ) {
        try {
            ClientConfig clientConfig = new DefaultClientConfig();

            // register an open trust manager to allow SSL connections to servers with self-signed certificates
            if ( config.isDisableSslValidation() ) {
                clientConfig.getProperties().put( HTTPSProperties.PROPERTY_HTTPS_PROPERTIES,
                                                  new HTTPSProperties( SslUtil.gullibleVerifier,
                                                                       SslUtil.createGullibleSslContext() ) );
            }

            addHandlers( clientConfig, readers, writers );

            Client client;
            if ( config.getProxyUri() != null ) {

                // set proxy configuration
                HttpURLConnectionFactory factory = new ProxyURLConnectionFactory( config.getProxyUri(),
                                                                                  config.getProxyUser(),
                                                                                  config.getProxyPassword() );
                client = new Client( new URLConnectionClientHandler( factory ), clientConfig );
            } else {

                // this should pick up proxy config from system properties
                client = Client.create( clientConfig );
            }

            // add preemptive proxy authorization (this will only work for unencrypted requests)
            String proxyUser = config.getProxyUser(), proxyPassword = config.getProxyPassword();
            if ( proxyUser == null ) {
                proxyUser = System.getProperty( "http.proxyUser" );
                proxyPassword = System.getProperty( "http.proxyPassword" );
            }
            client.addFilter( new ProxyAuthFilter( proxyUser, proxyPassword ) );

            addFilters( client, config );

            return client;

        } catch ( Exception e ) {
            throw new AtmosException( "Error configuring REST client", e );
        }
    }

    static void addHandlers( ClientConfig clientConfig,
                             List<Class<MessageBodyReader<?>>> readers,
                             List<Class<MessageBodyWriter<?>>> writers ) {
        // add our message body handlers
        clientConfig.getClasses().clear();

        // custom types and buffered writers to ensure content-length is set
        clientConfig.getClasses().add( MeasuredStringWriter.class );
        clientConfig.getClasses().add( MeasuredJaxbWriter.App.class );
        clientConfig.getClasses().add( MeasuredJaxbWriter.Text.class );
        clientConfig.getClasses().add( MeasuredJaxbWriter.General.class );
        clientConfig.getClasses().add( MeasuredInputStreamWriter.class );
        clientConfig.getClasses().add( BufferSegmentWriter.class );
        clientConfig.getClasses().add( MultipartReader.class );

        // Jersey providers for types we support
        clientConfig.getClasses().add( ByteArrayProvider.class );
        clientConfig.getClasses().add( FileProvider.class );
        clientConfig.getClasses().add( StringProvider.class );
        clientConfig.getClasses().add( XMLRootElementProvider.App.class );
        clientConfig.getClasses().add( XMLRootElementProvider.Text.class );
        clientConfig.getClasses().add( XMLRootElementProvider.General.class );

        // user-defined types
        if ( readers != null ) {
            for ( Class<MessageBodyReader<?>> reader : readers ) {
                clientConfig.getClasses().add( reader );
            }
        }
        if ( writers != null ) {
            for ( Class<MessageBodyWriter<?>> writer : writers ) {
                clientConfig.getClasses().add( writer );
            }
        }
    }

    static void addFilters( Client client, AtmosConfig config ) {
        // add filters
        client.addFilter( new ErrorFilter() );
        if ( config.isEnableRetry() ) client.addFilter( new RetryFilter( config ) );
        client.addFilter( new AuthFilter( config ) );
    }

    private JerseyUtil() {
    }
}
