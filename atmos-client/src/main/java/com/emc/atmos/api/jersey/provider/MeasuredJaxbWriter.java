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
package com.emc.atmos.api.jersey.provider;

import com.sun.jersey.core.impl.provider.entity.XMLRootElementProvider;
import com.sun.jersey.spi.inject.Injectable;

import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Providers;
import javax.xml.parsers.SAXParserFactory;

public class MeasuredJaxbWriter extends MeasuredMessageBodyWriter<Object> {
    public MeasuredJaxbWriter( MessageBodyWriter<Object> wrapped ) {
        super( wrapped );
    }

    @Produces( "application/xml" )
    public static final class App extends MeasuredJaxbWriter {
        public App( @Context Injectable<SAXParserFactory> spf, @Context Providers ps ) {
            super( new XMLRootElementProvider.App( spf, ps ) );
        }
    }

    @Produces( "text/xml" )
    public static final class Text extends MeasuredJaxbWriter {
        public Text( @Context Injectable<SAXParserFactory> spf, @Context Providers ps ) {
            super( new XMLRootElementProvider.Text( spf, ps ) );
        }
    }

    @Produces( "*/*" )
    public static final class General extends MeasuredJaxbWriter {
        public General( @Context Injectable<SAXParserFactory> spf, @Context Providers ps ) {
            super( new XMLRootElementProvider.General( spf, ps ) );
        }
    }
}
