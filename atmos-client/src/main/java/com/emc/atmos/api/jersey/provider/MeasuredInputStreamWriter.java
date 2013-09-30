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

import com.emc.atmos.api.jersey.MeasuredInputStream;
import com.emc.util.StreamUtil;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

@Produces("*/*")
public class MeasuredInputStreamWriter implements MessageBodyWriter<MeasuredInputStream> {
    @Override
    public long getSize( MeasuredInputStream mis,
                         Class<?> type,
                         Type genericType,
                         Annotation[] annotations,
                         MediaType mediaType ) {
        return mis.getSize();
    }

    @Override
    public boolean isWriteable( Class<?> type, Type genericType, Annotation annotations[], MediaType mediaType ) {
        return MeasuredInputStream.class.isAssignableFrom( type );
    }

    @Override
    public void writeTo( MeasuredInputStream mis,
                         Class<?> type,
                         Type genericType,
                         Annotation annotations[],
                         MediaType mediaType,
                         MultivaluedMap<String, Object> httpHeaders,
                         OutputStream entityStream ) throws IOException {
        StreamUtil.copy( mis, entityStream, mis.getSize() );
    }
}
