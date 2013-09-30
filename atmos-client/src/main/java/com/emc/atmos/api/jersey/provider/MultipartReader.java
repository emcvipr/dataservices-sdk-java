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

import com.emc.atmos.api.RestUtil;
import com.emc.atmos.api.multipart.MultipartEntity;

import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

@Consumes("multipart/byteranges")
public class MultipartReader implements MessageBodyReader<MultipartEntity> {
    @Override
    public boolean isReadable( Class<?> type, Type genericType, Annotation annotations[], MediaType mediaType ) {
        return MultipartEntity.class.isAssignableFrom( type )
               && RestUtil.TYPE_MULTIPART.equals( mediaType.getType() );
    }

    @Override
    public MultipartEntity readFrom( Class<MultipartEntity> type,
                                     Type genericType,
                                     Annotation annotations[],
                                     MediaType mediaType,
                                     MultivaluedMap<String, String> httpHeaders,
                                     InputStream entityStream ) throws IOException, WebApplicationException {
        return MultipartEntity.fromStream( entityStream,
                                           mediaType.getParameters().get( RestUtil.TYPE_PARAM_BOUNDARY ) );
    }
}
