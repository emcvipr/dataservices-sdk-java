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

import com.emc.atmos.api.AtmosConfig;

import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import java.util.List;

/**
 * Extension of AtmosApiClient that does *not* use the commons-http client and instead uses the default Jersey client
 * (URLConnection).  Note that this implementation does not support the Expect: 100-continue header.
 */
public class AtmosApiBasicClient extends AtmosApiClient {
    public AtmosApiBasicClient( AtmosConfig config ) {
        this( config, null, null );
    }

    public AtmosApiBasicClient( AtmosConfig config,
                                List<Class<MessageBodyReader<?>>> readers,
                                List<Class<MessageBodyWriter<?>>> writers ) {
        super( config, JerseyUtil.createClient( config, readers, writers ), null );
    }
}
