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

import com.sun.jersey.core.impl.provider.entity.StringProvider;

import javax.ws.rs.Produces;

@Produces( {"text/plain", "*/*"} )
public class MeasuredStringWriter extends MeasuredMessageBodyWriter<String> {
    private static final StringProvider provider = new StringProvider();

    public MeasuredStringWriter() {
        super( provider );
    }
}
