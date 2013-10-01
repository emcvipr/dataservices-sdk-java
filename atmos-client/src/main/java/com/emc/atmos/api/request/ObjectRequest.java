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
package com.emc.atmos.api.request;

import com.emc.atmos.api.ObjectIdentifier;
import com.emc.atmos.api.ObjectKey;
import com.emc.atmos.api.RestUtil;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Represents an Atmos REST request dealing with a specific object.
 *
 * @param <T> Represents the implementation type. Allows a consistent builder interface throughout the request
 *            hierarchy. Parameterize concrete subclasses with their own type and implement {@link #me()} to return
 *            "this". In abstract subclasses, return me() in builder methods.
 */
public abstract class ObjectRequest<T extends ObjectRequest<T>> extends Request {
    protected ObjectIdentifier identifier;

    /**
     * Returns "this" in concrete implementation classes. Used in builder methods to be consistent throughout the
     * hierarchy. For example, you can call <code>new CreateObjectRequest().identifier(path).content(content)</code>.
     *
     * @return this
     */
    protected abstract T me();

    @Override
    public Map<String, List<Object>> generateHeaders() {
        Map<String, List<Object>> headers = new TreeMap<String, List<Object>>();

        if ( identifier != null && identifier instanceof ObjectKey ) {
            RestUtil.addValue( headers, RestUtil.XHEADER_POOL, ((ObjectKey) identifier).getBucket() );
        }

        return headers;
    }

    /**
     * Builder method for {@link #setIdentifier(com.emc.atmos.api.ObjectIdentifier)}
     */
    public T identifier( ObjectIdentifier identifier ) {
        setIdentifier( identifier );
        return me();
    }

    /**
     * Returns the identifier of the target object for this request
     */
    public ObjectIdentifier getIdentifier() {
        return identifier;
    }

    /**
     * Sets the identifier of the target object for this request.
     */
    public void setIdentifier( ObjectIdentifier identifier ) {
        this.identifier = identifier;
    }
}
