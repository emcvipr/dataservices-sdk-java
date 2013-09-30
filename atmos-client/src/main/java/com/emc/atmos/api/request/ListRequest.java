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

import com.emc.atmos.api.RestUtil;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Common elements of a request for a list of items. Atmos uses the limit and token headers to page results. The limit
 * is specified in the request to limit the number of results returned. The token is specified in the response to
 * signify that there are more results to get and in the request to get the next page of results.
 *
 * @param <T> Represents the implementation type. Allows a consistent builder interface throughout the request
 *            hierarchy. Parameterize concrete subclasses with their own type and implement {@link #me()} to return
 *            "this". In abstract subclasses, return me() in builder methods.
 */
public abstract class ListRequest<T extends ListRequest<T>> extends Request {
    protected int limit;
    protected String token;

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

        RestUtil.addValue( headers, RestUtil.XHEADER_UTF8, "true" );

        if ( limit > 0 ) RestUtil.addValue( headers, RestUtil.XHEADER_LIMIT, limit );

        if ( token != null ) RestUtil.addValue( headers, RestUtil.XHEADER_TOKEN, token );

        return headers;
    }

    /**
     * Builder method for {@link #setLimit(int)}
     */
    public T limit( int limit ) {
        setLimit( limit );
        return me();
    }

    /**
     * Builder method for {@link #setToken(String)}
     */
    public T token( String token ) {
        setToken( token );
        return me();
    }

    /**
     * Gets the limit (page size) for this request.
     */
    public int getLimit() {
        return limit;
    }

    /**
     * Gets the cursor token for this request, returned in the response to indicate there is another page of results
     * and in the request to get the next page of results.
     */
    public String getToken() {
        return token;
    }

    /**
     * Sets the limit (page size) for this request.
     */
    public void setLimit( int limit ) {
        this.limit = limit;
    }

    /**
     * Sets the cursor token for this request, returned in the response to indicate there is another page of results
     * and in the request to get the next page of results. API implementations should set this value automatically if
     * it is returned in the response, so you can check this value for null to see if you have received the entire
     * list.
     */
    public void setToken( String token ) {
        this.token = token;
    }
}
