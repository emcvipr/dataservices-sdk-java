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

import java.io.Serializable;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Represents a request that has been pre-signed and is valid until <code>expiration</code>.
 * <p/>
 * Immutable.
 */
public class PreSignedRequest implements Serializable {
    private static final long serialVersionUID = -5841074558608401979L;

    private URL url;
    private String method;
    private String contentType;
    private Map<String, List<Object>> headers;
    private Date expiration;

    public PreSignedRequest( URL url,
                             String method,
                             String contentType,
                             Map<String, List<Object>> headers,
                             Date expiration ) {
        this.url = url;
        this.method = method;
        this.contentType = contentType;
        this.headers = headers;
        this.expiration = expiration;
    }

    /**
     * Gets the URL of the request. This includes the Atmos endpoint.
     */
    public URL getUrl() {
        return url;
    }

    /**
     * Gets the HTTP method of the request.
     */
    public String getMethod() {
        return method;
    }

    /**
     * Gets the content-type to use for the request.
     */
    public String getContentType() {
        return contentType;
    }

    /**
     * Gets the headers to use in the request.
     */
    public Map<String, List<Object>> getHeaders() {
        return headers;
    }

    /**
     * Gets the date this request expires (is no longer valid).
     */
    public Date getExpiration() {
        return expiration;
    }
}
