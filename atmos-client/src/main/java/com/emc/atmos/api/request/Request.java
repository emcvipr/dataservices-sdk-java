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

import java.util.List;
import java.util.Map;

/**
 * Represents an Atmos REST request.
 */
public abstract class Request {
    /**
     * Returns the service-relative path of this request (i.e. "objects/{identifier}" for read-object).
     */
    public abstract String getServiceRelativePath();

    /**
     * Override if a request requires a query string (i.e. "metadata/system" for getting system metadata)
     *
     * @return the URL query string for this request
     */
    public String getQuery() {
        return null;
    }

    /**
     * Returns the HTTP method this request will use.
     */
    public abstract String getMethod();

    /**
     * Returns the HTTP headers to send in this request, to be generated from other request properties immediately
     * before sending.
     */
    public abstract Map<String, List<Object>> generateHeaders();

    /**
     * Override and return true if this request supports the Expect: 100-continue header. Typically only object write
     * requests support this option.
     */
    public boolean supports100Continue() {
        return false;
    }
}
