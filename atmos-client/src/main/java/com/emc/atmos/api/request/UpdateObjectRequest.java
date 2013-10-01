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

import com.emc.atmos.api.Range;
import com.emc.atmos.api.RestUtil;

import java.util.List;
import java.util.Map;

/**
 * Represents an update object request.
 */
public class UpdateObjectRequest extends PutObjectRequest<UpdateObjectRequest> {
    protected Range range;

    @Override
    public String getServiceRelativePath() {
        return identifier.getRelativeResourcePath();
    }

    @Override
    public String getMethod() {
        return "PUT";
    }

    @Override
    public Map<String, List<Object>> generateHeaders() {
        Map<String, List<Object>> headers = super.generateHeaders();

        if ( range != null )
            RestUtil.addValue( headers, RestUtil.HEADER_RANGE, "bytes=" + range );

        return headers;
    }

    @Override
    protected UpdateObjectRequest me() {
        return this;
    }

    /**
     * Builder method for {@link #setRange(com.emc.atmos.api.Range)}
     */
    public UpdateObjectRequest range( Range range ) {
        setRange( range );
        return this;
    }

    /**
     * Returns the byte range for this update request.
     */
    public Range getRange() {
        return range;
    }

    /**
     * Sets the byte range for this update request (the range of bytes to update in the target object).
     */
    public void setRange( Range range ) {
        this.range = range;
    }
}
