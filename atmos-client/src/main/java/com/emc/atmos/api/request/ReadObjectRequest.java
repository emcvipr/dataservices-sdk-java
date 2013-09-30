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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Represents an Atmos read-object request.
 */
public class ReadObjectRequest extends ObjectRequest<ReadObjectRequest> {
    protected List<Range> ranges;

    @Override
    public String getServiceRelativePath() {
        return identifier.getRelativeResourcePath();
    }

    @Override
    public String getMethod() {
        return "GET";
    }

    @Override
    public Map<String, List<Object>> generateHeaders() {
        Map<String, List<Object>> headers = super.generateHeaders();

        RestUtil.addValue( headers, RestUtil.XHEADER_UTF8, "true" );

        if ( ranges != null && !ranges.isEmpty() )
            RestUtil.addValue( headers, RestUtil.HEADER_RANGE, "bytes=" + RestUtil.join( ranges, "," ) );

        return headers;
    }

    @Override
    protected ReadObjectRequest me() {
        return this;
    }

    /**
     * Builder method for {@link #setRanges(java.util.List)}
     */
    public ReadObjectRequest ranges( Range... range ) {
        if ( range == null || (range.length == 1 && range[0] == null) ) range = new Range[0];
        setRanges( Arrays.asList( range ) );
        return this;
    }

    /**
     * Returns the list of byte ranges to read from the target object.
     */
    public List<Range> getRanges() {
        return ranges;
    }

    /**
     * Sets the list of byte ranges to read from the target object.
     */
    public void setRanges( List<Range> ranges ) {
        this.ranges = ranges;
    }
}
