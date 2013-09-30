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
import com.emc.util.HttpUtil;

import java.util.List;
import java.util.Map;

/**
 * Represents a request to list/query the objects of a specific listable metadata name (tag).
 */
public class ListObjectsRequest extends ListMetadataRequest<ListObjectsRequest> {
    private String metadataName;

    @Override
    public String getServiceRelativePath() {
        return "objects";
    }

    @Override
    public String getMethod() {
        return "GET";
    }

    @Override
    public Map<String, List<Object>> generateHeaders() {
        Map<String, List<Object>> headers = super.generateHeaders();

        RestUtil.addValue( headers, RestUtil.XHEADER_TAGS, HttpUtil.encodeUtf8( metadataName ) );

        return headers;
    }

    @Override
    protected ListObjectsRequest me() {
        return this;
    }

    /**
     * Builder method for {@link #setMetadataName(String)}
     */
    public ListObjectsRequest metadataName( String metadataName ) {
        setMetadataName( metadataName );
        return this;
    }

    /**
     * Gets the metadata name (tag) to query.
     */
    public String getMetadataName() {
        return metadataName;
    }

    /**
     * Sets the metadata name (tag) to query. Atmos will return all of the objects that are assigned this exact tag.
     */
    public void setMetadataName( String metadataName ) {
        this.metadataName = metadataName;
    }
}
