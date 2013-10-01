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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Represents common elements of list requests that may include object metadata.
 *
 * @param <T> Represents the implementation type. Allows a consistent builder interface throughout the request
 *            hierarchy. Parameterize concrete subclasses with their own type and implement {@link #me()} to return
 *            "this". In abstract subclasses, return me() in builder methods.
 */
public abstract class ListMetadataRequest<T extends ListMetadataRequest<T>> extends ListRequest<T> {
    protected List<String> userMetadataNames;
    protected List<String> systemMetadataNames;
    protected boolean includeMetadata;

    @Override
    public Map<String, List<Object>> generateHeaders() {
        Map<String, List<Object>> headers = super.generateHeaders();

        if ( includeMetadata ) {
            RestUtil.addValue( headers, RestUtil.XHEADER_INCLUDE_META, 1 );
            if ( userMetadataNames != null )
                for ( String name : userMetadataNames ) RestUtil.addValue( headers, RestUtil.XHEADER_USER_TAGS, name );
            if ( systemMetadataNames != null )
                for ( String name : systemMetadataNames )
                    RestUtil.addValue( headers, RestUtil.XHEADER_SYSTEM_TAGS, name );
        }

        return headers;
    }

    /**
     * Builder method for {@link #setUserMetadataNames(java.util.List)}
     */
    public T userMetadataNames( String... userMetadataNames ) {
        if ( userMetadataNames == null || (userMetadataNames.length == 1 && userMetadataNames[0] == null) )
            userMetadataNames = new String[0];
        setUserMetadataNames( Arrays.asList( userMetadataNames ) );
        return me();
    }

    /**
     * Builder method for {@link #setSystemMetadataNames(java.util.List)}
     */
    public T systemMetadataNames( String... systemMetadataNames ) {
        if ( systemMetadataNames == null || (systemMetadataNames.length == 1 && systemMetadataNames[0] == null) )
            systemMetadataNames = new String[0];
        setSystemMetadataNames( Arrays.asList( systemMetadataNames ) );
        return me();
    }

    /**
     * Builder method for {@link #setIncludeMetadata(boolean)}
     */
    public T includeMetadata( boolean includeMetadata ) {
        setIncludeMetadata( includeMetadata );
        return me();
    }

    /**
     * Gets the list of user metadata names that will be returned for each object in the list.
     */
    public List<String> getUserMetadataNames() {
        return userMetadataNames;
    }

    /**
     * Gets the list of system metadata names that will be returned for each object in the list.
     */
    public List<String> getSystemMetadataNames() {
        return systemMetadataNames;
    }

    /**
     * Gets whether the resulting list should include metadata for each object.
     */
    public boolean isIncludeMetadata() {
        return includeMetadata;
    }

    /**
     * Sets the list of user metadata names that will be returned for each object in the list. If null, all user
     * metadata will be returned for each object in the list.
     */
    public void setUserMetadataNames( List<String> userMetadataNames ) {
        this.userMetadataNames = userMetadataNames;
    }

    /**
     * Sets the list of system metadata names that will be returned for each object in the list. If null, all system
     * metadata will be returned for each object in the list.
     */
    public void setSystemMetadataNames( List<String> systemMetadataNames ) {
        this.systemMetadataNames = systemMetadataNames;
    }

    /**
     * Sets whether the resulting list should include metadata for each object. Note that the default page size for
     * result lists changes when you include metadata in the results. Most Atmos systems default to 10k objects per
     * page without metadata and 500 with.
     */
    public void setIncludeMetadata( boolean includeMetadata ) {
        this.includeMetadata = includeMetadata;
    }
}
