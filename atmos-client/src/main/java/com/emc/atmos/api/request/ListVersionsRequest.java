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

import com.emc.atmos.api.ObjectId;

/**
 * Represents a request to list the versions of an object. Note that version lists are paged by default at 4096 results
 * per page.
 */
public class ListVersionsRequest extends ListRequest<ListVersionsRequest> {
    private ObjectId objectId;

    @Override
    public String getServiceRelativePath() {
        return objectId.getRelativeResourcePath();
    }

    @Override
    public String getQuery() {
        return "versions";
    }

    @Override
    public String getMethod() {
        return "GET";
    }

    @Override
    protected ListVersionsRequest me() {
        return this;
    }

    /**
     * Builder method for {@link #setObjectId(com.emc.atmos.api.ObjectId)}
     */
    public ListVersionsRequest objectId( ObjectId objectId ) {
        setObjectId( objectId );
        return this;
    }

    /**
     * Gets the target object ID for which to list versions.
     */
    public ObjectId getObjectId() {
        return objectId;
    }

    /**
     * Sets the target object ID for which to list versions.
     */
    public void setObjectId( ObjectId objectId ) {
        this.objectId = objectId;
    }
}
