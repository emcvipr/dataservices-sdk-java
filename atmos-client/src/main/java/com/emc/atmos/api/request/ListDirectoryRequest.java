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

import com.emc.atmos.api.ObjectPath;

/**
 * Represents a request to list all of the object under a directory in the subtenant namespace.
 */
public class ListDirectoryRequest extends ListMetadataRequest<ListDirectoryRequest> {
    protected ObjectPath path;

    @Override
    public String getServiceRelativePath() {
        return path.getRelativeResourcePath();
    }

    @Override
    public String getMethod() {
        return "GET";
    }

    @Override
    protected ListDirectoryRequest me() {
        return this;
    }

    /**
     * Builder method for {@link #setPath(com.emc.atmos.api.ObjectPath)}
     */
    public ListDirectoryRequest path( ObjectPath path ) {
        setPath( path );
        return this;
    }

    /**
     * Gets the path of the directory to list.
     */
    public ObjectPath getPath() {
        return path;
    }

    /**
     * Sets the path of the directory to list.
     */
    public void setPath( ObjectPath path ) {
        this.path = path;
    }
}
