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
package com.emc.vipr.services.s3.model;

import java.util.List;

public class GetFileAccessResult {
    private List<String> mountPoints;
    private boolean isTruncated;
    private List<FileAccessObject> objects;

    private String lastKey;

    /**
     * Returns all of the mount points providing NFS access to the objects in a
     * discrete list. These are provided as a convenience so that clients can
     * start mount operations before the entire list of objects is received.
     * Many objects may be hosted on a particular mount point.
     */
    public List<String> getMountPoints() {
        return mountPoints;
    }

    public void setMountPoints(List<String> mountPoints) {
        this.mountPoints = mountPoints;
    }

    /**
     * If true, the list of objects has been truncated based on the maxKeys
     * parameter.
     */
    public boolean isTruncated() {
        return isTruncated;
    }

    public void setTruncated(boolean truncated) {
        isTruncated = truncated;
    }

    /**
     * @return NFS details for all the objects accessible via NFS.
     */
    public List<FileAccessObject> getObjects() {
        return objects;
    }

    public void setObjects(List<FileAccessObject> objects) {
        this.objects = objects;
    }

    /**
     * @return the last key returned by this fileaccess response. if populated,
     *         the results in this response are truncated
     */
    public String getLastKey() {
        return lastKey;
    }

    public void setLastKey(String lastKey) {
        this.lastKey = lastKey;
    }
}
