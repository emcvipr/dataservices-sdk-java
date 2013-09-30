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

public class FileAccessObject {
    private String name;
    private String deviceExport;
    private String relativePath;
    private String owner;

    /**
     * The internal name given to the object. This is not necessarily the key,
     * but is assumed to be unique.
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * The mount point on which the object is located.
     */
    public String getDeviceExport() {
        return deviceExport;
    }

    public void setDeviceExport(String deviceExport) {
        this.deviceExport = deviceExport;
    }

    /**
     * The path to the object relative to its mount point.
     */
    public String getRelativePath() {
        return relativePath;
    }

    public void setRelativePath(String relativePath) {
        this.relativePath = relativePath;
    }

    /**
     * The UID of the Unix owner of the file representing the object.
     */
    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }
}
