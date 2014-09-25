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
package com.emc.atmos.api.bean;

import com.emc.atmos.api.Acl;
import com.emc.atmos.api.ChecksumValue;

import java.util.Map;

public class ObjectMetadata {
    private Map<String, Metadata> metadata;
    private Acl acl;
    private String contentType;
    private ChecksumValue wsChecksum;
    private ChecksumValue serverChecksum;

    public ObjectMetadata() {
    }

    public ObjectMetadata(Map<String, Metadata> metadata, Acl acl, String contentType,
                          ChecksumValue wsChecksum, ChecksumValue serverChecksum) {
        this.metadata = metadata;
        this.acl = acl;
        this.contentType = contentType;
        this.wsChecksum = wsChecksum;
        this.serverChecksum = serverChecksum;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public Acl getAcl() {
        return acl;
    }

    public void setAcl(Acl acl) {
        this.acl = acl;
    }

    public Map<String, Metadata> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Metadata> metadata) {
        this.metadata = metadata;
    }

    /**
     * Returns the wschecksum if that feature was enabled for this object.
     */
    public ChecksumValue getWsChecksum() {
        return wsChecksum;
    }

    public void setWsChecksum(ChecksumValue wsChecksum) {
        this.wsChecksum = wsChecksum;
    }

    /**
     * Returns the last server-generated checksum for a single update to this object.
     *
     * @see com.emc.atmos.api.request.PutObjectRequest#setServerGeneratedChecksumAlgorithm(com.emc.atmos.api.ChecksumAlgorithm)
     */
    public ChecksumValue getServerChecksum() {
        return serverChecksum;
    }

    public void setServerChecksum(ChecksumValue serverChecksum) {
        this.serverChecksum = serverChecksum;
    }
}
