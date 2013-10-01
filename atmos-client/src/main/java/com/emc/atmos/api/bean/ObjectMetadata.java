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

import java.util.Map;

public class ObjectMetadata {
    private Map<String, Metadata> metadata;
    private Acl acl;
    private String contentType;

    public ObjectMetadata() {
    }

    public ObjectMetadata( Map<String, Metadata> metadata, Acl acl, String contentType ) {
        this.metadata = metadata;
        this.acl = acl;
        this.contentType = contentType;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType( String contentType ) {
        this.contentType = contentType;
    }

    public Acl getAcl() {
        return acl;
    }

    public void setAcl( Acl acl ) {
        this.acl = acl;
    }

    public Map<String, Metadata> getMetadata() {
        return metadata;
    }

    public void setMetadata( Map<String, Metadata> metadata ) {
        this.metadata = metadata;
    }
}
