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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement( namespace = "", name = "access-token" )
public class AccessToken extends AccessTokenPolicy {
    private String id;
    private String path;
    private String objectId;

    @XmlElement( namespace = "", name = "access-token-id" )
    public String getId() {
        return id;
    }

    @XmlElement( namespace = "", name = "path" )
    public String getPath() {
        return this.path;
    }

    @XmlElement( namespace = "", name = "object-id" )
    public String getObjectId() {
        return this.objectId;
    }

    public void setId( String id ) {
        this.id = id;
    }

    public void setPath( String path ) {
        this.path = path;
    }

    public void setObjectId( String objectId ) {
        this.objectId = objectId;
    }
}
