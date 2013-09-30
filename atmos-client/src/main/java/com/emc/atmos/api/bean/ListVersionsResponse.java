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

import com.emc.atmos.api.ObjectId;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement( name = "ListVersionsResponse" )
public class ListVersionsResponse extends BasicResponse {
    private List<ObjectVersion> versions;

    @XmlElement( name = "Ver" )
    public List<ObjectVersion> getVersions() {
        return versions;
    }

    public void setVersions( List<ObjectVersion> versions ) {
        this.versions = versions;
    }

    @XmlTransient
    public List<ObjectId> getVersionIds() {
        List<ObjectId> vIds = new ArrayList<ObjectId>();
        for ( ObjectVersion version : versions ) {
            vIds.add( version.getVersionId() );
        }
        return vIds;
    }
}
