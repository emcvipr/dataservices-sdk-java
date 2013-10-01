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
import com.emc.atmos.api.bean.adapter.ObjectIdAdapter;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@XmlType( propOrder = {"objectId", "systemMetadata", "userMetadata"} )
public class ObjectEntry {
    private ObjectId objectId;
    private List<Metadata> systemMetadata;
    private List<Metadata> userMetadata;

    @XmlElement( name = "ObjectID" )
    @XmlJavaTypeAdapter( ObjectIdAdapter.class )
    public ObjectId getObjectId() {
        return objectId;
    }

    @XmlElementWrapper( name = "SystemMetadataList" )
    @XmlElement( name = "Metadata" )
    public List<Metadata> getSystemMetadata() {
        return systemMetadata;
    }

    @XmlTransient
    public Map<String, Metadata> getSystemMetadataMap() {
        if ( systemMetadata == null ) return null;
        Map<String, Metadata> metadataMap = new TreeMap<String, Metadata>();
        for ( Metadata metadata : systemMetadata ) {
            metadataMap.put( metadata.getName(), metadata );
        }
        return metadataMap;
    }

    @XmlElementWrapper( name = "UserMetadataList" )
    @XmlElement( name = "Metadata" )
    public List<Metadata> getUserMetadata() {
        return userMetadata;
    }

    @XmlTransient
    public Map<String, Metadata> getUserMetadataMap() {
        if ( userMetadata == null ) return null;
        Map<String, Metadata> metadataMap = new TreeMap<String, Metadata>();
        for ( Metadata metadata : userMetadata ) {
            metadataMap.put( metadata.getName(), metadata );
        }
        return metadataMap;
    }

    public void setObjectId( ObjectId objectId ) {
        this.objectId = objectId;
    }

    public void setSystemMetadata( List<Metadata> systemMetadata ) {
        this.systemMetadata = systemMetadata;
    }

    public void setUserMetadata( List<Metadata> userMetadata ) {
        this.userMetadata = userMetadata;
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) return true;
        if ( o == null || getClass() != o.getClass() ) return false;

        ObjectEntry that = (ObjectEntry) o;

        if ( !objectId.equals( that.objectId ) ) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return objectId.hashCode();
    }
}
