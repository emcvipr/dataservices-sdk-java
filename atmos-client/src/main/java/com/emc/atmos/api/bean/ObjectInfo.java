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

import javax.xml.bind.annotation.*;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.Date;
import java.util.List;

@XmlRootElement(name = "GetObjectInfoResponse")
@XmlType(propOrder = {"objectId", "selection", "numReplicas", "replicas", "retention", "expiration"})
public class ObjectInfo {
    private ObjectId objectId;
    private String selection;
    private int numReplicas;
    private List<Replica> replicas;
    private PolicyEvent retention;
    private PolicyEvent expiration;

    @XmlElement( name = "expiration" )
    public PolicyEvent getExpiration() {
        return expiration;
    }

    @XmlElement( name = "numReplicas" )
    public int getNumReplicas() {
        return numReplicas;
    }

    @XmlElement( name = "objectId" )
    @XmlJavaTypeAdapter( ObjectIdAdapter.class )
    public ObjectId getObjectId() {
        return objectId;
    }

    @XmlElementWrapper( name = "replicas" )
    @XmlElement( name = "replica" )
    public List<Replica> getReplicas() {
        return replicas;
    }

    @XmlElement( name = "retention" )
    public PolicyEvent getRetention() {
        return retention;
    }

    @XmlElement( name = "selection" )
    public String getSelection() {
        return selection;
    }

    @XmlTransient
    public Date getRetainedUntil() {
        if ( retention == null ) return null;
        return retention.getEndAt();
    }

    @XmlTransient
    public Date getExpiresAt() {
        if ( expiration == null ) return null;
        return expiration.getEndAt();
    }

    public void setExpiration( PolicyEvent expiration ) {
        this.expiration = expiration;
    }

    public void setNumReplicas( int numReplicas ) {
        this.numReplicas = numReplicas;
    }

    public void setObjectId( ObjectId objectId ) {
        this.objectId = objectId;
    }

    public void setReplicas( List<Replica> replicas ) {
        this.replicas = replicas;
    }

    public void setRetention( PolicyEvent retention ) {
        this.retention = retention;
    }

    public void setSelection( String selection ) {
        this.selection = selection;
    }
}
