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
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.Date;

@XmlType( propOrder = {"versionNumber", "versionId", "itime"} )
public class ObjectVersion {
    private int versionNumber;
    private ObjectId versionId;
    private Date itime;

    public ObjectVersion() {
    }

    public ObjectVersion( int versionNumber, ObjectId versionId, Date iTime ) {
        this.versionNumber = versionNumber;
        this.versionId = versionId;
        this.itime = iTime;
    }

    @XmlElement( name = "itime" )
    public Date getItime() {
        return itime;
    }

    public void setItime( Date itime ) {
        this.itime = itime;
    }

    @XmlElement( name = "OID" )
    @XmlJavaTypeAdapter( ObjectIdAdapter.class )
    public ObjectId getVersionId() {
        return versionId;
    }

    public void setVersionId( ObjectId versionId ) {
        this.versionId = versionId;
    }

    @XmlElement( name = "VerNum" )
    public int getVersionNumber() {
        return versionNumber;
    }

    public void setVersionNumber( int versionNumber ) {
        this.versionNumber = versionNumber;
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) return true;
        if ( o == null || getClass() != o.getClass() ) return false;

        ObjectVersion that = (ObjectVersion) o;

        if ( versionNumber != that.versionNumber ) return false;
        if ( !versionId.equals( that.versionId ) ) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = versionNumber;
        result = 31 * result + versionId.hashCode();
        return result;
    }
}
