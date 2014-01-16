/*
 * Copyright 2014 EMC Corporation. All Rights Reserved.
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
package com.emc.atmos.api;

import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlValue;

/**
 * Represents an object ID (44+ character hexadecimal value) that is the primary identifier for an object in Atmos.
 */
public class ObjectId implements ObjectIdentifier {
    private String id;

    public ObjectId( String id ) {
        this.id = id;
    }

    @XmlValue
    public String getId() {
        return this.id;
    }

    @Override
    @XmlTransient
    public String getRelativeResourcePath() {
        return "objects/" + id;
    }

    @Override
    public String toString() {
        return getId();
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) return true;
        if ( o == null || getClass() != o.getClass() ) return false;

        ObjectId objectId = (ObjectId) o;

        if ( !id.equals( objectId.id ) ) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
