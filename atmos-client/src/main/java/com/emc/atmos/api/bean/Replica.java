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

import javax.xml.bind.annotation.XmlType;

@XmlType( propOrder = {"id", "type", "current", "location", "storageType"} )
public class Replica {
    private int id;
    private String type;
    private boolean current;
    private String location;
    private String storageType;

    public boolean isCurrent() {
        return current;
    }

    public void setCurrent( boolean current ) {
        this.current = current;
    }

    public int getId() {
        return id;
    }

    public void setId( int id ) {
        this.id = id;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation( String location ) {
        this.location = location;
    }

    public String getStorageType() {
        return storageType;
    }

    public void setStorageType( String storageType ) {
        this.storageType = storageType;
    }

    public String getType() {
        return type;
    }

    public void setType( String type ) {
        this.type = type;
    }
}
