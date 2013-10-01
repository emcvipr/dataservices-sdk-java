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

import com.emc.util.HttpUtil;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

@XmlType( propOrder = {"name", "value", "listable"} )
public class Metadata {
    private String name;
    private String value;
    private boolean listable;

    public Metadata() {
    }

    public Metadata( String name, String value, boolean listable ) {
        this.name = name;
        this.value = value;
        this.listable = listable;
    }

    @XmlElement( name = "Name" )
    public String getName() {
        return name;
    }

    public void setName( String name ) {
        this.name = name;
    }

    @XmlElement( name = "Value" )
    public String getValue() {
        return value;
    }

    public void setValue( String value ) {
        this.value = value;
    }

    @XmlElement( name = "Listable" )
    public boolean isListable() {
        return listable;
    }

    public void setListable( boolean listable ) {
        this.listable = listable;
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) return true;
        if ( o == null || getClass() != o.getClass() ) return false;

        Metadata metadata = (Metadata) o;

        if ( !name.equals( metadata.name ) ) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        String value = (this.value == null) ? "" : this.value;
        return name + "=" + value;
    }

    public String toASCIIString() {
        String value = (this.value == null) ? "" : this.value;
        return HttpUtil.encodeUtf8( name ) + "=" + HttpUtil.encodeUtf8( value );
    }
}
