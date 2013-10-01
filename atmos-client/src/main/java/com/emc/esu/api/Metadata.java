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
package com.emc.esu.api;

/**
 * Encapsulates a piece of object metadata
 */
public class Metadata {
    private String name;
    private String value;
    private boolean listable;
    
    /**
     * Creates a new piece of metadata
     * @param name the name of the metadata (e.g. 'Title')
     * @param value the metadata value (e.g. 'Hamlet')
     * @param listable whether to make the value listable.  You can
     * query objects with a specific listable metadata tag using the listObjects
     * method in the API.
     */
    public Metadata( String name, String value, boolean listable ) {
            this.name = name;
            this.value = value;
            this.listable = listable;
    }
    
    /**
     * Returns a string representation of the metadata.
     * @return the metadata object in the format name=value.  Listable
     * metadata will appear as name(listable)=value
     */
    public String toString() {
            return name + (listable?"(listable)":"") + "=" + value;
    }
    
    /**
     * Returns the name of the metadata object
     */
    public String getName() {
            return name;
    }
    
    /**
     * Returns the metadata object's value
     */
    public String getValue() {
            return value;
    }
    
    /**
     * Sets the metadata's value.  Use updateObject to change this value on
     * the server.
     */
    public void setValue( String value ) {
            this.value = value;  
    }
    
    /**
     * Returns true if this metadata object is listable
     */
    public boolean isListable() {
            return listable;
    }
    
    /**
     * Sets the value of the listable flag.
     * @param listable whether this metadata object is listable.
     */
    public void setListable( boolean listable ) {
            this.listable = listable;
    }
}
