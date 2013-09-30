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
 * A metadata tag identifies a piece of metadata (but not its value)
 */
public class MetadataTag {
    private String name;
    private boolean listable;
    
    /**
     * Creates a new tag
     * @param name the name of the tag
     * @param listable whether the tag is listable
     */
    public MetadataTag( String name, boolean listable ) {
        this.name = name;
        this.listable = listable;
    }
    
    /**
     * Gets the name of the tag
     * @return the tag's name
     */
    public String getName() {
        return name;
    }
    
    /**
     * Returns whether the tag is listable
     * @return the listable flag
     */
    public boolean isListable() {
        return listable;
    }
    
    /**
     * Sets whether the tag is listable
     * @param listable the new value for the listable flag.
     */
    public void setListable( boolean listable ) {
        this.listable = listable;
    }

}
