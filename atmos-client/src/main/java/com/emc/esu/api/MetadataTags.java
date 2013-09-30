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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * The MetadataTags class contains a collection of metadata tags.
 */
public class MetadataTags implements Iterable<MetadataTag> {
    private Map<String,MetadataTag> tags = new HashMap<String,MetadataTag>();
    
    /**
     * Adds a tag to the set of tags
     * @param tag the tag to add
     */
    public void addTag( MetadataTag tag ) {
        tags.put( tag.getName(), tag );
    }
    
    /**
     * Removes a tag from the set of tags
     * @param tag the tag to remove
     */
    public void removeTag( MetadataTag tag ) {
        tags.remove( tag.getName() );
    }
    
    /**
     * Gets a tag from the set with the given name
     * @param name the name to search for.
     * @return the tag or null if this set does not contain a tag with the
     * given name.
     */
    public MetadataTag getTag( String name ) {
        return tags.get( name );
    }
    
    /**
     * Returns true if this set contains a tag with the given name.
     * @param name the name to search for
     * @return true if this set contains a tag with the given name.
     */
    public boolean contains( String name ) {
        return tags.containsKey( name );
    }
    
    /**
     * Return true if this set contains the given tag object.
     * @param tag the tag to search for
     * @return true if this set contains the given tag
     */
    public boolean contains( MetadataTag tag ) {
        return tags.containsValue( tag );
    }

    /**
     * Returns an iterator that iterates over the set of tags.
     */
    public Iterator<MetadataTag> iterator() {
        return tags.values().iterator();
    }
    
    /**
     * Returns the number of tags in this set
     * @return the tag count
     */
    public int count() {
        return tags.size();
    }
    
}
