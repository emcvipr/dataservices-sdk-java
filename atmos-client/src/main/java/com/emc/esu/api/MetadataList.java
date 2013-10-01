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
 * Contains a list of metadata items
 */
public class MetadataList implements Iterable<Metadata> {
    private Map<String,Metadata> meta = new HashMap<String,Metadata>();

    /**
     * Returns an iterator that iterates over the set of metadata items in
     * the list.
     */
    public Iterator<Metadata> iterator() {
        return meta.values().iterator();
    }
    
    /**
     * Adds a metadata item to the list
     * @param m metadata to add
     */
    public void addMetadata( Metadata m ) {
        meta.put( m.getName(), m );
    }
    
    /**
     * Removes a metadata item from the list
     * @param m metadata to remove
     */
    public void removeMetadata( Metadata m ) {
        meta.remove( m.getName() );
    }
    
    /**
     * Returns the metadata item with the specified name
     * @param name name to search for
     * @return the metadata with the given name or null if the list does not
     * contain metadata with the requested name.
     */
    public Metadata getMetadata( String name ) {
        return meta.get( name );
    }
    
    /**
     * Returns the number of items in the metadata list.
     * @return the item count
     */
    public int count() {
        return meta.size();
    }

}
