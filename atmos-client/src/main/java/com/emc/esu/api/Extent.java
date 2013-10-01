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
 * An extent specifies a portion of an object to read or write.  It contains
 * a starting offset and a number of bytes to read or write.
 */
public class Extent {
    /**
     * A static instance representing an entire object's content.
     */
    public static final Extent ALL_CONTENT = new Extent( -1, -1 );
    
    private long offset;
    private long size;
    
    /**
     * Creates a new extent
     * @param offset the starting offset in the object in bytes, 
     * starting with 0.  Use -1 to represent the entire object.
     * @param size the number of bytes to transfer.  Use -1 to represent
     * the entire object.
     */
    public Extent( long offset, long size ) {
            this.offset = offset;
            this.size = size;
    }
    
    /**
     * Returns the size of the extent.
     * @return the extent's size
     */
    public long getSize() {
            return this.size;
    }
    
    /**
     * Returns the starting offset of the extent
     * @return the extent's starting offset
     */
    public long getOffset() {
            return this.offset;
    }

    /**
     * Compares two extents.  Returns true if they are equal.
     */
    public boolean equals(Object obj) {
        if( !(obj instanceof Extent ) ) {
            return false;
        }
        
        Extent b = (Extent)obj;
        return b.getOffset() == offset && b.getSize() == size;
    }
    
    public String toString() {
        long end = offset + (size-1);
        return "bytes=" + offset + "-" + end;
    }

    public String getHeaderName() {
        return "Range";
    }
    

}
