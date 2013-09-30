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
 * Represents an object that is identified by its path in the 
 * filesystem namespace.  If the object path ends in a "/", it
 * is considered a directory object.  If it does not, it is a 
 * regular object.
 */
public class ObjectPath implements Identifier {

    /**
     * Stores the string representation of the identifier
     */
    private String path;

    /**
     * Constructs a new object identifier
     * @param path the object ID as a string
     */
    public ObjectPath( String path ) {
        this.path = path;
    }
    
    /**
     * Returns the identifier as a string
     * @return the identifier as a string
     */
    public String toString() {
        return path;
    }
    
    /**
     * Returns true if the object IDs are equal.
     */
    public boolean equals( Object obj ) {
        if( !(obj instanceof ObjectPath) ) {
            return false;
        }
        
        return path.equals( ((ObjectPath)obj).toString() );
        
    }
    
    /**
     * Gets the name of the object (the last path component)
     */
    public String getName() {
    	if( isDirectory() ) {
    		if( path.equals( "/" ) ) {
    			return "";
    		} else {
    			int slash = path.substring(0, path.length()-1 ).lastIndexOf( '/' );
    			return path.substring( slash+1, path.length()-1 );
    		}
    	} else {
    		int slash = path.lastIndexOf( '/' );
    		return path.substring( slash+1, path.length() );
    	}
    }
    
    /**
     * Returns a hash code for this object id.
     */
    public int hashCode() {
        return path.hashCode();
    }
    
    /**
     * Returns true if this path represents a directory object.
     */
    public boolean isDirectory() {
    	return path.endsWith( "/" );
    }
}
