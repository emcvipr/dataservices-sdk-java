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
 * A grantee represents a user or group that recieves a permission grant.
 */
public class Grantee {
    public static enum GRANT_TYPE { USER, GROUP };
    
    /**
     * Static instance that represents the special group 'other'
     */
    public static final Grantee OTHER = new Grantee( "other", GRANT_TYPE.GROUP );
    
    private String name;
    private GRANT_TYPE type;
    
    /**
     * Creates a new grantee.
     * @param name the name of the user or group
     * @param type the type of grantee, e.g. USER or GROUP.  Use the enum in 
     * this class to specify the type of grantee
     */
    public Grantee( String name, GRANT_TYPE type ) {
        this.name = name;
        this.type = type;
    }
    
    /**
     * Gets the grantee's name
     * @return the name of the grantee
     */
    public String getName() {
        return name;
    }
    
    /**
     * Gets the grantee's type.  You can compare this value to the enum
     * @return the type of grantee.
     */
    public GRANT_TYPE getType() {
        return type;
    }
    
    /**
     * Checks to see if a Grantee is equal to another.  Returns true if both
     * the names and types are equal.
     */
    public boolean equals( Object obj ) {
        if( !( obj instanceof Grantee ) ) {
            return false;
        }
        
        Grantee g = (Grantee)obj;
        
        return g.getName().equals( name ) && g.getType() == type;
    }
    
    /**
     * Returns a hash code for the Grantee.
     */
    public int hashCode() {
        return toString().hashCode();
    }

}
