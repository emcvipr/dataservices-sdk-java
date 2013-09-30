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
 * Used to grant a permission to a grantee (a user or group)
 */
public class Grant {
    // Developer Note
    // --------------
    // Grants are immutable because changing their values will change their
    // hashcode.  If the hashcode is changed, the Acl's Set of Grant objects
    // will likely break it's contains() method because the HashSet searches
    // for objects by first attempting to locate their bucket by hashcode.  If
    // the hashcode changes, the bucket will likely change and therefore the
    // hashset will look in the wrong bucket when calling contains().  The
    // result of breaking contains() is that Acl equals() will also break.

    
    private Grantee grantee;
    private String permission;
    

    /**
     * Creates a new grant
     * @param grantee the recipient of the permission
     * @param permission the rights to grant to the grantee.  Use
     * the constants in the Permission class.
     */
    public Grant( Grantee grantee, String permission ) { 
        this.grantee = grantee;
        this.permission = permission;
    }
    
    /**
     * Gets the recipient of the grant
     * @return the grantee
     */
    public Grantee getGrantee() {
        return grantee;
    }
    
    /**
     * Gets the rights assigned the grantee
     * @return the permissions assigned
     */
    public String getPermission() {
        return permission;
    }
        
    /**
     * Returns the grant in string form: grantee=permission
     */
    public String toString() {
        return grantee.getName() + "=" + permission;
    }
    
    /**
     * Checks to see if grants are equal.  This is true if the grantee and
     * permission are equal.
     */
    public boolean equals( Object obj ) {
        if( !(obj instanceof Grant ) ) {
            return false;
        }
        Grant g = (Grant) obj;
        return g.permission.equals(permission) && g.grantee.equals( grantee );
    }
    
    /**
     * Returns a hash code for the Grant.
     */
    public int hashCode() {
        return toString().hashCode();
    }
}
