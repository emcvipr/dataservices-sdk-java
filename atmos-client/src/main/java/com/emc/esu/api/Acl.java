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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * An Access Control List (ACL) is a collection of Grants that assign privileges
 * to users and/or groups.
 */
public class Acl implements Iterable<Grant> {
    private Set<Grant> grants;

    /**
     * Creates a new access control list
     */
    public Acl() {
        this.grants = new HashSet<Grant>();
    }
    
    
    /**
     * Adds a grant to the access control list
     * @param g the grant to add.
     */
    public void addGrant( Grant g ) {
        grants.add( g );
    }
    
    /**
     * Removes a grant from the access control list.
     * @param g the grant to remove
     */
    public void removeGrant( Grant g ) {
        grants.remove( g );
    }
    
    public int count() {
        return grants.size();
    }

    /**
     * Returns an iterator over this ACL's grant objects.
     */
    public Iterator<Grant> iterator() {
        return grants.iterator();
    }
    
    /**
     * Clears all the grants in the ACL.
     */
    public void clear() {
        grants.clear();
    }
    
    /**
     * Returns true if the ACLs are equal.  This is done by ensuring they
     * have the same number of grants and each ACL contains the same
     * set of grants.
     */
    public boolean equals( Object obj ) {
        if( !( obj instanceof Acl ) ) {
            return false;
        }
        
        Acl acl2 = (Acl)obj;

        return this.grants.equals( acl2.grants );
    }
    
    /**
     * Returns the ACL's grant set as a String.
     */
    public String toString() {
        return grants.toString();
    }

    /**
     * Returns true if this ACL contains the specified Grant.
     * @param g the grant to check for
     * @return true if this ACL contains the grant.
     */
    public boolean contains(Grant g) {
        return grants.contains( g );
    }
    
}
