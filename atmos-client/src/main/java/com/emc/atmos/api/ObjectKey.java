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
package com.emc.atmos.api;

/**
 * Represents a bucket and key combination that points to an object within a subtenant.
 */
public class ObjectKey implements ObjectIdentifier {
    private String bucket;
    private String key;

    public ObjectKey( String bucket, String key ) {
        this.bucket = bucket;
        this.key = key;
    }

    public String getBucket() {
        return this.bucket;
    }

    public String getKey() {
        return this.key;
    }

    @Override
    public String getRelativeResourcePath() {
        return "namespace/" + key;
    }

    @Override
    public String toString() {
        return getBucket() + "/" + getKey();
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) return true;
        if ( o == null || getClass() != o.getClass() ) return false;

        ObjectKey objectKey = (ObjectKey) o;

        if ( !bucket.equals( objectKey.bucket ) ) return false;
        if ( !key.equals( objectKey.key ) ) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = bucket.hashCode();
        result = 31 * result + key.hashCode();
        return result;
    }
}
