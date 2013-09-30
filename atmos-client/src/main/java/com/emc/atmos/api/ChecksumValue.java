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
 * Represents a checksum value. Can yield the algorithm, offset (if applicable) and hex-value of the sum.
 */
public abstract class ChecksumValue {
    public abstract ChecksumAlgorithm getAlgorithm();

    public abstract long getOffset();

    public abstract String getValue();

    /**
     * Outputs this checksum in a format suitable for including in Atmos create/update calls.
     */
    @Override
    public String toString() {
        return toString( true );
    }

    public String toString( boolean includeByteCount ) {
        String out = this.getAlgorithm().toString();
        if ( includeByteCount ) out += "/" + this.getOffset();
        out += "/" + getValue();
        return out;
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) return true;
        if ( !(o instanceof ChecksumValue) ) return false;

        ChecksumValue that = (ChecksumValue) o;

        if ( getOffset() != that.getOffset() ) return false;
        if ( getAlgorithm() != that.getAlgorithm() ) return false;
        if ( !getValue().equals( that.getValue() ) ) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = getAlgorithm().hashCode();
        result = 31 * result + (int) (getOffset() ^ (getOffset() >>> 32));
        result = 31 * result + getValue().hashCode();
        return result;
    }
}
