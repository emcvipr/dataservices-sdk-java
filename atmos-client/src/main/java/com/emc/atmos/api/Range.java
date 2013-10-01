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

public class Range {
    private long first, last;

    public Range( long first, long last ) {
        this.first = first;
        this.last = last;
    }

    public long getFirst() {
        return first;
    }

    public long getLast() {
        return last;
    }

    public long getSize() {
        return last - first + 1;
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) return true;
        if ( o == null || getClass() != o.getClass() ) return false;

        Range range = (Range) o;

        if ( first != range.first ) return false;
        if ( last != range.last ) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (first ^ (first >>> 32));
        result = 31 * result + (int) (last ^ (last >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "" + first + '-' + last;
    }
}
