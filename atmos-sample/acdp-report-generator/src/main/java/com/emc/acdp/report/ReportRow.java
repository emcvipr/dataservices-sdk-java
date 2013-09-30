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
package com.emc.acdp.report;

import java.util.TreeMap;

public class ReportRow extends TreeMap<String, Object> implements Comparable<ReportRow> {
    private int index;
    private int subIndex = 0;

    public ReportRow( int index ) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex( int index ) {
        this.index = index;
    }

    public int getSubIndex() {
        return subIndex;
    }

    public void setSubIndex( int subIndex ) {
        this.subIndex = subIndex;
    }

    @Override
    public int compareTo( ReportRow reportRow ) {
        int result = this.getIndex() - reportRow.getIndex();
        if ( result == 0 )
            result = this.getSubIndex() - reportRow.getSubIndex();
        return result;
    }

    public ReportRow clone() {
        ReportRow clone = new ReportRow( index );
        clone.putAll( this );
        return clone;
    }
}
