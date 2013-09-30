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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

public abstract class Renderer {
    protected OutputStream outputStream;
    private String[] columnNames;

    public abstract void renderHeader( Map<String, String> headerLabels ) throws IOException;

    public abstract void renderRow( ReportRow row ) throws IOException;

    public abstract void flush() throws IOException;

    public abstract void done() throws IOException;

    public Renderer( OutputStream outputStream ) {
        this.outputStream = outputStream;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public void setColumnNames( String[] columnNames ) {
        this.columnNames = columnNames;
    }
}
