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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CsvRenderer extends Renderer {
    private CSVPrinter printer;

    public CsvRenderer( OutputStream outputStream ) {
        super( outputStream );
        printer = new CSVPrinter( new OutputStreamWriter( outputStream ), CSVFormat.DEFAULT );
    }

    @Override
    public void renderHeader( Map<String, String> headerLabels ) throws IOException {
        List<String> headers = new ArrayList<String>();
        for ( String columnName : getColumnNames() ) {
            headers.add( headerLabels.get( columnName ) );
        }
        printer.printRecords( headers.toArray( new String[headers.size()] ) );
    }

    @Override
    public void renderRow( ReportRow row ) throws IOException {
        List<String> values = new ArrayList<String>();
        for ( String columnName : getColumnNames() ) {
            Object value = row.get( columnName );
            if ( value != null ) values.add( value.toString() );
            else values.add( "" );
        }
        printer.printRecords( values.toArray( new String[values.size()] ) );
    }

    @Override
    public void flush() throws IOException {
        printer.flush();
    }

    @Override
    public void done() throws IOException {
        printer.flush();
        outputStream.close();
        printer = null;
    }
}
