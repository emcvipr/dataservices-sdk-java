/*
 * Copyright 2014 EMC Corporation. All Rights Reserved.
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
package com.emc.acdp.report.test;

import com.emc.acdp.report.CsvRenderer;
import com.emc.acdp.report.ReportRow;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

public class TestCsvRenderer {
    public static final String CRLF = "\r\n";
    public static final String[] COLUMN_NAMES = new String[]{"one", "two", "three", "four", "five"};
    public static final String[] ROW1_VALUES = new String[]{"1", "2", "3", "4", "5"};
    public static final String[] ROW2_VALUES = new String[]{"A", "B", "C", "D", "E"};
    public static final String[] ROW3_VALUES = new String[]{"alpha", "bravo", "charlie", "delta", "echo"};
    public static final String[] ROW4_VALUES = new String[]{"uno", "dos", "tres", "cuatro", "cinco"};
    public static final String[] ROW5_VALUES = new String[]{"eins", "zwei", "drei", "vier", "funf"};
    public static final String CSV = join(upper(COLUMN_NAMES)) + CRLF
            + join(ROW1_VALUES) + CRLF
            + join(ROW2_VALUES) + CRLF
            + join(ROW3_VALUES) + CRLF
            + join(ROW4_VALUES) + CRLF
            + join(ROW5_VALUES) + CRLF;

    public static Map<String, String> headerLabels = new HashMap<String, String>();
    public static ReportRow[] reportRows = new ReportRow[5];
    public static int rowCount = 0;

    static {
        for (String columnName : COLUMN_NAMES) {
            headerLabels.put(columnName, columnName.toUpperCase());
        }
        reportRows[0] = createReportRow(ROW1_VALUES);
        reportRows[1] = createReportRow(ROW2_VALUES);
        reportRows[2] = createReportRow(ROW3_VALUES);
        reportRows[3] = createReportRow(ROW4_VALUES);
        reportRows[4] = createReportRow(ROW5_VALUES);
    }

    private static ReportRow createReportRow(String[] values) {
        ReportRow reportRow = new ReportRow(++rowCount);
        for (int i = 0; i < COLUMN_NAMES.length; i++) {
            reportRow.put(COLUMN_NAMES[i], values[i]);
        }
        return reportRow;
    }

    private static String join(String[] values) {
        StringBuilder builder = new StringBuilder();
        for (String value : values) {
            if (builder.length() > 0) builder.append(",");
            builder.append(value);
        }
        return builder.toString();
    }

    private static String[] upper(String[] values) {
        String[] uppers = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            uppers[i] = values[i].toUpperCase();
        }
        return uppers;
    }

    @Test
    public void testCsvRenderer() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        CsvRenderer renderer = new CsvRenderer(os);
        renderer.setColumnNames(COLUMN_NAMES);
        renderer.renderHeader(headerLabels);
        for (ReportRow row : reportRows) {
            renderer.renderRow(row);
        }
        renderer.done();
        Assert.assertEquals("CSV output incorrect", CSV, os.toString());
    }
}
