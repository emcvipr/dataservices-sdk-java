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
package com.emc.atmos.util;

import com.sun.jersey.core.header.FormDataContentDisposition;
import com.sun.jersey.core.header.reader.HttpHeaderReader;

import java.text.ParseException;
import java.util.Date;

public class ReorderedFormDataContentDisposition extends FormDataContentDisposition {
    public ReorderedFormDataContentDisposition( String type,
                                                String name,
                                                String fileName,
                                                Date creationDate,
                                                Date modificationDate, Date readDate, long size ) {
        super( type, name, fileName, creationDate, modificationDate, readDate, size );
    }

    public ReorderedFormDataContentDisposition( String header ) throws ParseException {
        super( header );
    }

    public ReorderedFormDataContentDisposition( HttpHeaderReader reader )
            throws ParseException {
        super( reader );
    }

    @Override
    protected StringBuilder toStringBuffer() {
        StringBuilder sb = new StringBuilder();
        sb.append( getType() );
        addStringParameter( sb, "name", getName() );
        addStringParameter( sb, "filename", getFileName() );
        addDateParameter( sb, "creation-date", getCreationDate() );
        addDateParameter( sb, "modification-date", getModificationDate() );
        addDateParameter( sb, "read-date", getReadDate() );
        addLongParameter( sb, "size", getSize() );
        return sb;
    }
}
