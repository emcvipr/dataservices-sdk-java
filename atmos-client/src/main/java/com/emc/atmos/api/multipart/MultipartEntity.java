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
package com.emc.atmos.api.multipart;

import com.emc.atmos.api.Range;
import com.emc.util.StreamUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a multipart response entity.
 */
public class MultipartEntity extends ArrayList<MultipartPart> {
    private static final long serialVersionUID = -4788353053749563899L;

    private static final Pattern PATTERN_CONTENT_TYPE = Pattern.compile( "^Content-Type: (.+)$" );
    private static final Pattern PATTERN_CONTENT_RANGE = Pattern.compile( "^Content-Range: bytes (\\d+)-(\\d+)/(\\d+)$" );

    /**
     * Parses a multipart response body provided by an InputStream. Returns an instance of this class that represents
     * the response. boundary may start with "--" or omit it.
     */
    public static MultipartEntity fromStream( InputStream is, String boundary ) throws IOException {
        if ( boundary.startsWith( "--" ) ) boundary = boundary.substring( 2 );

        List<MultipartPart> parts = new ArrayList<MultipartPart>();

        try {
            while ( true ) {

                // first, we expect a boundary ( EOL + '--' + <boundary_string> + EOL )
                if ( !"".equals( StreamUtil.readLine( is ) ) )
                    throw new MultipartException( "Parse error: expected EOL before boundary" );
                String line = StreamUtil.readLine( is );

                // two dashes after the boundary means EOS
                if ( ("--" + boundary + "--").equals( line ) ) break;

                if ( !("--" + boundary).equals( line ) ) throw new MultipartException(
                        "Parse error: expected [--" + boundary + "], instead got [" + line + "]" );

                Matcher matcher;
                String contentType = null;
                int start = -1, end = 0, length = 0;
                while ( !"".equals( line = StreamUtil.readLine( is ) ) ) {
                    matcher = PATTERN_CONTENT_TYPE.matcher( line );
                    if ( matcher.find() ) {
                        contentType = matcher.group( 1 );
                        continue;
                    }

                    matcher = PATTERN_CONTENT_RANGE.matcher( line );
                    if ( matcher.find() ) {
                        start = Integer.parseInt( matcher.group( 1 ) );
                        end = Integer.parseInt( matcher.group( 2 ) );
                        length = end - start + 1;
                        // total = Integer.parseInt( matcher.group( 3 ) );
                        continue;
                    }

                    throw new MultipartException( "Unrecognized header line: " + line );
                }

                if ( contentType == null )
                    throw new MultipartException( "Parse error: No content-type specified in part" );

                if ( start == -1 )
                    throw new MultipartException( "Parse error: No content-range specified in part" );

                // then the data of the part
                byte[] data = new byte[length];
                int read, count = 0;
                while ( count < length ) {
                    read = is.read( data, 0, length - count );
                    count += read;
                }

                parts.add( new MultipartPart( contentType, new Range( start, end ), data ) );
            }
        } finally {
            is.close();
        }

        return new MultipartEntity( parts );
    }

    public MultipartEntity( List<MultipartPart> parts ) {
        super( parts );
    }

    /**
     * Convenience method that aggregates the bytes of all parts into one contiguous byte array.
     */
    public byte[] aggregateBytes() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            for ( MultipartPart part : this ) {
                baos.write( part.getData() );
            }
            return baos.toByteArray();
        } catch ( IOException e ) {
            throw new RuntimeException( "Unexpected error", e ); // unrecoverable
        }
    }
}
