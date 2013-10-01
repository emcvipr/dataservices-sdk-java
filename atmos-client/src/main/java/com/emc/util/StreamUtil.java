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
package com.emc.util;

import java.io.*;

public class StreamUtil {
    public static String readAsString( InputStream in ) throws IOException {
        try {
            return new java.util.Scanner( in, "UTF-8" ).useDelimiter( "\\A" ).next();
        } catch ( java.util.NoSuchElementException e ) {
            return "";
        } finally {
            if ( in != null ) {
                in.close();
            }
        }
    }

    public static byte[] readAsBytes( InputStream in, int expectedLength ) throws IOException {
        try {
            byte[] output = new byte[expectedLength];

            int c = 0;
            while ( c < expectedLength ) {
                int read = in.read( output, c, expectedLength - c );
                if ( read == -1 ) {
                    // EOF!
                    throw new EOFException(
                            "EOF reading response at position " + c
                            + " size " + (expectedLength - c) );
                }
                c += read;
            }

            return output;
        } finally {
            if ( in != null ) {
                in.close();
            }
        }
    }

    public static byte[] readAsBytes( InputStream in ) throws IOException {
        try {
            byte[] buffer = new byte[4096];

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            int c = 0;
            while ( (c = in.read( buffer )) != -1 ) {
                baos.write( buffer, 0, c );
            }
            baos.close();

            return baos.toByteArray();
        } finally {
            if ( in != null ) {
                in.close();
            }
        }
    }

    /**
     * Reads from the input stream until a linefeed is encountered. All data up until that point is returned as a
     * string. If the byte preceding the linefeed is a carriage return, that is also removed from the returned value.
     * The stream is positioned immediately after the linefeed.
     */
    public static String readLine( InputStream in ) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        int c = in.read();
        if ( c == -1 || c == '\n' ) return "";
        int c2 = in.read();

        while ( c2 != -1 && (char) c2 != '\n' ) {
            baos.write( c );
            c = c2;
            c2 = in.read();
        }

        if ( (char) c != '\r' ) baos.write( c );

        return new String( baos.toByteArray(), "UTF-8" );
    }

    public static long copy( InputStream is, OutputStream os, long maxBytes ) throws IOException {
        byte[] buffer = new byte[1024 * 64]; // 64k buffer
        long count = 0;
        int read = 0, maxRead;

        while ( count < maxBytes ) {
            maxRead = (int) Math.min( (long) buffer.length, maxBytes - count );
            if ( -1 == (read = is.read( buffer, 0, maxRead )) ) break;
            os.write( buffer, 0, read );
            count += read;
        }
        return count;
    }

    private StreamUtil() {
    }
}
