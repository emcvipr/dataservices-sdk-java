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

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public final class HttpUtil {
    private static final String HEADER_FORMAT = "EEE, d MMM yyyy HH:mm:ss z";
    private static final ThreadLocal<DateFormat> headerFormat = new ThreadLocal<DateFormat>();
    private static final String ISO_8601_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private static final ThreadLocal<DateFormat> iso8601Format = new ThreadLocal<DateFormat>();
    private static final Logger l4j = Logger.getLogger( HttpUtil.class );

    public static synchronized String headerFormat( Date date ) {
        return getHeaderFormat().format( date );
    }

    public static String encodeUtf8( String value ) {
        // Use %20, not +
        try {
            return URLEncoder.encode( value, "UTF-8" ).replace( "+", "%20" );
        } catch ( UnsupportedEncodingException e ) {
            throw new RuntimeException( "UTF-8 encoding isn't supported on this system", e ); // unrecoverable
        }
    }

    public static String decodeUtf8( String value ) {
        try {
            // don't want '+' decoded to a space
            return URLDecoder.decode( value.replace( "+", "%2B" ), "UTF-8" );
        } catch ( UnsupportedEncodingException e ) {
            throw new RuntimeException( "UTF-8 encoding isn't supported on this system", e ); // unrecoverable
        }
    }

    /**
     * Reads the response body and returns it as a string.
     *
     * @param con the HTTP connection
     *
     * @return the string containing the response body
     *
     * @throws java.io.IOException if reading the response stream fails
     */
    public static String readResponseString( HttpURLConnection con )
            throws IOException {
        InputStream in = null;
        if ( con.getResponseCode() > 299 ) {
            in = con.getErrorStream();
        }
        if ( in == null ) {
            in = con.getInputStream();
        }
        if ( in == null ) {
            // could not get stream
            return "";
        }
        return StreamUtil.readAsString( in );
    }

    /**
     * Reads the response body and returns it in a byte array.
     *
     * @param con the HTTP connection
     *
     * @return the byte array containing the response body. Note that if you
     *         pass in a buffer, this will the same buffer object. Be sure to
     *         check the content length to know what data in the buffer is valid
     *         (from zero to contentLength).
     *
     * @throws IOException if reading the response stream fails.
     */
    public static byte[] readResponse( HttpURLConnection con )
            throws IOException {
        InputStream in = null;
        if ( con.getResponseCode() > 299 ) {
            in = con.getErrorStream();
            if ( in == null ) {
                in = con.getInputStream();
            }
        } else {
            in = con.getInputStream();
        }
        if ( in == null ) {
            // could not get stream
            return new byte[0];
        }
        int contentLength = con.getContentLength();
        // If we know the content length, read it directly into a buffer.
        if ( contentLength != -1 ) {
            return StreamUtil.readAsBytes( in, contentLength );

            // Else, use a ByteArrayOutputStream to collect the response.
        } else {
            l4j.debug( "Content length is unknown.  Buffering output." );
            byte[] data = StreamUtil.readAsBytes( in );
            l4j.debug( "Buffered " + data.length + " response bytes" );
            return data;
        }
    }

    /**
     * Simply writes the given content to the given connection's output stream.
     * WARNING: DO NOT connect the connection before calling this method (it will be connected here)
     */
    public static void writeRequest( HttpURLConnection con, String content ) throws IOException {
        con.setDoOutput( true );
        con.setFixedLengthStreamingMode( content.getBytes( "UTF-8" ).length );
        con.connect();
        OutputStreamWriter writer = new OutputStreamWriter( con.getOutputStream() );
        writer.write( content );
        writer.flush();
        writer.close();
    }

    private static DateFormat getHeaderFormat() {
        DateFormat format = headerFormat.get();
        if ( format == null ) {
            format = new SimpleDateFormat( HEADER_FORMAT, Locale.ENGLISH );
            format.setTimeZone( TimeZone.getTimeZone( "UTC" ) );
            headerFormat.set( format );
        }
        return format;
    }

    public static DateFormat get8601Format() {
        DateFormat format = iso8601Format.get();
        if ( format == null ) {
            format = new SimpleDateFormat( ISO_8601_FORMAT );
            format.setTimeZone( TimeZone.getTimeZone( "UTC" ) );
            iso8601Format.set( format );
        }
        return format;
    }
}
