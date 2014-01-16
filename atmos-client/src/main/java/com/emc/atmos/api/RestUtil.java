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
package com.emc.atmos.api;

import com.emc.atmos.AtmosException;
import com.emc.atmos.api.bean.Metadata;
import com.emc.atmos.api.bean.Permission;
import com.emc.util.HttpUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class RestUtil {
    public static final String HEADER_CONTENT_TYPE = "Content-Type";
    public static final String HEADER_DATE = "Date";
    public static final String HEADER_EXPECT = "Expect";
    public static final String HEADER_RANGE = "Range";

    public static final String XHEADER_CONTENT_CHECKSUM = "x-emc-content-checksum";
    public static final String XHEADER_DATE = "x-emc-date";
    public static final String XHEADER_EXPIRES = "x-emc-expires";
    public static final String XHEADER_FEATURES = "x-emc-features";
    public static final String XHEADER_FORCE = "x-emc-force";
    public static final String XHEADER_GENERATE_CHECKSUM = "x-emc-generate-checksum";
    public static final String XHEADER_GROUP_ACL = "x-emc-groupacl";
    public static final String XHEADER_INCLUDE_META = "x-emc-include-meta";
    public static final String XHEADER_LIMIT = "x-emc-limit";
    public static final String XHEADER_LISTABLE_META = "x-emc-listable-meta";
    public static final String XHEADER_LISTABLE_TAGS = "x-emc-listable-tags";
    public static final String XHEADER_META = "x-emc-meta";
    public static final String XHEADER_OBJECTID = "x-emc-objectid";
    public static final String XHEADER_PATH = "x-emc-path";
    public static final String XHEADER_POOL = "x-emc-pool";
    public static final String XHEADER_SIGNATURE = "x-emc-signature";
    public static final String XHEADER_SUPPORT_UTF8 = "x-emc-support-utf8";
    public static final String XHEADER_SYSTEM_TAGS = "x-emc-system-tags";
    public static final String XHEADER_TAGS = "x-emc-tags";
    public static final String XHEADER_TOKEN = "x-emc-token";
    public static final String XHEADER_UID = "x-emc-uid";
    public static final String XHEADER_USER_ACL = "x-emc-useracl";
    public static final String XHEADER_USER_TAGS = "x-emc-user-tags";
    public static final String XHEADER_UTF8 = "x-emc-utf8";
    public static final String XHEADER_VERSION_OID = "x-emc-version-oid";
    public static final String XHEADER_WSCHECKSUM = "x-emc-wschecksum";
    public static final String XHEADER_PROJECT = "x-emc-project-id";
    public static final String XHEADER_OBJECT_VPOOL = "x-emc-vpool";

    public static final String TYPE_MULTIPART = "multipart";
    public static final String TYPE_MULTIPART_BYTE_RANGES = "multipart/byteranges";
    public static final String TYPE_APPLICATION_OCTET_STREAM = "application/octet-stream";

    public static final String TYPE_DEFAULT = TYPE_APPLICATION_OCTET_STREAM;

    public static final String TYPE_PARAM_BOUNDARY = "boundary";

    public static final String PROP_ENABLE_EXPECT_100_CONTINUE = "com.emc.atmos.api.expect100Continue";

    private static final Logger l4j = Logger.getLogger( RestUtil.class );

    private static final Pattern OBJECTID_PATTERN = Pattern.compile( "/\\w+/objects/([0-9a-f]{44,})" );

    public static String sign( String string, byte[] hashKey ) {
        try {
            // Compute the signature hash
            l4j.debug( "Hashing: \n" + string );

            byte[] input = string.getBytes( "UTF-8" );

            Mac mac = Mac.getInstance( "HmacSHA1" );
            SecretKeySpec key = new SecretKeySpec( hashKey, "HmacSHA1" );
            mac.init( key );

            byte[] hashBytes = mac.doFinal( input );

            // Encode the hash in Base64.
            String hash = new String( Base64.encodeBase64( hashBytes ), "UTF-8" );

            l4j.debug( "Hash: " + hash );

            return hash;
        } catch ( Exception e ) {
            throw new RuntimeException( "Error signing string:\n" + string + "\n", e );
        }
    }

    /**
     * Generates the HMAC-SHA1 signature used to authenticate the request using
     * the Java security APIs, then adds the uid and signature to the headers.
     *
     * @param method  the HTTP method used
     * @param path    the resource path including any querystring
     * @param headers the HTTP headers for the request
     * @param hashKey the secret key to use when signing
     */
    public static void signRequest( String method, String path, String query, Map<String, List<Object>> headers,
                                    String uid, byte[] hashKey, long serverClockSkew ) {

        // Add date header
        Date serverTime = new Date( System.currentTimeMillis() - serverClockSkew );
        headers.put( HEADER_DATE, Arrays.asList( (Object) HttpUtil.headerFormat( serverTime ) ) );
        headers.put( XHEADER_DATE, Arrays.asList( (Object) HttpUtil.headerFormat( serverTime ) ) );

        // Add uid to headers
        if ( !headers.containsKey( XHEADER_UID ) )
            headers.put( XHEADER_UID, Arrays.asList( (Object) uid ) );

        // Build the string to hash.
        StringBuilder builder = new StringBuilder();

        builder.append( method ).append( "\n" );

        // Add the following header values or blank lines if they aren't present
        builder.append( generateHashLine( headers, HEADER_CONTENT_TYPE ) );
        builder.append( generateHashLine( headers, HEADER_RANGE ) );
        builder.append( generateHashLine( headers, HEADER_DATE ) );

        // Add the resource
        builder.append( path.toLowerCase() );
        if ( query != null ) builder.append( "?" ).append( query );
        builder.append( "\n" );

        // Do the 'x-emc' headers. The headers must be hashed in alphabetic
        // order and the values must be stripped of whitespace and newlines.
        // TreeMap will automatically sort by key.
        Map<String, String> emcHeaders = new TreeMap<String, String>();
        for ( String key : headers.keySet() ) {
            String lowerKey = key.toLowerCase();
            if ( lowerKey.indexOf( "x-emc" ) == 0 )
                emcHeaders.put( lowerKey, join( headers.get( key ), "," ) );
        }
        for ( Iterator<String> i = emcHeaders.keySet().iterator(); i.hasNext(); ) {
            String key = i.next();
            builder.append( key ).append( ':' ).append( normalizeSpace( emcHeaders.get( key ) ) );
            if ( i.hasNext() ) builder.append( "\n" );
        }

        String hash = sign( builder.toString(), hashKey );

        // Add signature to headers
        headers.put( XHEADER_SIGNATURE, Arrays.asList( (Object) hash ) );
    }

    public static String normalizeSpace( String str ) {
        int length;
        do {
            length = str.length();
            str = str.replace( "  ", " " );
        } while ( length != str.length() );

        return str.replace( "\n", "" ).trim();
    }

    public static String join( Iterable<?> list, String delimiter ) {
        if ( list == null ) return null;
        StringBuilder builder = new StringBuilder();
        for ( Iterator<?> i = list.iterator(); i.hasNext(); ) {
            Object value = i.next();
            builder.append( value );
            if ( i.hasNext() ) builder.append( delimiter );
        }
        return builder.toString();
    }

    /**
     * Initializes new keys with an empty ArrayList. Convenience method for generating a header map.
     */
    public static void addValue( Map<String, List<Object>> multiValueMap, String key, Object value ) {
        List<Object> values = multiValueMap.get( key );
        if ( values == null ) {
            values = new ArrayList<Object>();
            multiValueMap.put( key, values );
        }
        values.add( value );
    }

    public static String lastPathElement( String path ) {
        if ( path == null ) return null;
        String[] elements = path.split( "/" );
        return elements[elements.length - 1];
    }

    public static ObjectId parseObjectId( String path ) {
        Matcher matcher = OBJECTID_PATTERN.matcher( path );
        if ( matcher.find() )
            return new ObjectId( matcher.group( 1 ) );
        else
            throw new AtmosException( "Cannot find object ID in path" + path );
    }

    public static Map<String, Metadata> parseMetadataHeader( String headerValue, boolean listable ) {
        try {
            Map<String, Metadata> metadataMap = new TreeMap<String, Metadata>();
            if ( headerValue == null ) return metadataMap;
            String[] pairs = headerValue.split( ",(?=[^,]+=)" ); // comma with key as look-ahead (not part of match)
            for ( String pair : pairs ) {
                String[] components = pair.split( "=", 2 );
                String name = URLDecoder.decode( components[0].trim(), "UTF-8" );
                String value = components.length > 1 ? URLDecoder.decode( components[1], "UTF-8" ) : null;
                Metadata metadata = new Metadata( name, value, listable );
                metadataMap.put( name, metadata );
            }
            return metadataMap;
        } catch ( UnsupportedEncodingException e ) {
            throw new RuntimeException( "UTF-8 encoding isn't supported on this system", e );
        }
    }

    public static Map<String, Permission> parseAclHeader( String headerValue ) {
        Map<String, Permission> acl = new TreeMap<String, Permission>();
        if ( headerValue == null ) return acl;
        for ( String pair : headerValue.split( "," ) ) {
            String[] components = pair.split( "=", 2 );
            String name = components[0].trim();
            String permission = components[1];

            // Currently, the server returns "FULL" instead of "FULL_CONTROL".
            // For consistency, change this to the value used in the request
            if ( "FULL".equals( permission ) ) {
                permission = "FULL_CONTROL";
            }

            acl.put( name, Permission.valueOf( permission ) );
        }
        return acl;
    }

    private static String generateHashLine( Map<String, List<Object>> headers, String headerName ) {
        String value = join( headers.get( headerName ), "," );
        l4j.debug( headerName + ": " + value );
        if ( value != null ) return value + "\n";
        return "\n";
    }

    private RestUtil() {
    }
}
