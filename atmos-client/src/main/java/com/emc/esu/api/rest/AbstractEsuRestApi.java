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
package com.emc.esu.api.rest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;

import com.emc.esu.api.Acl;
import com.emc.esu.api.BufferSegment;
import com.emc.esu.api.Checksum;
import com.emc.esu.api.DirectoryEntry;
import com.emc.esu.api.EsuApi;
import com.emc.esu.api.EsuException;
import com.emc.esu.api.Extent;
import com.emc.esu.api.Grant;
import com.emc.esu.api.Grantee;
import com.emc.esu.api.Grantee.GRANT_TYPE;
import com.emc.esu.api.Identifier;
import com.emc.esu.api.ListOptions;
import com.emc.esu.api.Metadata;
import com.emc.esu.api.MetadataList;
import com.emc.esu.api.MetadataTag;
import com.emc.esu.api.MetadataTags;
import com.emc.esu.api.ObjectId;
import com.emc.esu.api.ObjectPath;
import com.emc.esu.api.ObjectResult;
import com.emc.esu.api.Permission;
import com.emc.esu.api.ServiceInformation;
import com.emc.esu.api.Version;

/**
 * Encapsulates common REST API functionality that is independant of 
 * the transport layer, e.g. signature generation and getShareableUrl.
 * @author Jason Cwik
 */
public abstract class AbstractEsuRestApi implements EsuApi {
    private static final DateFormat HEADER_FORMAT = new SimpleDateFormat(
            "EEE, d MMM yyyy HH:mm:ss z", Locale.ENGLISH);
    private static final String ISO8601_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private static final Pattern OBJECTID_EXTRACTOR = Pattern
            .compile("/\\w+/objects/([0-9a-f]{44,})");
    private static final Logger l4j = Logger.getLogger( AbstractEsuRestApi.class );

    protected String host;
    protected int port;
    protected String uid;
    protected byte[] secret;

    protected String context = "/rest";
    protected String proto;
    
    protected boolean unicodeEnabled = false;
    
    protected boolean readChecksum;
    
    private long serverOffset;

    /**
     * Creates a new AbstractEsuRestApi
     * @param host the host running the web services
     * @param port the port number, e.g. 80 or 443
     * @param uid the web service UID
     * @param sharedSecret the UID's shared secret key
     */
    public AbstractEsuRestApi(String host, int port, String uid,
            String sharedSecret) {
        try {
            this.secret = Base64.decodeBase64( sharedSecret.getBytes( "UTF-8" ) );
        } catch (UnsupportedEncodingException e) {
            throw new EsuException( "Could not decode shared secret", e );
        }
        this.host = host;
        this.uid = uid;
        this.port = port;

        if( port == 443 ) {
            proto = "https";
        } else {
            proto = "http";
        }
    }

    /**
     * Gets the context root of the REST api. By default this is /rest.
     * 
     * @return the context
     */
    public String getContext() {
        return context;
    }

    /**
     * Overrides the default context root of the REST api.
     * 
     * @param context the context to set
     */
    public void setContext(String context) {
        this.context = context;
    }

    /**
     * Returns the protocol being used (http or https).
     * 
     * @return the proto
     */
    public String getProtocol() {
        return proto;
    }

    /**
     * Overrides the protocol selection. By default, https will be used for port
     * 443. Http will be used otherwise
     * 
     * @param proto the proto to set
     */
    public void setProtocol(String proto) {
        this.proto = proto;
    }
    
    
    /**
     * Creates a new object in the cloud.
     * 
     * @param acl Access control list for the new object. May be null to use a
     *            default ACL
     * @param metadata Metadata for the new object. May be null for no metadata.
     * @param data The initial contents of the object. May be appended to later.
     *            May be null to create an object with no content.
     * @param mimeType the MIME type of the content. Optional, may be null. If
     *            data is non-null and mimeType is null, the MIME type will
     *            default to application/octet-stream.
     * @return Identifier of the newly created object.
     * @throws EsuException if the request fails.
     */
    public ObjectId createObject(Acl acl, MetadataList metadata, byte[] data,
            String mimeType) {
        return createObjectFromSegment(acl, metadata, data == null ? null
                : new BufferSegment(data), mimeType, null);
    }
    
    /**
     * Creates a new object in the cloud.
     * 
     * @param acl Access control list for the new object. May be null to use a
     *            default ACL
     * @param metadata Metadata for the new object. May be null for no metadata.
     * @param data The initial contents of the object. May be appended to later.
     *            May be null to create an object with no content.
     * @param mimeType the MIME type of the content. Optional, may be null. If
     *            data is non-null and mimeType is null, the MIME type will
     *            default to application/octet-stream.
     * @param checksum if not null, use the Checksum object to compute
     * the checksum for the create object request.  If appending
     * to the object with subsequent requests, use the same
     * checksum object for each request.
     * @return Identifier of the newly created object.
     * @throws EsuException if the request fails.
     */
    public ObjectId createObject(Acl acl, MetadataList metadata, byte[] data,
            String mimeType, Checksum checksum ) {
        return createObjectFromSegment(acl, metadata, data == null ? null
                : new BufferSegment(data), mimeType, checksum);
    }

    /**
     * Creates a new object in the cloud using a BufferSegment.
     * 
     * @param acl Access control list for the new object. May be null to use a
     *            default ACL
     * @param metadata Metadata for the new object. May be null for no metadata.
     * @param data The initial contents of the object. May be appended to later.
     *            May be null to create an object with no content.
     * @param mimeType the MIME type of the content. Optional, may be null. If
     *            data is non-null and mimeType is null, the MIME type will
     *            default to application/octet-stream.
     * @return Identifier of the newly created object.
     * @throws EsuException if the request fails.
     */
    public ObjectId createObjectFromSegment(Acl acl, MetadataList metadata,
            BufferSegment data, String mimeType) {
    	return createObjectFromSegment( acl, metadata, data, mimeType, null );
    }

    /**
     * Creates a new object in the cloud on the specified path.
     * 
     * @param path The path to create the object on.
     * @param acl Access control list for the new object. May be null to use a
     *            default ACL
     * @param metadata Metadata for the new object. May be null for no metadata.
     * @param data The initial contents of the object. May be appended to later.
     *            May be null to create an object with no content.
     * @param mimeType the MIME type of the content. Optional, may be null. If
     *            data is non-null and mimeType is null, the MIME type will
     *            default to application/octet-stream.
     * @return the ObjectId of the newly-created object for references by ID.
     * @throws EsuException if the request fails.
     */
    public ObjectId createObjectOnPath(ObjectPath path, Acl acl,
            MetadataList metadata, byte[] data, String mimeType) {
        return createObjectFromSegmentOnPath(path, acl, metadata,
                data == null ? null : new BufferSegment(data), mimeType, null);

    }
    
    /**
     * Creates a new object in the cloud on the specified path.
     * 
     * @param path The path to create the object on.
     * @param acl Access control list for the new object. May be null to use a
     *            default ACL
     * @param metadata Metadata for the new object. May be null for no metadata.
     * @param data The initial contents of the object. May be appended to later.
     *            May be null to create an object with no content.
     * @param mimeType the MIME type of the content. Optional, may be null. If
     *            data is non-null and mimeType is null, the MIME type will
     *            default to application/octet-stream.
     * @param checksum if not null, use the Checksum object to compute
     * the checksum for the create object request.  If appending
     * to the object with subsequent requests, use the same
     * checksum object for each request.
     * @return the ObjectId of the newly-created object for references by ID.
     * @throws EsuException if the request fails.
     */
    public ObjectId createObjectOnPath(ObjectPath path, Acl acl,
            MetadataList metadata, byte[] data, String mimeType, Checksum checksum) {
        return createObjectFromSegmentOnPath(path, acl, metadata,
                data == null ? null : new BufferSegment(data), mimeType, checksum);

    }    
  
    /**
     * Creates a new object in the cloud using a BufferSegment on the given
     * path.
     * 
     * @param path the path to create the object on.
     * @param acl Access control list for the new object. May be null to use a
     *            default ACL
     * @param metadata Metadata for the new object. May be null for no metadata.
     * @param data The initial contents of the object. May be appended to later.
     *            May be null to create an object with no content.
     * @param mimeType the MIME type of the content. Optional, may be null. If
     *            data is non-null and mimeType is null, the MIME type will
     *            default to application/octet-stream.
     * @return the ObjectId of the newly-created object for references by ID.
     * @throws EsuException if the request fails.
     */
    public ObjectId createObjectFromSegmentOnPath(ObjectPath path, Acl acl,
            MetadataList metadata, BufferSegment data, String mimeType) {
    	return createObjectFromSegmentOnPath( path, acl, metadata, data, mimeType, null );
    }

    
    /**
     * Updates an object in the cloud and optionally its metadata and ACL.
     * 
     * @param id The ID of the object to update
     * @param acl Access control list for the new object. Optional, default is
     *            NULL to leave the ACL unchanged.
     * @param metadata Metadata list for the new object. Optional, default is
     *            NULL for no changes to the metadata.
     * @param data The new contents of the object. May be appended to later.
     *            Optional, default is NULL (no content changes).
     * @param extent portion of the object to update. May be null to indicate
     *            the whole object is to be replaced. If not null, the extent
     *            size must match the data size.
     * @param mimeType the MIME type of the content. Optional, may be null. If
     *            data is non-null and mimeType is null, the MIME type will
     *            default to application/octet-stream.
     * @throws EsuException if the request fails.
     */
    public void updateObject(Identifier id, Acl acl, MetadataList metadata,
            Extent extent, byte[] data, String mimeType) {
        updateObjectFromSegment(id, acl, metadata, extent, data == null ? null
                : new BufferSegment(data), mimeType, null);
    }
    
    /**
     * Updates an object in the cloud and optionally its metadata and ACL.
     * 
     * @param id The ID of the object to update
     * @param acl Access control list for the new object. Optional, default is
     *            NULL to leave the ACL unchanged.
     * @param metadata Metadata list for the new object. Optional, default is
     *            NULL for no changes to the metadata.
     * @param data The new contents of the object. May be appended to later.
     *            Optional, default is NULL (no content changes).
     * @param extent portion of the object to update. May be null to indicate
     *            the whole object is to be replaced. If not null, the extent
     *            size must match the data size.
     * @param mimeType the MIME type of the content. Optional, may be null. If
     *            data is non-null and mimeType is null, the MIME type will
     *            default to application/octet-stream.
     * @param checksum if not null, use the Checksum object to compute
     * the checksum for the update object request.  If appending
     * to the object with subsequent requests, use the same
     * checksum object for each request.
     * @throws EsuException if the request fails.
     */
    public void updateObject(Identifier id, Acl acl, MetadataList metadata,
            Extent extent, byte[] data, String mimeType, Checksum checksum ) {
        updateObjectFromSegment(id, acl, metadata, extent, data == null ? null
                : new BufferSegment(data), mimeType, checksum);
    }

    /**
     * Updates an object in the cloud and optionally its metadata and ACL.
     * 
     * @param id The ID of the object to update
     * @param acl Access control list for the new object. Optional, default is
     *            NULL to leave the ACL unchanged.
     * @param metadata Metadata list for the new object. Optional, default is
     *            NULL for no changes to the metadata.
     * @param data The new contents of the object. May be appended to later.
     *            Optional, default is NULL (no content changes).
     * @param extent portion of the object to update. May be null to indicate
     *            the whole object is to be replaced. If not null, the extent
     *            size must match the data size.
     * @param mimeType the MIME type of the content. Optional, may be null. If
     *            data is non-null and mimeType is null, the MIME type will
     *            default to application/octet-stream.
     * @throws EsuException if the request fails.
     */
    public void updateObjectFromSegment(Identifier id, Acl acl,
            MetadataList metadata, Extent extent, BufferSegment data,
            String mimeType) {
    	updateObjectFromSegment( id, acl, metadata, extent, data, mimeType, null );
    }
    
    /**
     * Reads an object's content.
     * 
     * @param id the identifier of the object whose content to read.
     * @param extent the portion of the object data to read. Optional. Default
     *            is null to read the entire object.
     * @param buffer the buffer to use to read the extent. Must be large enough
     *            to read the response or an error will be thrown. If null, a
     *            buffer will be allocated to hold the response data. If you
     *            pass a buffer that is larger than the extent, only
     *            extent.getSize() bytes will be valid.
     * @return the object data read as a byte array.
     */
    public byte[] readObject(Identifier id, Extent extent, byte[] buffer) {
    	return readObject( id, extent, buffer, null );
    }
    
    /**
     * Lists all objects with the given tag.
     * 
     * @param tag the tag to search for
     * @return The list of objects with the given tag. If no objects are found
     *         the array will be empty.
     * @throws EsuException if no objects are found (code 1003)
     */
    public List<Identifier> listObjects(MetadataTag tag) {
        return filterIdList(listObjects(tag.getName(), null));
    }
    
	/**
     * Lists all objects with the given tag.
     * 
     * @param tag the tag to search for
     * @param options the options for listing the objects
     * @return The list of objects with the given tag. If no objects are found
     *         the array will be empty.
     * @throws EsuException if no objects are found (code 1003)
     */
    public List<ObjectResult> listObjects(MetadataTag tag, ListOptions options) {
        return listObjects(tag.getName(), options);
    }
    
    /**
     * Lists all objects with the given tag.
     * 
     * @param tag the tag to search for
     * @return The list of objects with the given tag. If no objects are found
     *         the array will be empty.
     * @throws EsuException if no objects are found (code 1003)
     */
    public List<Identifier> listObjects(String tag) {
    	return filterIdList( listObjects( tag, null ) );
    }
    
	/**
     * Lists all objects with the given tag and returns both their IDs and their
     * metadata.
     * 
     * @param tag the tag to search for
     * @return The list of objects with the given tag. If no objects are found
     *         the array will be empty.
     */
    public List<ObjectResult> listObjectsWithMetadata(MetadataTag tag) {
        return listObjectsWithMetadata(tag.getName());
    }

    /**
     * Lists all objects with the given tag and returns both their IDs and their
     * metadata.
     * 
     * @param tag the tag to search for
     * @return The list of objects with the given tag. If no objects are found
     *         the array will be empty.
     */
    public List<ObjectResult> listObjectsWithMetadata(String tag) {
    	ListOptions options = new ListOptions();
    	options.setIncludeMetadata( true );
    	return listObjects( tag, options );
    }

    
    /**
     * Lists the contents of a directory.
     * @param path the path to list.  Must be a directory.
     * @return the directory entries in the directory.
     * @deprecated Use the version with ListOptions to control the result
     * count and handle large result sets.
     */
    public List<DirectoryEntry> listDirectory( ObjectPath path ) {
    	return listDirectory( path, null );
    }


    /**
     * Generates an HMAC-SHA1 signature of the given input string using the
     * shared secret key.
     * @param input the string to sign
     * @return the HMAC-SHA1 signature in Base64 format
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     * @throws IllegalStateException
     * @throws UnsupportedEncodingException
     */
    public String sign( String input ) throws NoSuchAlgorithmException, InvalidKeyException, IllegalStateException, UnsupportedEncodingException {
        // Compute the signature hash
        l4j.debug( "Hashing: \n" + input.toString() );

        String hashOut = sign( input.getBytes("UTF-8") );
        
        l4j.debug( "Hash: " + hashOut );

        return hashOut;
    }
    
    public String sign( byte[] input ) throws UnsupportedEncodingException, InvalidKeyException, NoSuchAlgorithmException {
        Mac mac = Mac.getInstance( "HmacSHA1" );
        SecretKeySpec key = new SecretKeySpec( secret, "HmacSHA1" );
        mac.init( key );

        byte[] hashData = mac.doFinal( input );

        // Encode the hash in Base64.
        return new String( Base64.encodeBase64( hashData ), "UTF-8" );
    }

    /**
     * An Atmos user (UID) can construct a pre-authenticated URL to an 
     * object, which may then be used by anyone to retrieve the 
     * object (e.g., through a browser). This allows an Atmos user 
     * to let a non-Atmos user download a specific object. The 
     * entire object/file is read.
     * @param id the object to generate the URL for
     * @param expiration the expiration date of the URL
     * @return a URL that can be used to share the object's content
     */
    public URL getShareableUrl(Identifier id, Date expiration, String disposition) {
        try {
            String resource = getResourcePath( context, id );
            
            StringBuffer sb = new StringBuffer();
            sb.append( "GET\n" );
            sb.append( resource.toLowerCase() + "\n" );
            sb.append( uid + "\n" );
            sb.append( ""+(expiration.getTime()/1000) );
            if(disposition != null) {
            	sb.append("\n" + disposition);
            }
            
            String signature = sign( sb.toString() );
            String query = "uid=" + encodeUtf8(uid) + "&expires=" + (expiration.getTime()/1000) +
                "&signature=" + encodeUtf8(signature);
            if(disposition != null) {
            	disposition = encodeUtf8(disposition);

            	query += "&disposition=" + disposition;
            }
            
            // We do this a little strangely here.  Technically, the trailing "=" in the Base-64 signature
            // should be encoded since it's a "reserved" character.  Atmos 1.2 is strict about this, but
            // 1.3 relaxes the rules a bit.  Most URL generators (java.net.URI included) don't have facilities
            // to break down the query components and encode them individually.  Therefore, we encode the
            // query ourselves here and append it to the generated URL.  This will then work with both
            // 1.2 and 1.3.
            URL u = buildUrl( resource, null );
                u = new URL( u.toString() + "?" + query );

                l4j.debug( "URL: " + u );
            
            return u;
        } catch (UnsupportedEncodingException e) {
            throw new EsuException( "Unsupported encoding", e );
        } catch (InvalidKeyException e) {
            throw new EsuException( "Invalid secret key", e );
        } catch (NoSuchAlgorithmException e) {
            throw new EsuException( "Missing signature algorithm", e );
        } catch (IllegalStateException e) {
            throw new EsuException( "Error signing request", e );
        } catch (MalformedURLException e) {
            throw new EsuException( "Invalid URL format", e );
        } catch (URISyntaxException e) {
            throw new EsuException( "Invalid URL", e );
        }
    }
    
    /**
     * An Atmos user (UID) can construct a pre-authenticated URL to an 
     * object, which may then be used by anyone to retrieve the 
     * object (e.g., through a browser). This allows an Atmos user 
     * to let a non-Atmos user download a specific object. The 
     * entire object/file is read.
     * @param id the object to generate the URL for
     * @param expiration the expiration date of the URL
     * @return a URL that can be used to share the object's content
     */
    public URL getShareableUrl(Identifier id, Date expiration) {
    	return getShareableUrl(id, expiration, null);
    }

    /**
     * Gets the appropriate resource path depending on identifier
     * type.
     */
    protected String getResourcePath( String ctx, Identifier id ) {
                if( id instanceof ObjectId ) {
                        return ctx + "/objects/" + id;
                } else {
                        return ctx + "/namespace" + id;
                }
        }
    
    
    /**
     * Builds a new URL to the given resource
     * @throws URISyntaxException 
     * @throws MalformedURLException 
     */
    protected URL buildUrl(String resource, String query ) throws URISyntaxException, MalformedURLException  {
    	int uriport =0;
    	if( "http".equals(proto) && port == 80 ) {
    		// Default port
    		uriport = -1;
    	} else if( "https".equals(proto) && port == 443 ) {
    		uriport = -1;
    	} else {
    		uriport = port;
    	}
        URI uri = new URI( proto, null, host, uriport, resource, query, null );
        l4j.debug("URI: " + uri);
        URL u = new URL(uri.toASCIIString());
        l4j.debug( "URL: " + u );
        return u;
    }

    /**
     * Helper method that closes a stream ignoring errors.
     * @param out the OutputStream to close
     */
    protected void silentClose(OutputStream out) {
        if( out == null ) {
            return;
        }
        try {
            out.close();
        } catch (IOException e) {
            // ignore
        }
    }

    /**
     * Parses the given header text and appends to the metadata list
     * @param meta the metadata list to append to
     * @param header the metadata header to parse
     * @param listable true if the header being parsed contains listable metadata.
     * @throws UnsupportedEncodingException 
     */
    protected void readMetadata(MetadataList meta, String header, boolean listable) throws UnsupportedEncodingException {
        if (header == null) {
            return;
        }

		String[] attrs = header.split( ",(?=[^,]+=)" );
        for (int i = 0; i < attrs.length; i++) {
            String[] nvpair = attrs[i].split("=", 2);
            String name = nvpair[0];
            String value = nvpair.length>1?nvpair[1]:null;

            name = name.trim();
            
            if(unicodeEnabled) {
                name = decodeUtf8(name);
            	value = decodeUtf8(value);
            }

            Metadata m = new Metadata(name, value, listable);
            l4j.debug("Meta: " + m);
            meta.addMetadata(m);
        }
    }

    /**
     * Enumerates the given list of metadata tags and sets the x-emc-tags
     * header.
     * @param tags the tag list to enumerate
     * @param headers the HTTP request headers
     * @throws UnsupportedEncodingException
     */
    protected void processTags(MetadataTags tags, Map<String, String> headers) throws UnsupportedEncodingException {
        StringBuffer taglist = new StringBuffer();

        l4j.debug("Processing " + tags.count() + " metadata tag entries");

        if(unicodeEnabled) {
            headers.put("x-emc-utf8", "true");
        }

        for (Iterator<MetadataTag> i = tags.iterator(); i.hasNext();) {
            MetadataTag tag = i.next();
            if (taglist.length() > 0) {
                taglist.append(",");
            }
            taglist.append(unicodeEnabled ? encodeUtf8( tag.getName() ) : tag.getName());
        }

        if (taglist.length() > 0) {
            headers.put("x-emc-tags", taglist.toString());
        }
    }

    /**
     * Parses the value of an ACL response header and builds an ACL
     * @param acl a reference to the ACL to append to
     * @param header the acl response header
     * @param type the type of Grantees in the header (user or group)
     */
    protected void readAcl(Acl acl, String header, GRANT_TYPE type) {
        l4j.debug("readAcl: " + header);
        String[] grants = header.split(",");
        for (int i = 0; i < grants.length; i++) {
            String[] nvpair = grants[i].split("=", 2);
            String grantee = nvpair[0];
            String permission = nvpair[1];

            grantee = grantee.trim();

            // Currently, the server returns "FULL" instead of "FULL_CONTROL".
            // For consistency, change this to value use in the request
            if ("FULL".equals(permission)) {
                permission = Permission.FULL_CONTROL;
            }

            l4j.debug("grant: " + grantee + "." + permission + " (" + type
                    + ")");

            Grantee ge = new Grantee(grantee, type);
            Grant gr = new Grant(ge, permission);
            l4j.debug("Grant: " + gr);
            acl.addGrant(gr);
        }
    }



    /**
     * Parses an XML response and extracts the list of ObjectIDs.
     * @param response the response byte array to parse as XML
     * @return the list of object IDs contained in the response.
     */
    @SuppressWarnings("rawtypes")
    protected List<ObjectId> parseObjectList( byte[] response ) {
        List<ObjectId> objs = new ArrayList<ObjectId>();
        
        // Use JDOM to parse the XML
        SAXBuilder sb = new SAXBuilder();
        try {
            Document d = sb.build( new ByteArrayInputStream( response ) );
            
            // The ObjectID element is part of a namespace so we need to use
            // the namespace to identify the elements.
            Namespace esuNs = Namespace.getNamespace( "http://www.emc.com/cos/" );

            List children = d.getRootElement().getChildren( "ObjectID", esuNs );
            
            l4j.debug( "Found " + children.size() + " objects" );
            for( Iterator i=children.iterator(); i.hasNext(); ) {
                Object o = i.next();
                if( o instanceof Element ) {
                    ObjectId oid = new ObjectId( ((Element)o).getText() );
                    l4j.debug( oid.toString() );
                    objs.add( oid );
                } else {
                    l4j.debug( o + " is not an Element!" );
                }
            }
            
        } catch (JDOMException e) {
            throw new EsuException( "Error parsing response", e );
        } catch (IOException e) {
            throw new EsuException( "Error reading response", e );
        }

        return objs;
    }

    @SuppressWarnings("rawtypes")
	protected List<Identifier> parseVersionList( byte[] response ) {
        List<Identifier> objs = new ArrayList<Identifier>();
        
        // Use JDOM to parse the XML
        SAXBuilder sb = new SAXBuilder();
        try {
            Document d = sb.build( new ByteArrayInputStream( response ) );
            
            // The ObjectID element is part of a namespace so we need to use
            // the namespace to identify the elements.
            Namespace esuNs = Namespace.getNamespace( "http://www.emc.com/cos/" );

            List children = d.getRootElement().getChildren( "Ver", esuNs );
            
            l4j.debug( "Found " + children.size() + " objects" );
            for( Iterator i=children.iterator(); i.hasNext(); ) {
                Object o = i.next();
                if( o instanceof Element ) {
                    Element objectIdElement = (Element)((Element)o).getChildren( "OID", esuNs ).get(0);
                    ObjectId oid = new ObjectId( objectIdElement.getText() );
                    l4j.debug( oid.toString() );
                    objs.add( oid );
                } else {
                    l4j.debug( o + " is not an Element!" );
                }
            }
            
        } catch (JDOMException e) {
            throw new EsuException( "Error parsing response", e );
        } catch (IOException e) {
            throw new EsuException( "Error reading response", e );
        }

        return objs;
    }
    
    @SuppressWarnings("rawtypes")
	protected List<Version> parseVersionListLong( byte[] response ) {
        List<Version> objs = new ArrayList<Version>();
        DateFormat itimeParser = new SimpleDateFormat(ISO8601_FORMAT);
        itimeParser.setTimeZone(TimeZone.getTimeZone("UTC"));
        
        // Use JDOM to parse the XML
        SAXBuilder sb = new SAXBuilder();
        try {
            Document d = sb.build( new ByteArrayInputStream( response ) );
            
            // The ObjectID element is part of a namespace so we need to use
            // the namespace to identify the elements.
            Namespace esuNs = Namespace.getNamespace( "http://www.emc.com/cos/" );

            List children = d.getRootElement().getChildren( "Ver", esuNs );
            
            l4j.debug( "Found " + children.size() + " objects" );
            for( Iterator i=children.iterator(); i.hasNext(); ) {
                Object o = i.next();
                if( o instanceof Element ) {
                	Element e = (Element)o;
                    ObjectId id = new ObjectId( e.getChildText("OID", esuNs) );
                    int versionNumber = 
                    		Integer.parseInt(e.getChildText("VerNum", esuNs));
                    String sitime = e.getChildText("itime", esuNs);
                    Date itime = null;
                    try {
						itime = itimeParser.parse(sitime);
					} catch (ParseException e1) {
						throw new EsuException("Could not parse itime: " + sitime, e1);
					}
                    
                    objs.add(new Version(id, versionNumber, itime));
                } else {
                    l4j.debug( o + " is not an Element!" );
                }
            }
            
        } catch (JDOMException e) {
            throw new EsuException( "Error parsing response", e );
        } catch (IOException e) {
            throw new EsuException( "Error reading response", e );
        }

        return objs;
    }

    /**
     * Parses an XML response and extracts the list of ObjectIDs
     * and metadata.
     * @param response the response byte array to parse as XML
     * @return the list of object IDs contained in the response.
     */
    @SuppressWarnings("rawtypes")
	protected List<ObjectResult> parseObjectListWithMetadata( byte[] response ) {
        List<ObjectResult> objs = new ArrayList<ObjectResult>();
        
        // Use JDOM to parse the XML
        SAXBuilder sb = new SAXBuilder();
        try {
            Document d = sb.build( new ByteArrayInputStream( response ) );
            
            // The ObjectID element is part of a namespace so we need to use
            // the namespace to identify the elements.
            Namespace esuNs = Namespace.getNamespace( "http://www.emc.com/cos/" );

            List children = d.getRootElement().getChildren( "Object", esuNs );
            
            l4j.debug( "Found " + children.size() + " objects" );
            for( Iterator i=children.iterator(); i.hasNext(); ) {
                Object o = i.next();
                if( o instanceof Element ) {
                        Element e = (Element)o;
                        ObjectResult obj = new ObjectResult();
                    Element objectIdElement = e.getChild( "ObjectID", esuNs );
                    ObjectId oid = new ObjectId( objectIdElement.getText() );
                    obj.setId( oid );
                    
                    // next, get metadata
                    Element sMeta = e.getChild( "SystemMetadataList", esuNs );
                    Element uMeta = e.getChild( "UserMetadataList", esuNs );
                    obj.setMetadata( new MetadataList() );
                    
                    if( sMeta != null ) {
	                    for( Iterator m = sMeta.getChildren( "Metadata" , esuNs ).iterator(); m.hasNext(); ) {
	                        Element metaElement = (Element)m.next();
	                        
	                        String mName = metaElement.getChildText( "Name", esuNs );
	                        String mValue = metaElement.getChildText( "Value", esuNs );
	                        
	                        obj.getMetadata().addMetadata( new Metadata( mName, mValue, false ) );
	                    }
                    }
                    
                    if( uMeta != null ) {
	                    for( Iterator m = uMeta.getChildren( "Metadata" , esuNs ).iterator(); m.hasNext(); ) {
	                        Element metaElement = (Element)m.next();
	                        
	                        String mName = metaElement.getChildText( "Name", esuNs );
	                        String mValue = metaElement.getChildText( "Value", esuNs );
	                        String mListable = metaElement.getChildText( "Listable", esuNs );
	                        
	                        obj.getMetadata().addMetadata( new Metadata( mName, mValue, "true".equals( mListable ) ) );
	                    }
                    }
                    
                    objs.add( obj );
                } else {
                    l4j.debug( o + " is not an Element!" );
                }
            }
            
        } catch (JDOMException e) {
            throw new EsuException( "Error parsing response", e );
        } catch (IOException e) {
            throw new EsuException( "Error reading response", e );
        }

        return objs;
    }
    
    
    @SuppressWarnings("rawtypes")
	protected List<DirectoryEntry> parseDirectoryListing( byte[] data, 
    		ObjectPath basePath ) {
    	
        // Parse
        List<DirectoryEntry> objs = new ArrayList<DirectoryEntry>();

        // Use JDOM to parse the XML
        SAXBuilder sb = new SAXBuilder();
        try {
            Document d = sb.build(new ByteArrayInputStream(data));

            // The ObjectID element is part of a namespace so we need to use
            // the namespace to identify the elements.
            Namespace esuNs = Namespace.getNamespace("http://www.emc.com/cos/");

            List children = d.getRootElement().getChild("DirectoryList", esuNs)
                    .getChildren("DirectoryEntry", esuNs);
            l4j.debug("Found " + children.size() + " objects");
            for (Iterator i = children.iterator(); i.hasNext();) {
                Object o = i.next();
                if (o instanceof Element) {
                    DirectoryEntry de = new DirectoryEntry();
                    de.setId(new ObjectId(((Element) o).getChildText(
                            "ObjectID", esuNs)));
                    String name = ((Element) o).getChildText("Filename", esuNs);
                    String type = ((Element) o).getChildText("FileType", esuNs);

                    name = basePath.toString() + name;
                    if ("directory".equals(type)) {
                        name += "/";
                    }
                    de.setPath(new ObjectPath(name));
                    de.setType(type);
                    
                    // next, get metadata
                    Element sMeta = ((Element) o).getChild( "SystemMetadataList", esuNs );
                    Element uMeta = ((Element) o).getChild( "UserMetadataList", esuNs );
                    
                    if( sMeta != null ) {
	                    de.setSystemMetadata( new MetadataList() );
	                    
	                    for( Iterator m = sMeta.getChildren( "Metadata" , esuNs ).iterator(); m.hasNext(); ) {
	                        Element metaElement = (Element)m.next();
	                        
	                        String mName = metaElement.getChildText( "Name", esuNs );
	                        String mValue = metaElement.getChildText( "Value", esuNs );
	                        
	                        de.getSystemMetadata().addMetadata( new Metadata( mName, mValue, false ) );
	                    }
                    }
                    
                    if( uMeta != null ) {
                    	de.setUserMetadata( new MetadataList() );
	                    for( Iterator m = uMeta.getChildren( "Metadata" , esuNs ).iterator(); m.hasNext(); ) {
	                        Element metaElement = (Element)m.next();
	                        
	                        String mName = metaElement.getChildText( "Name", esuNs );
	                        String mValue = metaElement.getChildText( "Value", esuNs );
	                        String mListable = metaElement.getChildText( "Listable", esuNs );
	                        
	                        de.getUserMetadata().addMetadata( new Metadata( mName, mValue, "true".equals( mListable ) ) );
	                    }
                    }

                    objs.add(de);
                } else {
                    l4j.debug(o + " is not an Element!");
                }
            }

        } catch (JDOMException e) {
            throw new EsuException("Error parsing response", e);
        } catch (IOException e) {
            throw new EsuException("Error reading response", e);
        }

        return objs;
    }
    
    /**
     * Parses the given header and appends to the list of metadata tags.
     * @param tags the list of metadata tags to append to
     * @param header the header to parse
     * @param listable true if the metadata tags in the header are listable
     * @throws UnsupportedEncodingException
     */
    protected void readTags( MetadataTags tags, String header, boolean listable) throws UnsupportedEncodingException {
        if (header == null) {
            return;
        }

        String[] attrs = header.split(",");
        for (int i = 0; i < attrs.length; i++) {
            String attr = attrs[i].trim();
            tags.addTag(new MetadataTag(unicodeEnabled ? decodeUtf8( attr ) : attr, listable));
        }
    }

    /**
     * Iterates through the given metadata and adds the appropriate metadata
     * headers to the request.
     * 
     * @param metadata the metadata to add
     * @param headers the map of request headers.
     * @throws UnsupportedEncodingException 
     */
    protected void processMetadata(MetadataList metadata,
            Map<String, String> headers) throws UnsupportedEncodingException {

        StringBuffer listable = new StringBuffer();
        StringBuffer nonListable = new StringBuffer();
        
        if(unicodeEnabled) {
        	headers.put("x-emc-utf8", "true");
        }

        l4j.debug("Processing " + metadata.count() + " metadata entries");

        for (Iterator<Metadata> i = metadata.iterator(); i.hasNext();) {
            Metadata meta = i.next();
            if (meta.isListable()) {
                if (listable.length() > 0) {
                    listable.append(", ");
                }
                listable.append(formatTag(meta));
            } else {
                if (nonListable.length() > 0) {
                    nonListable.append(", ");
                }
                nonListable.append(formatTag(meta));
            }
        }

        // Only set the headers if there's data
        if (listable.length() > 0) {
            headers.put("x-emc-listable-meta", listable.toString());
        }
        if (nonListable.length() > 0) {
            headers.put("x-emc-meta", nonListable.toString());
        }

    }

    /**
     * Formats a tag value for passing in the header.
     * @throws UnsupportedEncodingException 
     */
    protected String formatTag(Metadata meta) throws UnsupportedEncodingException {
        // strip commas and newlines for now.
    	if(unicodeEnabled) {
    		String name = encodeUtf8(meta.getName());
    		
        	if( meta.getValue() == null ) {
        		return name + "=";
        	}
    		String value = encodeUtf8(meta.getValue());
    		return name + "=" + value;
    	} else {
        	if( meta.getValue() == null ) {
        		return meta.getName() + "=";
        	}
	        String fixed = meta.getValue().replace("\n", "");
	        fixed = fixed.replace( ",", "" );
	        return meta.getName() + "=" + fixed;
    	}
    }

    protected String encodeUtf8(String value) throws UnsupportedEncodingException {
        // Use %20, not +
        return URLEncoder.encode(value, "UTF-8").replace("+", "%20");
    }
    
    protected String decodeUtf8(String value) throws UnsupportedEncodingException {
        return URLDecoder.decode(value, "UTF-8");
    }

    /**
     * Enumerates the given ACL and creates the appropriate request headers.
     * 
     * @param acl the ACL to enumerate
     * @param headers the set of request headers.
     */
    protected void processAcl(Acl acl, Map<String, String> headers) {
        StringBuffer userGrants = new StringBuffer();
        StringBuffer groupGrants = new StringBuffer();

        for (Iterator<Grant> i = acl.iterator(); i.hasNext();) {
            Grant grant = i.next();
            if (grant.getGrantee().getType() == Grantee.GRANT_TYPE.USER) {
                if (userGrants.length() > 0) {
                    userGrants.append(",");
                }
                userGrants.append(grant.toString());
            } else {
                if (groupGrants.length() > 0) {
                    groupGrants.append(",");
                }
                groupGrants.append(grant.toString());
            }
        }

        headers.put("x-emc-useracl", userGrants.toString());
        headers.put("x-emc-groupacl", groupGrants.toString());
    }
    


    /**
     * Condenses consecutive spaces into one.
     */
    protected String normalizeSpace(String str) {
		int length = str.length();
		while(true) {
			str = str.replace( "  ", " " );
			if( str.length() == length ) {
				// unchanged
				break;
			}
			length = str.length();
		}
		
		// Strip any trailing space
		while( str.endsWith(" ") ) {
			str = str.substring(0, str.length()-1);
		}
		
		return str;
		
	}

	/**
     * Gets the current time formatted for HTTP headers
     */
    protected String getDateHeader() {
        TimeZone tz = TimeZone.getTimeZone("GMT");
        l4j.debug("TZ: " + tz);
        
        // Per the Java documentation, DateFormat objects are not thread safe.
        synchronized(HEADER_FORMAT) {
	        HEADER_FORMAT.setTimeZone(tz);
	        String dateHeader = HEADER_FORMAT.format(new Date(System.currentTimeMillis()-serverOffset));
	        l4j.debug("Date: " + dateHeader);
	        return dateHeader;
        }
    }
    
    protected ObjectId getObjectId( String location ) {
        Matcher m = OBJECTID_EXTRACTOR.matcher(location);
        if (m.find()) {
            String vid = m.group(1);
            l4j.debug("vId: " + vid);
            return new ObjectId(vid);
        } else {
            throw new EsuException("Could not find ObjectId in " + location);
        }
    }
    
    protected byte[] readStream( InputStream in, int contentLength ) throws IOException {
        try {
            byte[] output;
            // If we know the content length, read it directly into a buffer.
            if (contentLength != -1) {
                output = new byte[contentLength];

                int c = 0;
                while (c < contentLength) {
                    int read = in.read(output, c, contentLength - c);
                    if (read == -1) {
                        // EOF!
                        throw new EOFException(
                                "EOF reading response at position " + c
                                        + " size " + (contentLength - c));
                    }
                    c += read;
                }

                return output;
            } else {
                l4j.debug("Content length is unknown.  Buffering output.");
                // Else, use a ByteArrayOutputStream to collect the response.
                byte[] buffer = new byte[4096];

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                int c = 0;
                while ((c = in.read(buffer)) != -1) {
                    baos.write(buffer, 0, c);
                }
                baos.close();

                l4j.debug("Buffered " + baos.size() + " response bytes");

                return baos.toByteArray();
            }
        } finally {
            if (in != null) {
                in.close();
            }
        }

    }
    
    protected ServiceInformation parseServiceInformation( byte[] response, Map<String, List<String>> map ) {
        // Use JDOM to parse the XML
        SAXBuilder sb = new SAXBuilder();
        try {
            Document d = sb.build( new ByteArrayInputStream( response ) );
            
            ServiceInformation si = new ServiceInformation();
            
            // The ObjectID element is part of a namespace so we need to use
            // the namespace to identify the elements.
            Namespace esuNs = Namespace.getNamespace( "http://www.emc.com/cos/" );

            Element ver = d.getRootElement().getChild( "Version", esuNs );
            Element atmos = ver.getChild( "Atmos", esuNs );
            
            si.setAtmosVersion( atmos.getTextNormalize() );
            
            // Check for UTF8 support
            for(String key : map.keySet()) {
            	if("x-emc-support-utf8".equalsIgnoreCase(key)) {
            		for(String val : map.get(key)) {
            			if("true".equalsIgnoreCase(val)) {
            				si.setUnicodeMetadataSupported(true);
            			}
            		}
            	}
            	if("x-emc-features".equalsIgnoreCase(key)) {
            		for(String val : map.get(key)) {
            			String[] features = val.split(",");
            			for(String feature : features) {
            				si.addFeature(feature.trim());
            			}
            		}
            		
            	}
            }
            
            return si;
        } catch (JDOMException e) {
            throw new EsuException( "Error parsing response", e );
        } catch (IOException e) {
            throw new EsuException( "Error reading response", e );
        }
    }
    
    /**
     * Converts an ObjectResult list to an Identifier list.
     */
    private List<Identifier> filterIdList(List<ObjectResult> list) {
		List<Identifier> result = new ArrayList<Identifier>( list.size() );
		
		for( ObjectResult r : list ) {
			result.add( r.getId() );
		}
		
		return result;
	}

    /**
     * Joins a list of Strings using a delimiter (similar to PERL, PHP, etc)
     * @param list the list of Strings
     * @param delimiter the string to join the list with
     * @return the joined String.
     */
    protected String join(List<String> list, String delimiter) {
		boolean first = true;
		StringBuffer sb = new StringBuffer();
		
		for( String s : list ) {
			if( first ) {
				first = false;
			} else {
				sb.append( delimiter );
			}
			sb.append( s );
		}
		
		return sb.toString();
	}


	/**
	 * @return the readChecksum
	 */
	public boolean isReadChecksum() {
		return readChecksum;
	}

	/**
	 * Turns read checksum verification on or off.  Note that 
	 * checksums are only returned from the server for erasure coded objects.
	 * @param readChecksum the readChecksum to set
	 */
	public void setReadChecksum(boolean readChecksum) {
		this.readChecksum = readChecksum;
	}

	/**
	 * Returns true if unicode metadata processing is enabled.
	 */
	public boolean isUnicodeEnabled() {
		return unicodeEnabled;
	}

	/**
	 * Set to true to enable Unicode metadata processing.  
	 */
	public void setUnicodeEnabled(boolean unicodeEnabled) {
		this.unicodeEnabled = unicodeEnabled;
	}

	/**
	 * Gets the current server offset in milliseconds.  This value can be used
	 * to adjust for clock skew between the client and server.
	 * @return the serverOffset
	 */
	public long getServerOffset() {
		return serverOffset;
	}

	/**
	 * Sets the server offset in millesconds.  This value can be used to
	 * adjust for clock skew between the client and the server.
	 * @param serverOffset the serverOffset to set
	 */
	public void setServerOffset(long serverOffset) {
		this.serverOffset = serverOffset;
	}
	
	/**
	 * Makes a request to the server to get the value of the response Date
	 * header.  Compares this date with the local system time to calculate
	 * the offset between the client and the server.  You can pass this value
	 * to the setServerOffset method to adjust for clock skew.
	 * @return the offset between the client and server in milliseconds.  If
	 * the client is ahead of the server, this will be positive.  If the server
	 * is ahead of the client, it will be negative.
	 */
	public abstract long calculateServerOffset();

    //---------- Features supported by the Atmos 2.0 REST API. ----------\\

    @Override
    public ObjectId createObjectWithKey( String keyPool, String key, Acl acl, MetadataList metadata,
                                         byte[] data, long length, String mimeType ) {
        return createObjectWithKeyFromSegment( keyPool, key, acl, metadata,
                                               new BufferSegment( data, 0, (int) length ), mimeType );
    }

    @Override
    public ObjectId createObjectWithKey( String keyPool, String key, Acl acl, MetadataList metadata,
                                         byte[] data, long length, String mimeType, Checksum checksum ) {
        return createObjectWithKeyFromSegment( keyPool, key, acl, metadata,
                                               new BufferSegment( data, 0, (int) length ), mimeType, checksum );
    }

    @Override
    public ObjectId createObjectWithKeyFromSegment( String keyPool, String key, Acl acl, MetadataList metadata,
                                                    BufferSegment data, String mimeType ) {
        return createObjectWithKeyFromSegment( keyPool, key, acl, metadata, data, mimeType, null );
    }

    @Override
    public byte[] readObjectWithKey( String keyPool, String key, Extent extent, byte[] buffer ) {
        return readObjectWithKey( keyPool, key, extent, buffer, null );
    }

    @Override
    public void updateObjectWithKey( String keyPool, String key, Acl acl, MetadataList metadata,
                                     Extent extent, byte[] data, String mimeType ) {
        updateObjectWithKeyFromSegment( keyPool, key, acl, metadata, extent, new BufferSegment( data ), mimeType );
    }

    @Override
    public void updateObjectWithKey( String keyPool, String key, Acl acl, MetadataList metadata,
                                     Extent extent, byte[] data, String mimeType, Checksum checksum ) {
        updateObjectWithKeyFromSegment( keyPool, key, acl, metadata, extent,
                                        new BufferSegment( data ), mimeType, checksum );
    }

    @Override
    public void updateObjectWithKeyFromSegment( String keyPool, String key, Acl acl, MetadataList metadata,
                                                Extent extent, BufferSegment data, String mimeType ) {
        updateObjectWithKeyFromSegment( keyPool, key, acl, metadata, extent, data, mimeType, null );
    }
}
