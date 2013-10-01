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
package com.emc.esu.api.rest;

import com.emc.esu.api.*;
import org.apache.http.*;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.*;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.impl.cookie.DateParseException;
import org.apache.http.impl.cookie.DateUtils;
import org.apache.http.message.AbstractHttpMessage;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.jdom.Document;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.*;

/**
 * This is an enhanced version of the REST API that uses the Apache Commons
 * HTTP Client as its transport layer instead of the built-in Java HTTP
 * client.  It should perform better at the expense of a slightly larger
 * footprint.  See the JARs in the commons-httpclient directory in the
 * project's root folder.
 * 
 * @author Jason Cwik
 *
 */
public class EsuRestApiApache extends AbstractEsuRestApi {
    private static final Logger l4j = Logger.getLogger( EsuRestApiApache.class );
    private DefaultHttpClient httpClient;

    /**
     * Creates a new EsuRestApiApache object.
     * 
     * @param host the hostname or IP address of the ESU server
     * @param port the port on the server to communicate with. Generally this is
     *            80 for HTTP and 443 for HTTPS.
     * @param uid the username to use when connecting to the server
     * @param sharedSecret the Base64 encoded shared secret to use to sign
     *            requests to the server.
     */
    public EsuRestApiApache(String host, int port, String uid, String sharedSecret) {
        super(host, port, uid, sharedSecret);

        SchemeRegistry schemeRegistry = new SchemeRegistry();
        schemeRegistry.register(new Scheme("https", port, SSLSocketFactory.getSocketFactory()));
        schemeRegistry.register(new Scheme("http", port, PlainSocketFactory.getSocketFactory()));

        PoolingClientConnectionManager cm = new PoolingClientConnectionManager(schemeRegistry);
        // Increase max total connection to 200
        cm.setMaxTotal(200);
        // Increase default max connection per route to 20
        cm.setDefaultMaxPerRoute(200);

        httpClient = new DefaultHttpClient( cm, null );
    }
    
    public void setProxy( String host, int port, boolean https ) {
    	setProxy( host, port, https, null, null );
    }
    
    public void setProxy( String host, int port, boolean https, String username, 
    		String password ) {
    	HttpHost proxyHost = new HttpHost( host, port, https?"https":"http" );
    	httpClient.getParams().setParameter( ConnRoutePNames.DEFAULT_PROXY, 
    			proxyHost );
    	
    	if( username != null ) {
    		httpClient.getCredentialsProvider().setCredentials(
    				new AuthScope(host, port),
    				new UsernamePasswordCredentials(username, password));
    	}
    }
    
    /**
     * Creates a new object in the cloud.
     * @param path The path to create the object on.
     * @param acl Access control list for the new object.  May be null
     * to use a default ACL
     * @param metadata Metadata for the new object.  May be null for
     * no metadata.
     * @param data The initial contents of the object.  May be appended
     * to later.  The stream will NOT be closed at the end of the request.
     * @param length The length of the stream in bytes.  If the stream
     * is longer than the length, only length bytes will be written.  If
     * the stream is shorter than the length, an error will occur.
     * @param mimeType the MIME type of the content.  Optional, 
     * may be null.  If data is non-null and mimeType is null, the MIME
     * type will default to application/octet-stream.
     * @return Identifier of the newly created object.
     * @throws EsuException if the request fails.
     */
    public ObjectId createObjectFromStreamOnPath( ObjectPath path, Acl acl, MetadataList metadata, 
            InputStream data, long length, String mimeType ) {
        try {
            String resource = getResourcePath(context, path);
            URL u = buildUrl(resource, null);

            if (data == null) {
                throw new IllegalArgumentException("Input stream is required");
            }

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            // Figure out the mimetype
            if (mimeType == null) {
                mimeType = "application/octet-stream";
            }

            headers.put("Content-Type", mimeType);
            headers.put("x-emc-uid", uid);

            // Process metadata
            if (metadata != null) {
                processMetadata(metadata, headers);
            }

            l4j.debug("meta " + headers.get("x-emc-meta"));

            // Add acl
            if (acl != null) {
                processAcl(acl, headers);
            }

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("POST", resource, null, headers);

            HttpResponse response = restPost( u, headers, data, length );

            // Check response
            handleError( response );

            // The new object ID is returned in the location response header
            String location = response.getFirstHeader("location").getValue();
            
            // Cleanup the connection
            cleanup( response );

            // Parse the value out of the URL
            return getObjectId( location );
        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    }



    /**
     * Creates a new object in the cloud.
     * 
     * @param acl Access control list for the new object. May be null to use a
     *            default ACL
     * @param metadata Metadata for the new object. May be null for no metadata.
     * @param data The initial contents of the object. May be appended to later.
     *            The stream will NOT be closed at the end of the request.
     * @param length The length of the stream in bytes. If the stream is longer
     *            than the length, only length bytes will be written. If the
     *            stream is shorter than the length, an error will occur.
     * @param mimeType the MIME type of the content. Optional, may be null. If
     *            data is non-null and mimeType is null, the MIME type will
     *            default to application/octet-stream.
     * @return Identifier of the newly created object.
     * @throws EsuException if the request fails.
     */
    public ObjectId createObjectFromStream(Acl acl, MetadataList metadata,
            InputStream data, long length, String mimeType) {
        try {
            String resource = context + "/objects";
            URL u = buildUrl(resource, null);

            if (data == null) {
                throw new IllegalArgumentException("Input stream is required");
            }

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            // Figure out the mimetype
            if (mimeType == null) {
                mimeType = "application/octet-stream";
            }

            headers.put("Content-Type", mimeType);
            headers.put("x-emc-uid", uid);

            // Process metadata
            if (metadata != null) {
                processMetadata(metadata, headers);
            }

            l4j.debug("meta " + headers.get("x-emc-meta"));

            // Add acl
            if (acl != null) {
                processAcl(acl, headers);
            }

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("POST", resource, null, headers);

            HttpResponse response = restPost( u, headers, data, length );

            // Check response
            handleError( response );

            // The new object ID is returned in the location response header
            String location = response.getFirstHeader("location").getValue();
            
            // Cleanup the connection
            cleanup( response );

            // Parse the value out of the URL
            return getObjectId( location );
        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
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
     * @param checksum if not null, use the Checksum object to compute
     * the checksum for the create object request.  If appending
     * to the object with subsequent requests, use the same
     * checksum object for each request.
     * @return Identifier of the newly created object.
     * @throws EsuException if the request fails.
     */
    public ObjectId createObjectFromSegment(Acl acl, MetadataList metadata,
            BufferSegment data, String mimeType, Checksum checksum) {

        try {
            String resource = context + "/objects";
            URL u = buildUrl(resource, null);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            // Figure out the mimetype
            if (mimeType == null) {
                mimeType = "application/octet-stream";
            }

            headers.put("Content-Type", mimeType);
            headers.put("x-emc-uid", uid);

            // Process metadata
            if (metadata != null) {
                processMetadata(metadata, headers);
            }

            l4j.debug("meta " + headers.get("x-emc-meta"));

            // Add acl
            if (acl != null) {
                processAcl(acl, headers);
            }

            // Process data
            if (data == null) {
                data = new BufferSegment(new byte[0]);
            }

            // Add date
            headers.put("Date", getDateHeader());

            // Compute checksum
            if( checksum != null ) {
            	checksum.update( data.getBuffer(), data.getOffset(), data.getSize() );
            	headers.put( "x-emc-wschecksum", checksum.toString() );
            }

            // Sign request
            signRequest("POST", resource, null, headers);
            
            HttpResponse response = restPost( u, headers, data );


            // Check response
            handleError( response );

            // The new object ID is returned in the location response header
            String location = response.getFirstHeader("location").getValue();
            
            // Cleanup the connection
            cleanup( response );

            // Parse the value out of the URL
            return getObjectId( location );
        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
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
     * @param checksum if not null, use the Checksum object to compute
     * the checksum for the create object request.  If appending
     * to the object with subsequent requests, use the same
     * checksum object for each request.
     * @return the ObjectId of the newly-created object for references by ID.
     * @throws EsuException if the request fails.
     */
    public ObjectId createObjectFromSegmentOnPath(ObjectPath path, Acl acl,
            MetadataList metadata, BufferSegment data, String mimeType, Checksum checksum) {
        try {
            String resource = getResourcePath(context, path);
            URL u = buildUrl(resource, null);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            // Figure out the mimetype
            if (mimeType == null) {
                mimeType = "application/octet-stream";
            }

            headers.put("Content-Type", mimeType);
            headers.put("x-emc-uid", uid);

            // Process metadata
            if (metadata != null) {
                processMetadata(metadata, headers);
            }

            l4j.debug("meta " + headers.get("x-emc-meta"));

            // Add acl
            if (acl != null) {
                processAcl(acl, headers);
            }

            // Process data
            if (data == null) {
                data = new BufferSegment(new byte[0]);
            }

            // Add date
            headers.put("Date", getDateHeader());

            // Compute checksum
            if( checksum != null ) {
            	checksum.update( data.getBuffer(), data.getOffset(), data.getSize() );
            	headers.put( "x-emc-wschecksum", checksum.toString() );
            }

            // Sign request
            signRequest("POST", resource, null, headers);
            HttpResponse response = restPost( u, headers, data );

            // Check response
            handleError( response );

            // The new object ID is returned in the location response header
            String location = response.getFirstHeader("location").getValue();
            
            // Cleanup the connection
            cleanup( response );

            // Parse the value out of the URL
            return getObjectId( location );

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    }

    /**
     * Deletes an object from the cloud.
     * 
     * @param id the identifier of the object to delete.
     */
    public void deleteObject(Identifier id) {
        try {
            String resource = getResourcePath(context, id);
            URL u = buildUrl(resource, null);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("DELETE", resource, null, headers);
            
            HttpResponse response = restDelete( u, headers );
            
            handleError( response );
            
            cleanup( response );

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    }
    
    /**
     * Deletes a version of an object from the cloud.
     * 
     * @param id the identifier of the object to delete.
     */
    public void deleteVersion(ObjectId id) {
        try {
            String resource = getResourcePath(context, id);
            String query = "versions";
            URL u = buildUrl(resource, query);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("DELETE", resource, query, headers);
            
            HttpResponse response = restDelete( u, headers );
            
            handleError( response );
            
            cleanup( response );

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    }

    /**
     * Deletes metadata items from an object.
     * 
     * @param id the identifier of the object whose metadata to delete.
     * @param tags the list of metadata tags to delete.
     */
    public void deleteUserMetadata(Identifier id, MetadataTags tags) {
        if (tags == null) {
            throw new EsuException("Must specify tags to delete");
        }
        try {
            String resource = getResourcePath(context, id);
            String query = "metadata/user";
            URL u = buildUrl(resource, query );

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            // process tags
            if (tags != null) {
                processTags(tags, headers);
            }

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("DELETE", resource, query, headers);
            
            HttpResponse response = restDelete( u, headers );
            
            handleError( response );
            
            finishRequest( response );
            
        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    }

    /**
     * Returns an object's ACL
     * 
     * @param id the identifier of the object whose ACL to read
     * @return the object's ACL
     */
    public Acl getAcl(Identifier id) {
        try {
            String resource = getResourcePath(context, id);
            String query = "acl";
            URL u = buildUrl(resource, query);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("GET", resource, query, headers);
            HttpResponse response = restGet( u, headers );
            
            handleError(response);

            // Parse return headers. User grants are in x-emc-useracl and
            // group grants are in x-emc-groupacl
            Acl acl = new Acl();
            readAcl(acl, response.getFirstHeader("x-emc-useracl").getValue(),
                    Grantee.GRANT_TYPE.USER);
            readAcl(acl, response.getFirstHeader("x-emc-groupacl").getValue(),
                    Grantee.GRANT_TYPE.GROUP);

            finishRequest( response );
            return acl;

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    }

    /**
     * Returns a list of the tags that are listable the current user's tennant.
     * 
     * @param tag optional. If specified, the list will be limited to the tags
     *            under the specified tag. If null, only top level tags will be
     *            returned.
     * @return the list of listable tags.
     */
    public MetadataTags getListableTags(MetadataTag tag) {
        return getListableTags(tag == null ? null : tag.getName());
    }

    /**
     * Returns a list of the tags that are listable the current user's tennant.
     * 
     * @param tag optional. If specified, the list will be limited to the tags
     *            under the specified tag. If null, only top level tags will be
     *            returned.
     * @return the list of listable tags.
     */
    public MetadataTags getListableTags(String tag) {
        try {
            String resource = context + "/objects";
            String query = "listabletags";
            URL u = buildUrl(resource, query);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            if(unicodeEnabled) {
                headers.put("x-emc-utf8", "true");
            }

            // Add tag
            if (tag != null) {
                headers.put("x-emc-tags", unicodeEnabled ? encodeUtf8(tag) : tag);
            }

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("GET", resource, query, headers);
            
            HttpResponse response = restGet( u, headers );
            handleError( response );

            Header header = response.getFirstHeader("x-emc-listable-tags");
            MetadataTags tags = new MetadataTags();
            if (header != null) {
                l4j.debug("x-emc-listable-tags: " + header);
                readTags(tags, header.getValue(), true);
            }

            finishRequest( response );
            return tags;
        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    }

    /**
     * Fetches the system metadata for the object.
     * 
     * @param id the identifier of the object whose system metadata to fetch.
     * @param tags A list of system metadata tags to fetch. Optional. Default
     *            value is null to fetch all system metadata.
     * @return The list of system metadata for the object.
     */
    public MetadataList getSystemMetadata(Identifier id, MetadataTags tags) {
        try {
            String resource = getResourcePath(context, id);
            String query = "metadata/system";
            URL u = buildUrl(resource, query);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            // process tags
            if (tags != null) {
                processTags(tags, headers);
            }

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("GET", resource, query, headers);

            HttpResponse response = restGet( u, headers );
            handleError( response );

            // Parse return headers. Regular metadata is in x-emc-meta and
            // listable metadata is in x-emc-listable-meta
            MetadataList meta = new MetadataList();
            readMetadata(meta, response.getFirstHeader("x-emc-meta"), false);
            readMetadata(meta, response.getFirstHeader("x-emc-listable-meta"), true);

            finishRequest( response );
            return meta;

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    }

    /**
     * Fetches the user metadata for the object.
     * 
     * @param id the identifier of the object whose user metadata to fetch.
     * @param tags A list of user metadata tags to fetch. Optional. If null, all
     *            user metadata will be fetched.
     * @return The list of user metadata for the object.
     */
    public MetadataList getUserMetadata(Identifier id, MetadataTags tags) {
        try {
            String resource = getResourcePath(context, id);
            String query = "metadata/user";
            URL u = buildUrl(resource, query);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            if(unicodeEnabled) {
                headers.put("x-emc-utf8", "true");
            }

            // process tags
            if (tags != null) {
                processTags(tags, headers);
            }

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("GET", resource, query, headers);

            HttpResponse response = restGet( u, headers );
            handleError( response );

            // Parse return headers. Regular metadata is in x-emc-meta and
            // listable metadata is in x-emc-listable-meta
            MetadataList meta = new MetadataList();
            readMetadata(meta, response.getFirstHeader("x-emc-meta"), false);
            readMetadata(meta, response.getFirstHeader("x-emc-listable-meta"), true);

            finishRequest( response );
            return meta;

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    }

    /**
     * Lists all objects with the given tag.
     * 
     * @param tag the tag to search for
     * @return The list of objects with the given tag. If no objects are found
     *         the List will be empty.
     */
    public List<ObjectResult> listObjects(String tag, ListOptions options) {
        try {
            String resource = context + "/objects";
            URL u = buildUrl(resource, null);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            if(unicodeEnabled) {
                headers.put("x-emc-utf8", "true");
            }

            // Add tag
            if (tag != null) {
                headers.put("x-emc-tags", unicodeEnabled ? encodeUtf8(tag) : tag);
            } else {
                throw new EsuException("Tag cannot be null");
            }
            
            // Process options
            if( options != null ) {
            	if( options.isIncludeMetadata() ) {
            		headers.put( "x-emc-include-meta", "1" );
            		if( options.getSystemMetadata() != null ) {
            			headers.put( "x-emc-system-tags", 
            					join( options.getSystemMetadata(), "," ) );
            		}
            		if( options.getUserMetadata() != null ) {
            			headers.put( "x-emc-user-tags", 
            					join( options.getUserMetadata(), "," ) );            			
            		}
            	}
            	if( options.getLimit() > 0 ) {
            		headers.put( "x-emc-limit", ""+options.getLimit() );
            	}
            	if( options.getToken() != null ) {
            		headers.put( "x-emc-token", options.getToken() );
            	}
            }


            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("GET", resource, null, headers);
            
            HttpResponse response = restGet( u, headers );
            
            try {
            	handleError( response );
            } catch( EsuException e ) {
            	if( e.getAtmosCode() == 1003 ) {
            		return Collections.emptyList();
            	}
            	throw e;
            }
            
            if( options != null ) {
            	// Update the token for listing more results.  If there are no
            	// more results, the header will not be set and the token will
            	// be cleared in the options object.
            	Header token = response.getFirstHeader("x-emc-token");
            	if( token != null ) {
            		options.setToken( token.getValue() );
            	} else {
            		options.setToken( null );
            	}
            } else {
            	Header token = response.getFirstHeader("x-emc-token");
            	if( token != null ) {
            		l4j.warn( "Result set truncated. Use ListOptions to " +
    				"retrieve token for next page of results." );
            	}            	
            }

            // Get object id list from response
            byte[] data = readStream( response.getEntity().getContent(), 
                    (int) response.getEntity().getContentLength() );

            if( l4j.isDebugEnabled() ) {
                l4j.debug("Response: " + new String(data, "UTF-8"));
            }
            finishRequest( response );

            return parseObjectListWithMetadata(data);

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    }

    /**
     * Returns the list of user metadata tags assigned to the object.
     * 
     * @param id the object whose metadata tags to list
     * @return the list of user metadata tags assigned to the object
     */
    public MetadataTags listUserMetadataTags(Identifier id) {
        try {
            String resource = getResourcePath(context, id);
            String query = "metadata/tags";
            URL u = buildUrl(resource, query);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            if(unicodeEnabled) {
                headers.put("x-emc-utf8", "true");
            }

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("GET", resource, query, headers);
            
            HttpResponse response = restGet( u, headers );
            handleError( response );

            // Get the user metadata tags out of x-emc-listable-tags and
            // x-emc-tags
            MetadataTags tags = new MetadataTags();

            Header listableHeader = response.getFirstHeader( "x-emc-listable-tags" );
            Header metaHeader = response.getFirstHeader( "x-emc-tags" );
            if (listableHeader != null) readTags(tags, listableHeader.getValue(), true);
            if (metaHeader != null) readTags(tags, metaHeader.getValue(), false);

            finishRequest( response );
            
            return tags;

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    }

    /**
     * Lists the versions of an object.
     * 
     * @param id the object whose versions to list.
     * @return The list of versions of the object. If the object does not have
     *         any versions, the array will be empty.
     */
    public List<Identifier> listVersions(Identifier id) {
        try {
            String resource = getResourcePath(context, id);
            String query = "versions";
            URL u = buildUrl(resource, query);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("GET", resource, query, headers);

            HttpResponse response = restGet( u, headers );
            handleError( response );

            // Get object id list from response
            byte[] data = readStream( response.getEntity().getContent(), 
                    (int) response.getEntity().getContentLength() );

            if( l4j.isDebugEnabled() ) {
                l4j.debug("Response: " + new String(data, "UTF-8"));
            }
            finishRequest( response );

            return parseVersionList(data);

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    }

    public List<Version> listVersions(ObjectId id, ListOptions options) {
        try {
            String resource = getResourcePath(context, id);
            String query = "versions";
            URL u = buildUrl(resource, query);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            // Process options
            if( options != null ) {
            	if( options.isIncludeMetadata() ) {
            		l4j.warn("Include metadata is not supported for listVersions");
            	}
            	if( options.getLimit() > 0 ) {
            		headers.put( "x-emc-limit", ""+options.getLimit() );
            	}
            	if( options.getToken() != null ) {
            		headers.put( "x-emc-token", options.getToken() );
            	}
            }

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("GET", resource, query, headers);

            HttpResponse response = restGet( u, headers );
            handleError( response );

            if( options != null ) {
            	// Update the token for listing more results.  If there are no
            	// more results, the header will not be set and the token will
            	// be cleared in the options object.
            	Header token = response.getFirstHeader("x-emc-token");
            	if( token != null ) {
            		options.setToken( token.getValue() );
            	} else {
            		options.setToken( null );
            	}
            } else {
            	Header token = response.getFirstHeader("x-emc-token");
            	if( token != null ) {
            		l4j.warn( "Result set truncated. Use ListOptions to " +
    				"retrieve token for next page of results." );
            	}            	
            }

            // Get object id list from response
            byte[] data = readStream( response.getEntity().getContent(), 
                    (int) response.getEntity().getContentLength() );

            if( l4j.isDebugEnabled() ) {
                l4j.debug("Response: " + new String(data, "UTF-8"));
            }
            finishRequest( response );

            return parseVersionListLong(data);

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    }

    
    /**
     * Executes a query for objects matching the specified XQuery string.
     * 
     * @param xquery the XQuery string to execute against the cloud.
     * @return the list of objects matching the query. If no objects are found,
     *         the array will be empty.
     */
    public List<ObjectId> queryObjects(String xquery) {
        try {
            String resource = context + "/objects";
            URL u = buildUrl(resource, null);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            // Add query
            if (xquery != null) {
                headers.put("x-emc-xquery", xquery);
            } else {
                throw new EsuException("Query cannot be null");
            }

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("GET", resource, null, headers);
            
            HttpResponse response = restGet( u, headers );
            handleError( response );

            // Get object id list from response
            byte[] data = readStream( response.getEntity().getContent(), 
                    (int) response.getEntity().getContentLength() );

            if( l4j.isDebugEnabled() ) {
                l4j.debug("Response: " + new String(data, "UTF-8"));
            }
            finishRequest( response );
            return parseObjectList(data);

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
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
     * @param checksum if not null, the given checksum object will be used
     * to verify checksums during the read operation.  Note that only erasure
     * coded objects will return checksums *and* if you're reading the object
     * in chunks, you'll have to read the data back sequentially to keep
     * the checksum consistent.  If the read operation does not return
     * a checksum from the server, the checksum operation will be skipped.
     * @return the object data read as a byte array.
     */
    public byte[] readObject(Identifier id, Extent extent, byte[] buffer, Checksum checksum) {
        try {
            String resource = getResourcePath(context, id);
            URL u = buildUrl(resource, null);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            // Add date
            headers.put("Date", getDateHeader());

            // Add extent if needed
            if (extent != null && !extent.equals(Extent.ALL_CONTENT)) {
                headers.put(extent.getHeaderName(), extent.toString());
            }

            // Sign request
            signRequest("GET", resource, null, headers);
            HttpResponse response = restGet( u, headers );
            handleError( response );

            // The requested content is in the response body.
            byte[] data = readStream( response.getEntity().getContent(), 
                    (int) response.getEntity().getContentLength() );

            finishRequest( response );
            
            // See if a checksum was returned.
            Header checksumHeader = response.getFirstHeader("x-emc-wschecksum");
            
            if( checksumHeader != null && checksum != null ) {
            	String checksumStr = checksumHeader.getValue();
            	l4j.debug( "Checksum header: " + checksumStr );
            	checksum.setExpectedValue( checksumStr );
            	if( response.getEntity().getContentLength() != -1 ) {
            		checksum.update( data, 0, (int)response.getEntity().getContentLength() );
            	} else {
            		checksum.update( data, 0, data.length );
            	}
            }

            return data;

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }

    }

    /**
     * Reads an object's content and returns an InputStream to read the content.
     * Since the input stream is linked to the HTTP connection, it is imperative
     * that you close the input stream as soon as you are done with the stream
     * to release the underlying connection.
     * 
     * @param id the identifier of the object whose content to read.
     * @param extent the portion of the object data to read. Optional. Default
     *            is null to read the entire object.
     * @return an InputStream to read the object data.
     */
    public InputStream readObjectStream(Identifier id, Extent extent) {
        try {
            String resource = getResourcePath(context, id);
            URL u = buildUrl(resource, null);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            // Add date
            headers.put("Date", getDateHeader());

            // Add extent if needed
            if (extent != null && !extent.equals(Extent.ALL_CONTENT)) {
                headers.put(extent.getHeaderName(), extent.toString());
            }

            // Sign request
            signRequest("GET", resource, null, headers);
            
            HttpResponse response = restGet( u, headers );
            handleError( response );

            return new CommonsInputStreamWrapper( response );

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
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
    public void updateObjectFromSegment(Identifier id, Acl acl,
            MetadataList metadata, Extent extent, BufferSegment data,
            String mimeType, Checksum checksum) {
        try {
            String resource = getResourcePath(context, id);
            URL u = buildUrl(resource, null);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            // Figure out the mimetype
            if (mimeType == null) {
                mimeType = "application/octet-stream";
            }

            headers.put("Content-Type", mimeType);
            headers.put("x-emc-uid", uid);

            // Process metadata
            if (metadata != null) {
                processMetadata(metadata, headers);
            }

            l4j.debug("meta " + headers.get("x-emc-meta"));

            // Add acl
            if (acl != null) {
                processAcl(acl, headers);
            }

            // Add extent if needed
            if (extent != null && !extent.equals(Extent.ALL_CONTENT)) {
                headers.put(extent.getHeaderName(), extent.toString());
            }

            // Process data
            if (data == null) {
                data = new BufferSegment(new byte[0]);
            }

            // Add date
            headers.put("Date", getDateHeader());

            // Compute checksum
            if( checksum != null ) {
            	checksum.update( data.getBuffer(), data.getOffset(), data.getSize() );
            	headers.put( "x-emc-wschecksum", checksum.toString() );
            }

            // Sign request
            signRequest("PUT", resource, null, headers);
            
            HttpResponse response = restPut( u, headers, data );
            handleError( response );
            
            finishRequest( response );
        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }

    }

    /**
     * Updates an object in the cloud.
     * 
     * @param id The ID of the object to update
     * @param acl Access control list for the new object. Optional, default is
     *            NULL to leave the ACL unchanged.
     * @param metadata Metadata list for the new object. Optional, default is
     *            NULL for no changes to the metadata.
     * @param data The updated data to apply to the object. Requred. Note that
     *            the input stream is NOT closed at the end of the request.
     * @param extent portion of the object to update. May be null to indicate
     *            the whole object is to be replaced. If not null, the extent
     *            size must match the data size.
     * @param length The length of the stream in bytes. If the stream is longer
     *            than the length, only length bytes will be written. If the
     *            stream is shorter than the length, an error will occur.
     * @param mimeType the MIME type of the content. Optional, may be null. If
     *            data is non-null and mimeType is null, the MIME type will
     *            default to application/octet-stream.
     * @throws EsuException if the request fails.
     */
    public void updateObjectFromStream(Identifier id, Acl acl,
            MetadataList metadata, Extent extent, InputStream data, long length,
            String mimeType) {
        try {
            String resource = getResourcePath(context, id);
            URL u = buildUrl(resource, null);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            // Figure out the mimetype
            if (mimeType == null) {
                mimeType = "application/octet-stream";
            }

            headers.put("Content-Type", mimeType);
            headers.put("x-emc-uid", uid);

            // Process metadata
            if (metadata != null) {
                processMetadata(metadata, headers);
            }

            l4j.debug("meta " + headers.get("x-emc-meta"));

            // Add acl
            if (acl != null) {
                processAcl(acl, headers);
            }

            // Add extent if needed
            if (extent != null && !extent.equals(Extent.ALL_CONTENT)) {
                headers.put(extent.getHeaderName(), extent.toString());
            }

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("PUT", resource, null, headers);
            
            HttpResponse response = restPut( u, headers, data, length );
            handleError( response );
            
            finishRequest( response );
            
        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }

    }

    /**
     * Writes the metadata into the object. If the tag does not exist, it is
     * created and set to the corresponding value. If the tag exists, the
     * existing value is replaced.
     * 
     * @param id the identifier of the object to update
     * @param metadata metadata to write to the object.
     */
    public void setUserMetadata(Identifier id, MetadataList metadata) {
        try {
            String resource = getResourcePath(context, id);
            String query = "metadata/user";
            URL u = buildUrl(resource, query);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            // Process metadata
            if (metadata != null) {
                processMetadata(metadata, headers);
            }

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("POST", resource, query, headers);
            
            HttpResponse response = restPost( u, headers, null );
            handleError( response );
            
            finishRequest( response );
        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }

    }

    /**
     * Sets (overwrites) the ACL on the object.
     * 
     * @param id the identifier of the object to change the ACL on.
     * @param acl the new ACL for the object.
     */
    public void setAcl(Identifier id, Acl acl) {
        try {
            String resource = getResourcePath(context, id);
            String query = "acl";
            URL u = buildUrl(resource, query);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            // Add acl
            if (acl != null) {
                processAcl(acl, headers);
            }

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("POST", resource, query, headers);
            
            HttpResponse response = restPost( u, headers, null );
            handleError( response );
            
            finishRequest( response );
            
        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }

    }

    /**
     * Creates a new immutable version of an object.
     * 
     * @param id the object to version
     * @return the id of the newly created version
     */
    public ObjectId versionObject(Identifier id) {
        try {
            String resource = getResourcePath(context, id);
            String query = "versions";
            URL u = buildUrl(resource, query);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("POST", resource, query, headers);
            
            HttpResponse response = restPost( u, headers, null );
            handleError( response );
            
            // The new object ID is returned in the location response header
            String location = response.getFirstHeader("location").getValue();
            
            finishRequest( response );

            // Parse the value out of the URL
            return getObjectId( location );

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    }


    /**
     * Lists the contents of a directory.
     * 
     * @param path the path to list. Must be a directory.
     * @return the directory entries in the directory.
     */
	public List<DirectoryEntry> listDirectory(ObjectPath path, 
			ListOptions options) {
    	
        if (!path.isDirectory()) {
            throw new EsuException(
                    "listDirectory must be called with a directory path");
        }

        // Read out the directory's contents
        byte[] data = null;
        try {
            String resource = getResourcePath(context, path);
            URL u = buildUrl(resource, null);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            // Process options
            if( options != null ) {
            	if( options.isIncludeMetadata() ) {
            		headers.put( "x-emc-include-meta", "1" );
            		if( options.getSystemMetadata() != null ) {
            			headers.put( "x-emc-system-tags", 
            					join( options.getSystemMetadata(), "," ) );
            		}
            		if( options.getUserMetadata() != null ) {
            			headers.put( "x-emc-user-tags", 
            					join( options.getUserMetadata(), "," ) );            			
            		}
            	}
            	if( options.getLimit() > 0 ) {
            		headers.put( "x-emc-limit", ""+options.getLimit() );
            	}
            	if( options.getToken() != null ) {
            		headers.put( "x-emc-token", options.getToken() );
            	}
            }

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("GET", resource, null, headers);
            HttpResponse response = restGet( u, headers );
            handleError( response );

            if( options != null ) {
            	// Update the token for listing more results.  If there are no
            	// more results, the header will not be set and the token will
            	// be cleared in the options object.
            	Header token = response.getFirstHeader("x-emc-token");
            	if( token != null ) {
            		options.setToken( token.getValue() );
            	} else {
            		options.setToken( null );
            	}
            } else {
            	Header token = response.getFirstHeader("x-emc-token");
            	if( token != null ) {
            		l4j.warn( "Result set truncated. Use ListOptions to " +
    				"retrieve token for next page of results." );
            	}            	
            }

            // The requested content is in the response body.
            data = readStream( response.getEntity().getContent(), 
                    (int) response.getEntity().getContentLength() );

            finishRequest( response );
            
        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }

 
        return parseDirectoryListing( data, path );

    }

    public ObjectMetadata getAllMetadata(Identifier id) {
        try {
            String resource = getResourcePath(context, id);
            URL u = buildUrl(resource, null);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            if(unicodeEnabled) {
                headers.put("x-emc-utf8", "true");
            }

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("HEAD", resource, null, headers);
            
            HttpResponse response = restHead( u, headers );
            handleError( response );
 
            // Parse return headers. User grants are in x-emc-useracl and
            // group grants are in x-emc-groupacl
            Acl acl = new Acl();
            readAcl(acl, response.getFirstHeader("x-emc-useracl").getValue(),
                    Grantee.GRANT_TYPE.USER);
            readAcl(acl, response.getFirstHeader("x-emc-groupacl").getValue(),
                    Grantee.GRANT_TYPE.GROUP);

            // Parse return headers. Regular metadata is in x-emc-meta and
            // listable metadata is in x-emc-listable-meta
            MetadataList meta = new MetadataList();
            readMetadata(meta, response.getFirstHeader("x-emc-meta"), false);
            readMetadata(meta, response.getFirstHeader("x-emc-listable-meta"), true);

            ObjectMetadata om = new ObjectMetadata();
            om.setAcl(acl);
            om.setMetadata(meta);
            om.setMimeType(response.getFirstHeader( "Content-Type").getValue());

            finishRequest( response );
            
            return om;

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    }
    
	public ServiceInformation getServiceInformation() {
        try {
            String resource = context + "/service";
            URL u = buildUrl(resource, null);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("GET", resource, null, headers);
            
            HttpResponse response = restGet( u, headers );
            handleError( response );

            // Get object id list from response
            byte[] data = readStream( response.getEntity().getContent(), 
                    (int) response.getEntity().getContentLength() );

            if( l4j.isDebugEnabled() ) {
                l4j.debug("Response: " + new String(data, "UTF-8"));
            }
            finishRequest( response );
            
            Map<String, List<String>> rHeaders = getResponseHeaders(response);

            return parseServiceInformation(data, rHeaders);

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
	}
	
	
	/**
     * Renames a file or directory within the namespace.
     * @param source The file or directory to rename
     * @param destination The new path for the file or directory
     * @param force If true, the desination file or 
     * directory will be overwritten.  Directories must be empty to be 
     * overwritten.  Also note that overwrite operations on files are
     * not synchronous; a delay may be required before the object is
     * available at its destination.
     */
    public void rename(ObjectPath source, ObjectPath destination, boolean force) {
        try {
            String resource = getResourcePath(context, source);
            String query = "rename";
            URL u = buildUrl(resource, query);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            if(unicodeEnabled) {
                headers.put("x-emc-utf8", "true");
            }

            String destPath = destination.toString();
            if (destPath.startsWith("/"))
            {
                destPath = destPath.substring(1);
            }
            headers.put("x-emc-path", unicodeEnabled ? encodeUtf8(destPath) : destPath);

            if (force) {
                headers.put("x-emc-force", "true");
            }

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("POST", resource, query, headers);
            
            HttpResponse response = restPost( u, headers, null );
            handleError( response );
            
            finishRequest( response );

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    	
    }

    /**
     * Restores a version of an object to the base version (i.e. "promote" an 
     * old version to the current version).
     * @param id Base object ID (target of the restore)
     * @param vId Version object ID to restore
     */
    public void restoreVersion( ObjectId id, ObjectId vId ) {
        try {
            String resource = getResourcePath(context, id);
            String query = "versions";
            URL u = buildUrl(resource, query);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);
            
            // Version to promote
            headers.put("x-emc-version-oid", vId.toString());

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("PUT", resource, query, headers);
            
            HttpResponse response = restPut( u, headers, null );
            
            handleError( response );
            
            cleanup( response );

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    	
    }
    
    /**
     * Get information about an object's state including
     * replicas, expiration, and retention.
     * @param id the object identifier
     * @return and ObjectInfo object containing the state information
     */
    public ObjectInfo getObjectInfo( Identifier id ) {
        try {
            String resource = getResourcePath(context, id);
            String query = "info";
            URL u = buildUrl(resource, query);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("GET", resource, query, headers);

            HttpResponse response = restGet( u, headers );
            handleError( response );

            // Get object id list from response
            byte[] data = readStream( response.getEntity().getContent(), 
                    (int) response.getEntity().getContentLength() );

            String responseXml = new String(data, "UTF-8");
            if( l4j.isDebugEnabled() ) {
                l4j.debug("Response: " + responseXml);
            }
            finishRequest( response );

            return new ObjectInfo(responseXml);

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    	
    }

	@Override
	public long calculateServerOffset() {
		try {
	        String resource = context + "/service";
	        URL u = buildUrl(resource, null);
	
	        // Build headers
	        Map<String, String> headers = new HashMap<String, String>();
	
	        headers.put("x-emc-uid", uid);
	
	        // Add date
	        headers.put("Date", getDateHeader());
	
	        // Sign request
	        signRequest("GET", resource, null, headers);
	        
	        HttpResponse response = restGet( u, headers );
	        Header responseDate = response.getFirstHeader(HttpHeaders.DATE);
	        if(responseDate == null) {
	        	throw new EsuException("Could not get date from response: " + 
	        			response.getStatusLine().getStatusCode() + ": " + 
	        			response.getStatusLine().getReasonPhrase());
	        }

	        Date serverDate = null;
	        try {
				serverDate = DateUtils.parseDate(responseDate.getValue());
			} catch (DateParseException e) {
				throw new EsuException("Failed to parse date: " + responseDate.getValue(), e);
			}
	        
	        return System.currentTimeMillis() - serverDate.getTime();
        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
	}


    
    
    /////////////////////
    // Private Methods //
    /////////////////////
    
    private void handleError(HttpResponse resp) {
        StatusLine status = resp.getStatusLine();
        if( status.getStatusCode() > 299 ) {
            try {
                HttpEntity body = resp.getEntity();
                if( body == null ) {
                    throw new EsuException( status.getReasonPhrase(), status.getStatusCode() );
                }
                
                byte[] response = readStream( body.getContent(), (int) body.getContentLength() );
                l4j.debug("Error response: " + new String(response, "UTF-8"));
                SAXBuilder sb = new SAXBuilder();

                Document d = sb.build(new ByteArrayInputStream(response));

                String code = d.getRootElement().getChildText("Code");
                String message = d.getRootElement().getChildText("Message");

                if (code == null && message == null) {
                    // not an error from ESU
                    throw new EsuException( status.getReasonPhrase(), status.getStatusCode() );
                }

                l4j.debug("Error: " + code + " message: " + message);
                throw new EsuException(message, status.getStatusCode(), Integer.parseInt(code));

            } catch (IOException e) {
                l4j.debug("Could not read error response body", e);
                // Just throw what we know from the response
                throw new EsuException(status.getReasonPhrase(), status.getStatusCode());
            } catch (JDOMException e) {
                l4j.debug("Could not parse response body for " + status.getStatusCode()
                        + ": " + status.getReasonPhrase(), e);
                throw new EsuException("Could not parse response body for "
                        + status.getStatusCode() + ": " + status.getReasonPhrase(), e,
                        status.getStatusCode());

            } finally {
                if( resp.getEntity() != null ) {
                    try {
                    	EntityUtils.consume( resp.getEntity() );
                    } catch (IOException e) {
                        l4j.warn( "Error finishing error response", e );
                    }
                }
            }

        }
    }

    private HttpResponse restPost( URL url, Map<String,String> headers, InputStream in, long contentLength ) throws URISyntaxException, ClientProtocolException, IOException {
        HttpPost post = new HttpPost( url.toURI() );
        
        setHeaders( post, headers );
        
        if( in != null ) {
            post.setEntity( new InputStreamEntity( in, contentLength ) );
        } else {
            post.setEntity( new ByteArrayEntity( new byte[0] ) );
        }
        
        return httpClient.execute( post );
    }
    
    private HttpResponse restPost( URL url, Map<String,String> headers, BufferSegment data ) throws URISyntaxException, ClientProtocolException, IOException {
        HttpPost post = new HttpPost( url.toURI() );
        
        setHeaders( post, headers );
        
        if( data != null ) {
            if( data.getOffset() == 0 && (data.getSize() == data.getBuffer().length ) ) {
                // use the native byte array
                post.setEntity( new ByteArrayEntity( data.getBuffer() ) );
            } else {
                post.setEntity( new InputStreamEntity( 
                        new ByteArrayInputStream(data.getBuffer(), data.getOffset(), data.getSize()),
                        data.getSize() ) );
            }
        } else {
            post.setEntity( new ByteArrayEntity( new byte[0] ) );
        }
        
        return httpClient.execute( post );
    }

    private HttpResponse restDelete(URL url, Map<String, String> headers) throws URISyntaxException, ClientProtocolException, IOException {
        HttpDelete delete = new HttpDelete( url.toURI() );
        
        setHeaders(delete, headers);
        
        return httpClient.execute( delete );
    }

    private HttpResponse restGet(URL url, Map<String, String> headers) throws URISyntaxException, ClientProtocolException, IOException {
        HttpGet get = new HttpGet( url.toURI() );
        
        setHeaders(get, headers);
        
        return httpClient.execute( get );
    }

    private HttpResponse restPut(URL url, Map<String, String> headers,
            BufferSegment data) throws ClientProtocolException, IOException, URISyntaxException {
        
        HttpPut put = new HttpPut( url.toURI() );
        
        setHeaders( put, headers );
        
        if( data != null ) {
            if( data.getOffset() == 0 && (data.getSize() == data.getBuffer().length ) ) {
                // use the native byte array
                put.setEntity( new ByteArrayEntity( data.getBuffer() ) );
            } else {
                put.setEntity( new InputStreamEntity( 
                        new ByteArrayInputStream(data.getBuffer(), data.getOffset(), data.getSize()),
                        data.getSize() ) );
            }
        } else {
            put.setEntity( new ByteArrayEntity( new byte[0] ) );
        }
        
        return httpClient.execute( put );

    }

    private HttpResponse restPut(URL url, Map<String, String> headers,
            InputStream in, long contentLength) throws URISyntaxException, ClientProtocolException, IOException {
        HttpPut put = new HttpPut( url.toURI() );
        
        setHeaders( put, headers );
        
        if( in != null ) {
            put.setEntity( new InputStreamEntity( in, contentLength ) );
        } else {
            put.setEntity( new ByteArrayEntity( new byte[0] ) );
        }
        
        return httpClient.execute( put );
    }

    private HttpResponse restHead(URL url, Map<String, String> headers) throws URISyntaxException, ClientProtocolException, IOException {
        HttpHead head = new HttpHead( url.toURI() );
        
        setHeaders( head, headers );
        
        return httpClient.execute( head );
    }
    
    private void setHeaders( AbstractHttpMessage request, Map<String, String> headers ) {
        for( String headerName : headers.keySet() ) {
            request.addHeader( headerName, headers.get( headerName ) );
        }
    }
    
    private void finishRequest(HttpResponse response) throws IOException {
        if( response.getEntity() != null ) {
            cleanup( response );
        }
    }

    private void readMetadata(MetadataList meta, Header firstHeader,
            boolean listable) throws UnsupportedEncodingException {
        if( firstHeader != null ) {
            super.readMetadata(meta, firstHeader.getValue(), listable );
        }
    }

    private void cleanup( HttpResponse response ) throws IOException {
    	if( response.getEntity() != null ) {
    		EntityUtils.consume( response.getEntity() );
    	}
    }
    
    /**
     * Generates the HMAC-SHA1 signature used to authenticate the request using
     * the Java security APIs.
     * 
     * @param method the HTTP method used
     * @param resource the resource path
     * @param query the URL querystring (if present)
     * @param headers the HTTP headers for the request
     * @throws IOException if character data cannot be encoded.
     * @throws GeneralSecurityException If errors occur generating the HMAC-SHA1
     *             signature.
     */
    private void signRequest(String method, String resource, String query, Map<String, String> headers) throws IOException,
            GeneralSecurityException {
        // Build the string to hash.
        StringBuffer hashStr = new StringBuffer();
        hashStr.append(method + "\n");

        // If content type exists, add it. Otherwise add a blank line.
        if (headers.containsKey("Content-Type")) {
            l4j.debug("Content-Type: " + headers.get("Content-Type"));
            hashStr.append(headers.get("Content-Type") + "\n");
        } else {
            hashStr.append("\n");
        }

        // If the range header exists, add it. Otherwise add a blank line.
        if (headers.containsKey("Range")) {
            hashStr.append(headers.get("Range") + "\n");
        } else if (headers.containsKey("Content-Range")) {
            hashStr.append(headers.get("Content-Range") + "\n");
        } else {
            hashStr.append("\n");
        }

        // Add the current date and the resource.
        hashStr.append(headers.get("Date") + "\n" );
        
        hashStr.append( resource.toLowerCase() );
        if ( query != null) {
            hashStr.append("?" + query + "\n");
        } else {
            hashStr.append("\n");
        }

        // Do the 'x-emc' headers. The headers must be hashed in alphabetic
        // order and the values must be stripped of whitespace and newlines.
        List<String> keys = new ArrayList<String>();
        Map<String, String> newheaders = new HashMap<String, String>();

        // Extract the keys and values
        for (Iterator<String> i = headers.keySet().iterator(); i.hasNext();) {
            String key = i.next();
            if (key.indexOf("x-emc") == 0) {
                keys.add(key.toLowerCase());
                newheaders.put(key.toLowerCase(), headers.get(key).replace(
                        "\n", ""));
            }
        }

        // Sort the keys and add the headers to the hash string.
        Collections.sort(keys);
        boolean first = true;
        for (Iterator<String> i = keys.iterator(); i.hasNext();) {
            String key = i.next();
            if (!first) {
                hashStr.append("\n");
            } else {
                first = false;
            }
            // this.trace( "xheader: " . k . "." . newheaders[k] );
            hashStr.append(key + ':' + normalizeSpace(newheaders.get(key)));
        }

        String hashOut = sign(hashStr.toString());
        
        headers.put( "x-emc-signature", hashOut );

    }

    /**
     * Extracts the values of the response headers into a Map<String,<List<String>>
     * @param response the HttpResponse to parse.
     */
    private Map<String, List<String>> getResponseHeaders(HttpResponse response) {
    	Map<String, List<String>> rHeaders = new HashMap<String, List<String>>();
		Header[] headers = response.getAllHeaders();
		for(Header h : headers) {
			String name = h.getName();
			String value = h.getValue();
			List<String> hValues = null;
			if(rHeaders.containsKey(name)) {
				hValues = rHeaders.get(name);
			} else {
				hValues = new ArrayList<String>();
				rHeaders.put(name, hValues);
			}
			hValues.add(value);
		}
		
		return Collections.unmodifiableMap(rHeaders);
	}

    //---------- Features supported by the Atmos 2.0 REST API. ----------\\


    @Override
    public ObjectId createObjectWithKeyFromSegment( String keyPool, String key, Acl acl, MetadataList metadata,
                                                    BufferSegment data, String mimeType, Checksum checksum ) {
        try {
            String resource = context + "/namespace/" + key;
            URL u = buildUrl(resource, null);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            // Figure out the mimetype
            if (mimeType == null) {
                mimeType = "application/octet-stream";
            }

            headers.put("Content-Type", mimeType);
            headers.put("x-emc-uid", uid);
            headers.put("x-emc-pool", keyPool);

            // Process metadata
            if (metadata != null) {
                processMetadata(metadata, headers);
            }

            l4j.debug("meta " + headers.get("x-emc-meta"));

            // Add acl
            if (acl != null) {
                processAcl(acl, headers);
            }

            // Process data
            if (data == null) {
                data = new BufferSegment(new byte[0]);
            }

            // Add date
            headers.put("Date", getDateHeader());

            // Compute checksum
            if( checksum != null ) {
                checksum.update( data.getBuffer(), data.getOffset(), data.getSize() );
                headers.put( "x-emc-wschecksum", checksum.toString() );
            }

            // Sign request
            signRequest("POST", resource, null, headers);

            HttpResponse response = restPost( u, headers, data );


            // Check response
            handleError( response );

            // The new object ID is returned in the location response header
            String location = response.getFirstHeader("location").getValue();

            // Cleanup the connection
            cleanup( response );

            // Parse the value out of the URL
            return getObjectId( location );
        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    }

    @Override
    public void hardLink( ObjectPath source, ObjectPath target ) {
        try {
            String resource = getResourcePath( context, source );
            String query = "hardlink";
            URL u = buildUrl( resource, query );

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put( "x-emc-uid", uid );

            String destPath = target.toString();
            if ( destPath.startsWith( "/" ) ) {
                destPath = destPath.substring( 1 );
            }
            headers.put( "x-emc-path", destPath );

            // Add date
            headers.put( "Date", getDateHeader() );

            // Compute checksum
            // Sign request
            signRequest( "POST", resource, query, headers );

            HttpResponse response = restPost( u, headers, null );

            // Check response
            handleError( response );

            // Cleanup the connection
            cleanup( response );

        } catch ( MalformedURLException e ) {
            throw new EsuException( "Invalid URL", e );
        } catch ( IOException e ) {
            throw new EsuException( "Error connecting to server", e );
        } catch ( GeneralSecurityException e ) {
            throw new EsuException( "Error computing request signature", e );
        } catch ( URISyntaxException e ) {
            throw new EsuException( "Invalid URL", e );
        }
    }

    @Override
    public ObjectId createObjectWithKeyFromStream( String keyPool, String key, Acl acl, MetadataList metadata,
                                                   InputStream data, long length, String mimeType ) {
        try {
            String resource = context + "/namespace/" + key;
            URL u = buildUrl(resource, null);

            if (data == null) {
                throw new IllegalArgumentException("Input stream is required");
            }

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            // Figure out the mimetype
            if (mimeType == null) {
                mimeType = "application/octet-stream";
            }

            headers.put("Content-Type", mimeType);
            headers.put("x-emc-uid", uid);
            headers.put("x-emc-pool", keyPool);

            // Process metadata
            if (metadata != null) {
                processMetadata(metadata, headers);
            }

            l4j.debug("meta " + headers.get("x-emc-meta"));

            // Add acl
            if (acl != null) {
                processAcl(acl, headers);
            }

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("POST", resource, null, headers);

            HttpResponse response = restPost( u, headers, data, length );

            // Check response
            handleError( response );

            // The new object ID is returned in the location response header
            String location = response.getFirstHeader("location").getValue();

            // Cleanup the connection
            cleanup( response );

            // Parse the value out of the URL
            return getObjectId( location );
        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    }

    @Override
    public void deleteObjectWithKey( String keyPool, String key ) {
        try {
            String resource = context + "/namespace/" + key;
            URL u = buildUrl(resource, null);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);
            headers.put("x-emc-pool", keyPool);

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("DELETE", resource, null, headers);

            HttpResponse response = restDelete( u, headers );

            handleError( response );

            cleanup( response );

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    }

    @Override
    public ObjectMetadata getAllMetadata( String keyPool, String key ) {
        try {
            String resource = context + "/namespace/" + key;
            URL u = buildUrl(resource, null);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);
            headers.put("x-emc-pool", keyPool);

            if(unicodeEnabled) {
                headers.put("x-emc-utf8", "true");
            }

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("HEAD", resource, null, headers);

            HttpResponse response = restHead( u, headers );
            handleError( response );

            // Parse return headers. User grants are in x-emc-useracl and
            // group grants are in x-emc-groupacl
            Acl acl = new Acl();
            readAcl(acl, response.getFirstHeader("x-emc-useracl").getValue(),
                    Grantee.GRANT_TYPE.USER);
            readAcl(acl, response.getFirstHeader("x-emc-groupacl").getValue(),
                    Grantee.GRANT_TYPE.GROUP);

            // Parse return headers. Regular metadata is in x-emc-meta and
            // listable metadata is in x-emc-listable-meta
            MetadataList meta = new MetadataList();
            readMetadata(meta, response.getFirstHeader("x-emc-meta"), false);
            readMetadata(meta, response.getFirstHeader("x-emc-listable-meta"), true);

            ObjectMetadata om = new ObjectMetadata();
            om.setAcl(acl);
            om.setMetadata(meta);
            om.setMimeType(response.getFirstHeader( "Content-Type").getValue());

            finishRequest( response );

            return om;

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    }

    @Override
    public MetadataList getSystemMetadata( String keyPool, String key, MetadataTags tags ) {
        try {
            String resource = context + "/namespace/" + key;
            String query = "metadata/system";
            URL u = buildUrl(resource, query);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);
            headers.put("x-emc-pool", keyPool);

            // process tags
            if (tags != null) {
                processTags(tags, headers);
            }

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("GET", resource, query, headers);

            HttpResponse response = restGet( u, headers );
            handleError( response );

            // Parse return headers. Regular metadata is in x-emc-meta and
            // listable metadata is in x-emc-listable-meta
            MetadataList meta = new MetadataList();
            readMetadata(meta, response.getFirstHeader("x-emc-meta"), false);
            readMetadata(meta, response.getFirstHeader("x-emc-listable-meta"), true);

            finishRequest( response );
            return meta;

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    }

    @Override
    public byte[] readObjectWithKey( String keyPool, String key, Extent extent, byte[] buffer, Checksum checksum ) {
        try {
            String resource = context + "/namespace/" + key;
            URL u = buildUrl(resource, null);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);
            headers.put("x-emc-pool", keyPool);

            // Add date
            headers.put("Date", getDateHeader());

            // Add extent if needed
            if (extent != null && !extent.equals(Extent.ALL_CONTENT)) {
                headers.put(extent.getHeaderName(), extent.toString());
            }

            // Sign request
            signRequest("GET", resource, null, headers);
            HttpResponse response = restGet( u, headers );
            handleError( response );

            // The requested content is in the response body.
            byte[] data = readStream( response.getEntity().getContent(),
                                      (int) response.getEntity().getContentLength() );

            finishRequest( response );

            // See if a checksum was returned.
            Header checksumHeader = response.getFirstHeader("x-emc-wschecksum");

            if( checksumHeader != null && checksum != null ) {
                String checksumStr = checksumHeader.getValue();
                l4j.debug( "Checksum header: " + checksumStr );
                checksum.setExpectedValue( checksumStr );
                if( response.getEntity().getContentLength() != -1 ) {
                    checksum.update( data, 0, (int)response.getEntity().getContentLength() );
                } else {
                    checksum.update( data, 0, data.length );
                }
            }

            return data;

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    }

    @Override
    public InputStream readObjectStreamWithKey( String keyPool, String key, Extent extent ) {
        try {
            String resource = context + "/namespace/" + key;
            URL u = buildUrl(resource, null);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);
            headers.put("x-emc-pool", keyPool);

            // Add date
            headers.put("Date", getDateHeader());

            // Add extent if needed
            if (extent != null && !extent.equals(Extent.ALL_CONTENT)) {
                headers.put(extent.getHeaderName(), extent.toString());
            }

            // Sign request
            signRequest("GET", resource, null, headers);

            HttpResponse response = restGet( u, headers );
            handleError( response );

            return new CommonsInputStreamWrapper( response );

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    }

    @Override
    public void updateObjectWithKeyFromStream( String keyPool, String key, Acl acl, MetadataList metadata,
                                               Extent extent, InputStream data, long length, String mimeType ) {
        try {
            String resource = context + "/namespace/" + key;
            URL u = buildUrl(resource, null);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            // Figure out the mimetype
            if (mimeType == null) {
                mimeType = "application/octet-stream";
            }

            headers.put("Content-Type", mimeType);
            headers.put("x-emc-uid", uid);
            headers.put("x-emc-pool", keyPool);

            // Process metadata
            if (metadata != null) {
                processMetadata(metadata, headers);
            }

            l4j.debug("meta " + headers.get("x-emc-meta"));

            // Add acl
            if (acl != null) {
                processAcl(acl, headers);
            }

            // Add extent if needed
            if (extent != null && !extent.equals(Extent.ALL_CONTENT)) {
                headers.put(extent.getHeaderName(), extent.toString());
            }

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("PUT", resource, null, headers);

            HttpResponse response = restPut( u, headers, data, length );
            handleError( response );

            finishRequest( response );

        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    }

    @Override
    public void updateObjectWithKeyFromSegment( String keyPool, String key, Acl acl, MetadataList metadata,
                                                Extent extent, BufferSegment data, String mimeType,
                                                Checksum checksum ) {
        try {
            String resource = context + "/namespace/" + key;
            URL u = buildUrl(resource, null);

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            // Figure out the mimetype
            if (mimeType == null) {
                mimeType = "application/octet-stream";
            }

            headers.put("Content-Type", mimeType);
            headers.put("x-emc-uid", uid);
            headers.put("x-emc-pool", keyPool);

            // Process metadata
            if (metadata != null) {
                processMetadata(metadata, headers);
            }

            l4j.debug("meta " + headers.get("x-emc-meta"));

            // Add acl
            if (acl != null) {
                processAcl(acl, headers);
            }

            // Add extent if needed
            if (extent != null && !extent.equals(Extent.ALL_CONTENT)) {
                headers.put(extent.getHeaderName(), extent.toString());
            }

            // Process data
            if (data == null) {
                data = new BufferSegment(new byte[0]);
            }

            // Add date
            headers.put("Date", getDateHeader());

            // Compute checksum
            if( checksum != null ) {
                checksum.update( data.getBuffer(), data.getOffset(), data.getSize() );
                headers.put( "x-emc-wschecksum", checksum.toString() );
            }

            // Sign request
            signRequest("PUT", resource, null, headers);

            HttpResponse response = restPut( u, headers, data );
            handleError( response );

            finishRequest( response );
        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (GeneralSecurityException e) {
            throw new EsuException("Error computing request signature", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
    }
}
