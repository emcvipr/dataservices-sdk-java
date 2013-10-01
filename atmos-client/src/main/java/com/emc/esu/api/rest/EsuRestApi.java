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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.*;
import java.security.GeneralSecurityException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jdom.Document;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import com.emc.esu.api.Acl;
import com.emc.esu.api.BufferSegment;
import com.emc.esu.api.Checksum;
import com.emc.esu.api.DirectoryEntry;
import com.emc.esu.api.EsuException;
import com.emc.esu.api.Extent;
import com.emc.esu.api.Grantee;
import com.emc.esu.api.HttpInputStreamWrapper;
import com.emc.esu.api.Identifier;
import com.emc.esu.api.ListOptions;
import com.emc.esu.api.MetadataList;
import com.emc.esu.api.MetadataTag;
import com.emc.esu.api.MetadataTags;
import com.emc.esu.api.ObjectId;
import com.emc.esu.api.ObjectInfo;
import com.emc.esu.api.ObjectMetadata;
import com.emc.esu.api.ObjectPath;
import com.emc.esu.api.ObjectResult;
import com.emc.esu.api.ServiceInformation;
import com.emc.esu.api.Version;

/**
 * Implements the REST version of the ESU API. This class uses HttpUrlRequest to
 * perform object and metadata calls against the ESU server. All of the methods
 * that communicate with the server are atomic and stateless so the object can
 * be used safely in a multithreaded environment.
 * 
 * NOTE: When running on MacOS, it is highly recommended to set the platform
 * encoding to ISO-8859-1 instead of the default MacRoman.  Java's internal
 * HTTP layer does not translate characters properly and user metadata
 * containing extended characters may not be handled properly.  You can set
 * the encoding by passing the argument "-Dfile.encoding=ISO-8859-1" to the
 * VM on startup.
 */
public class EsuRestApi extends AbstractEsuRestApi {
    private static final Logger l4j = Logger.getLogger(EsuRestApi.class);

    private Map<String, String> customHeaders;

    /**
     * Creates a new EsuRestApi object.
     * 
     * @param host the hostname or IP address of the ESU server
     * @param port the port on the server to communicate with. Generally this is
     *            80 for HTTP and 443 for HTTPS.
     * @param uid the username to use when connecting to the server
     * @param sharedSecret the Base64 encoded shared secret to use to sign
     *            requests to the server.
     */
    public EsuRestApi(String host, int port, String uid, String sharedSecret) {
        super(host, port, uid, sharedSecret);
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
    		HttpURLConnection con = (HttpURLConnection) u.openConnection();

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

    		con.setFixedLengthStreamingMode((int) length);
    		con.setDoOutput(true);

    		// Add date
    		headers.put("Date", getDateHeader());

    		// Sign request
    		signRequest("POST", resource, null, headers);
    		configureRequest( con, "POST", headers );

    		con.connect();

    		// post data
    		OutputStream out = null;
    		byte[] buffer = new byte[128 * 1024];
    		int read = 0;
    		try {
    			out = con.getOutputStream();
    			while (read < length) {
                    // make sure we don't write past the content-length
                    int maxRead = (int) Math.min( (long) buffer.length, length - read );
                    int c = data.read( buffer, 0, maxRead );
                    if (c == -1) {
    					throw new EsuException(
    							"EOF encountered reading data stream");
    				}
    				out.write(buffer, 0, c);
    				read += c;
    			}
    			out.close();
    		} catch (IOException e) {
    			silentClose(out);
    			con.disconnect();
    			throw new EsuException("Error posting data", e);
    		}

    		// Check response
    		if (con.getResponseCode() > 299) {
    			handleError(con);
    		}

    		// The new object ID is returned in the location response header
    		String location = con.getHeaderField("location");
    		con.disconnect();

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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

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

            con.setFixedLengthStreamingMode((int) length);
            con.setDoOutput(true);

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("POST", resource, null, headers);
            configureRequest( con, "POST", headers );

            con.connect();

            // post data
            OutputStream out = null;
            byte[] buffer = new byte[128 * 1024];
            int read = 0;
            try {
                out = con.getOutputStream();
                while (read < length) {
                    // make sure we don't write past the content-length
                    int maxRead = (int) Math.min( (long) buffer.length, length - read );
                    int c = data.read( buffer, 0, maxRead );
                    if (c == -1) {
                        throw new EsuException(
                                "EOF encountered reading data stream");
                    }
                    out.write(buffer, 0, c);
                    read += c;
                }
                out.close();
            } catch (IOException e) {
                silentClose(out);
                con.disconnect();
                throw new EsuException("Error posting data", e);
            }

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }

            // The new object ID is returned in the location response header
            String location = con.getHeaderField("location");
            con.disconnect();

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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

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
            con.setFixedLengthStreamingMode(data.getSize());
            con.setDoOutput(true);

            // Add date
            headers.put("Date", getDateHeader());
            
            // Compute checksum
            if( checksum != null ) {
            	checksum.update( data.getBuffer(), data.getOffset(), data.getSize() );
            	headers.put( "x-emc-wschecksum", checksum.toString() );
            }

            // Sign request
            signRequest("POST", resource, null, headers);
            configureRequest( con, "POST", headers );

            con.connect();

            // post data
            OutputStream out = null;
            try {
                out = con.getOutputStream();
                out.write(data.getBuffer(), data.getOffset(), data.getSize());
                out.close();
            } catch (IOException e) {
                silentClose(out);
                con.disconnect();
                throw new EsuException("Error posting data", e);
            }

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }

            // The new object ID is returned in the location response header
            String location = con.getHeaderField("location");
            con.disconnect();

            // Parse the value out of the URL
            return getObjectId(location);
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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

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
            con.setFixedLengthStreamingMode(data.getSize());
            con.setDoOutput(true);

            // Add date
            headers.put("Date", getDateHeader());

            // Compute checksum
            if( checksum != null ) {
            	checksum.update( data.getBuffer(), data.getOffset(), data.getSize() );
            	headers.put( "x-emc-wschecksum", checksum.toString() );
            }

            // Sign request
            signRequest("POST", resource, null, headers);
            configureRequest( con, "POST", headers );

            con.connect();

            // post data
            OutputStream out = null;
            try {
                out = con.getOutputStream();
                out.write(data.getBuffer(), data.getOffset(), data.getSize());
                out.close();
            } catch (IOException e) {
                silentClose(out);
                con.disconnect();
                throw new EsuException("Error posting data", e);
            }

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }

            // The new object ID is returned in the location response header
            String location = con.getHeaderField("location");
            con.disconnect();

            // Parse the value out of the URL
            return getObjectId(location);

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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("DELETE", resource, null, headers);
            configureRequest( con, "DELETE", headers );

            con.connect();

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }
            con.disconnect();

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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("DELETE", resource, query, headers);
            configureRequest( con, "DELETE", headers );

            con.connect();

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }
            con.disconnect();

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
            URL u = buildUrl(resource, query);
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

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
            configureRequest( con, "DELETE", headers );

            con.connect();

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }
            con.disconnect();

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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("GET", resource, query, headers);
            configureRequest( con, "GET", headers );

            con.connect();

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }

            // Parse return headers. User grants are in x-emc-useracl and
            // group grants are in x-emc-groupacl
            Acl acl = new Acl();
            readAcl(acl, con.getHeaderField("x-emc-useracl"),
                    Grantee.GRANT_TYPE.USER);
            readAcl(acl, con.getHeaderField("x-emc-groupacl"),
                    Grantee.GRANT_TYPE.GROUP);

            con.disconnect();
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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

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
            configureRequest( con, "GET", headers );

            con.connect();

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }

            String header = con.getHeaderField("x-emc-listable-tags");
            l4j.debug("x-emc-listable-tags: " + header);
            MetadataTags tags = new MetadataTags();
            readTags(tags, header, true);

            con.disconnect();
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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

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
            configureRequest( con, "GET", headers );

            con.connect();

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }

            // Parse return headers. Regular metadata is in x-emc-meta and
            // listable metadata is in x-emc-listable-meta
            MetadataList meta = new MetadataList();
            readMetadata(meta, con.getHeaderField("x-emc-meta"), false);
            readMetadata(meta, con.getHeaderField("x-emc-listable-meta"), true);

            con.disconnect();
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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();
            
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
            configureRequest( con, "GET", headers );

            con.connect();

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }

            // Parse return headers. Regular metadata is in x-emc-meta and
            // listable metadata is in x-emc-listable-meta
            MetadataList meta = new MetadataList();
            readMetadata(meta, con.getHeaderField("x-emc-meta"), false);
            readMetadata(meta, con.getHeaderField("x-emc-listable-meta"), true);

            con.disconnect();
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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

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
            configureRequest( con, "GET", headers );

            con.connect();

            // Check response
            try {
	            if (con.getResponseCode() > 299) {
	                handleError(con);
	            }
            } catch( EsuException e ) {
            	if( e.getAtmosCode() == 1003 ) {
            		return Collections.emptyList();
            	}
            	throw e;
            }

            // Get object id list from response
            byte[] response = readResponse(con, null);

            l4j.debug("Response: " + new String(response, "UTF-8"));
            con.disconnect();
            
            if( options != null ) {
            	// Update the token for listing more results.  If there are no
            	// more results, the header will not be set and the token will
            	// be cleared in the options object.
            	options.setToken( con.getHeaderField("x-emc-token") );
            } else {
            	if( con.getHeaderField( "x-emc-token" ) != null ) {
            		l4j.warn( "Result set truncated. Use ListOptions to " +
            				"retrieve token for next page of results." );
            	}
            }

            return parseObjectListWithMetadata(response);

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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

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
            configureRequest( con, "GET", headers );

            con.connect();

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }

            // Get the user metadata tags out of x-emc-listable-tags and
            // x-emc-tags
            MetadataTags tags = new MetadataTags();

            readTags(tags, con.getHeaderField("x-emc-listable-tags"), true);
            readTags(tags, con.getHeaderField("x-emc-tags"), false);

            con.disconnect();
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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("GET", resource, query, headers);
            configureRequest( con, "GET", headers );

            con.connect();

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }

            // Get object id list from response
            byte[] response = readResponse(con, null);

            l4j.debug("Response: " + new String(response, "UTF-8"));

            con.disconnect();
            return parseVersionList(response);

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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            // Add date
            headers.put("Date", getDateHeader());

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

            // Sign request
            signRequest("GET", resource, query, headers);
            configureRequest( con, "GET", headers );

            con.connect();

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }

            if( options != null ) {
            	// Update the token for listing more results.  If there are no
            	// more results, the header will not be set and the token will
            	// be cleared in the options object.
            	options.setToken( con.getHeaderField("x-emc-token") );
            } else {
            	if( con.getHeaderField( "x-emc-token" ) != null ) {
            		l4j.warn( "Result set truncated. Use ListOptions to " +
            				"retrieve token for next page of results." );
            	}
            }

            // Get object id list from response
            byte[] response = readResponse(con, null);

            l4j.debug("Response: " + new String(response, "UTF-8"));

            con.disconnect();
            return parseVersionListLong(response);

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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

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
            configureRequest( con, "GET", headers );

            con.connect();

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }

            // Get object id list from response
            Map<String,List<String>> responseHeaders = con.getHeaderFields();
            l4j.debug("Response headers: " + responseHeaders);
            byte[] response = readResponse(con, null);

            if(l4j.isDebugEnabled()) {
            	String responseBody = new String(response, "UTF-8");
            	l4j.debug("Response: " + responseBody);
            }

            con.disconnect();
            return parseObjectList(response);

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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

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
            configureRequest( con, "GET", headers );

            con.connect();

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }
            
            if(buffer != null && extent != null) {
            	if(extent.getSize() > (long)buffer.length) {
            		throw new IllegalArgumentException(
            				"The buffer is smaller than the requested extent");
            	}
            }

            // The requested content is in the response body.
            byte[] data = readResponse(con, buffer);
                       
            // See if a checksum was returned.
            String checksumStr = con.getHeaderField("x-emc-wschecksum");
            if( checksumStr != null && checksum != null ) {
            	l4j.debug( "Checksum header: " + checksumStr );
            	checksum.setExpectedValue( checksumStr );
            	if( con.getContentLength() != -1 ) {
            		checksum.update( data, 0, con.getContentLength() );
            	} else {
                    // readResponse should return a new content-sized buffer in this case
            		checksum.update( data, 0, data.length );
            	}
            }
            
            con.disconnect();
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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

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
            configureRequest( con, "GET", headers );

            con.connect();

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }

            return new HttpInputStreamWrapper(con.getInputStream(), con);

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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

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
            con.setFixedLengthStreamingMode(data.getSize());
            con.setDoOutput(true);

            // Add date
            headers.put("Date", getDateHeader());

            // Compute checksum
            if( checksum != null ) {
            	checksum.update( data.getBuffer(), data.getOffset(), data.getSize() );
            	headers.put( "x-emc-wschecksum", checksum.toString() );
            }

            // Sign request
            signRequest("PUT", resource, null, headers);
            configureRequest( con, "PUT", headers );

            con.connect();

            // post data
            OutputStream out = null;
            try {
                out = con.getOutputStream();
                out.write(data.getBuffer(), data.getOffset(), data.getSize());
                out.close();
            } catch (IOException e) {
                silentClose(out);
                con.disconnect();
                throw new EsuException("Error posting data", e);
            }

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }
            con.disconnect();
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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

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

            con.setFixedLengthStreamingMode((int) length);
            con.setDoOutput(true);

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("PUT", resource, null, headers);
            configureRequest( con, "PUT", headers );

            con.connect();

            // post data
            OutputStream out = null;
            byte[] buffer = new byte[128 * 1024];
            int read = 0;
            try {
                out = con.getOutputStream();
                while (read < length) {
                    // make sure we don't write past the content-length
                    int maxRead = (int) Math.min( (long) buffer.length, length - read );
                    int c = data.read( buffer, 0, maxRead );
                    if (c == -1) {
                        throw new EsuException(
                                "EOF encountered reading data stream");
                    }
                    out.write(buffer, 0, c);
                    read += c;
                }
                out.close();
            } catch (IOException e) {
                silentClose(out);
                con.disconnect();
                throw new EsuException("Error posting data", e);
            }

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }
            con.disconnect();
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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

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
            configureRequest( con, "POST", headers );

            con.connect();

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }

            // Read the response to complete the request (will be empty)
            InputStream in = con.getInputStream();
            in.close();
            con.disconnect();
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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

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
            configureRequest( con, "POST", headers );

            con.connect();

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }

            // Read the response to complete the request (will be empty)
            InputStream in = con.getInputStream();
            in.close();
            con.disconnect();
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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("POST", resource, query, headers);
            configureRequest( con, "POST", headers );

            con.connect();

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }

            // The new object ID is returned in the location response header
            String location = con.getHeaderField("location");

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
			ListOptions options ) {
    	
        if (!path.isDirectory()) {
            throw new EsuException(
                    "listDirectory must be called with a directory path");
        }
        
        byte[] data = null;

        // Read out the directory's contents
        try {
            String resource = getResourcePath(context, path);
            URL u = buildUrl(resource, null);
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);
            
            if(unicodeEnabled) {
                headers.put("x-emc-utf8", "true");
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
            configureRequest( con, "GET", headers );

            con.connect();

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }
            
            if( options != null ) {
            	// Update the token for listing more results.  If there are no
            	// more results, the header will not be set and the token will
            	// be cleared in the options object.
            	options.setToken( con.getHeaderField("x-emc-token") );
            } else {
            	if( con.getHeaderField( "x-emc-token" ) != null ) {
            		l4j.warn( "Result set truncated. Use ListOptions to " +
            				"retrieve token for next page of results." );
            	}
            }

            // The requested content is in the response body.
            data = readResponse(con, null);
                       
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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

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
            configureRequest( con, "HEAD", headers );

            con.connect();

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }

            // Parse return headers. User grants are in x-emc-useracl and
            // group grants are in x-emc-groupacl
            Acl acl = new Acl();
            readAcl(acl, con.getHeaderField("x-emc-useracl"),
                    Grantee.GRANT_TYPE.USER);
            readAcl(acl, con.getHeaderField("x-emc-groupacl"),
                    Grantee.GRANT_TYPE.GROUP);

            // Parse return headers. Regular metadata is in x-emc-meta and
            // listable metadata is in x-emc-listable-meta
            MetadataList meta = new MetadataList();
            readMetadata(meta, con.getHeaderField("x-emc-meta"), false);
            readMetadata(meta, con.getHeaderField("x-emc-listable-meta"), true);

            ObjectMetadata om = new ObjectMetadata();
            om.setAcl(acl);
            om.setMetadata(meta);
            om.setMimeType(con.getContentType());

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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("GET", resource, null, headers);
            configureRequest( con, "GET", headers );

            con.connect();

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }

            // Get object id list from response
            byte[] response = readResponse(con, null);

            l4j.debug("Response: " + new String(response, "UTF-8"));
            con.disconnect();
            
            return parseServiceInformation(response, con.getHeaderFields());

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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

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
            
            // Compute checksum
            // Sign request
            signRequest("POST", resource, query, headers);
            configureRequest( con, "POST", headers );

            con.connect();

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }

            con.disconnect();

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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);
            
            // Version to promote
            headers.put("x-emc-version-oid", vId.toString());

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("PUT", resource, query, headers);
            configureRequest( con, "PUT", headers );

            con.connect();

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }
            con.disconnect();
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
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("x-emc-uid", uid);

            // Add date
            headers.put("Date", getDateHeader());

            // Sign request
            signRequest("GET", resource, query, headers);
            configureRequest( con, "GET", headers );

            con.connect();

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }

            // Get object id list from response
            byte[] response = readResponse(con, null);
            String responseXml = new String(response, "UTF-8"); 
            
            l4j.debug("Response: " + responseXml );

            con.disconnect();
            return new ObjectInfo( responseXml );

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
            String resource = context + "/";
            URL u = buildUrl(resource, null);
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

            con.connect();

            // Check response
            Date serverDate = new Date(con.getHeaderFieldDate("Date", 0));
        	if(serverDate.getTime() == 0) {
        		EsuException e = new EsuException(
        				"Unable to get date from server request: " + 
        						con.getResponseMessage(), 
        						con.getResponseCode());
        		throw(e);
        	}
        	
        	return System.currentTimeMillis()-serverDate.getTime();
        } catch (MalformedURLException e) {
            throw new EsuException("Invalid URL", e);
        } catch (IOException e) {
            throw new EsuException("Error connecting to server", e);
        } catch (URISyntaxException e) {
            throw new EsuException("Invalid URL", e);
        }
	}





    // ///////////////////
    // Private Methods //
    // ///////////////////


    protected void configureRequest( HttpURLConnection con, String method, Map<String, String> headers )
            throws ProtocolException, UnsupportedEncodingException {

        // add any custom headers (i.e. for authentication proxy)
        if ( getCustomHeaders() != null ) headers.putAll( getCustomHeaders() );

        // Can set all the headers, etc now.
        for ( String name : headers.keySet() ) {
            con.setRequestProperty( name, headers.get(name) );
        }

        // Set the method.
        con.setRequestMethod( method );
    }

    /**
     * Generates the HMAC-SHA1 signature used to authenticate the request using
     * the Java security APIs.
     * 
     * @param method the HTTP method used
     * @param path the resource path
     * @param query the URL querystring (if present)
     * @param headers the HTTP headers for the request
     * @throws IOException if character data cannot be encoded.
     * @throws GeneralSecurityException If errors occur generating the HMAC-SHA1
     *             signature.
     */
    protected void signRequest(String method, String path, String query, Map<String, String> headers) throws IOException,
            GeneralSecurityException {
        // Build the string to hash.

    	ByteArrayOutputStream out = new ByteArrayOutputStream();
    	
        appendBytes( out, method + "\n", null );

        // If content type exists, add it. Otherwise add a blank line.
        if (headers.containsKey("Content-Type")) {
            l4j.debug("Content-Type: " + headers.get("Content-Type"));
            appendBytes( out, headers.get("Content-Type") + "\n", null );
        } else {
            appendBytes( out, "\n", null );
        }

        // If the range header exists, add it. Otherwise add a blank line.
        if (headers.containsKey("Range")) {
            appendBytes( out, headers.get("Range") + "\n", null);
        } else if (headers.containsKey("Content-Range")) {
            appendBytes( out, headers.get("Content-Range") + "\n", null );
        } else {
            appendBytes( out, "\n", null );
        }

        // Add the current date and the resource.
        appendBytes( out, headers.get("Date") + "\n"
                + path.toLowerCase(), "UTF-8" );
        if (query != null) {
            appendBytes( out,"?" + query + "\n", "UTF-8" );
        } else {
            appendBytes( out, "\n", null );
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
                appendBytes( out, "\n", null );
            } else {
                first = false;
            }
            // this.trace( "xheader: " . k . "." . newheaders[k] );
            appendBytes( out, key + ':' + normalizeSpace(newheaders.get(key)), null );
        }

        
        byte[] data = out.toByteArray();
        
        l4j.debug("Hashing:\n" + new String(data, "UTF-8"));
        
        String hashOut = sign( data );
        
        headers.put( "x-emc-signature", hashOut );

    }

    private void appendBytes(OutputStream out, String s,
			String encoding) throws UnsupportedEncodingException, IOException {
		if( encoding == null ) {
			out.write( s.getBytes() );
		} else {
			out.write( s.getBytes( encoding ) );
		}
	}

	/**
     * Attempts to generate a reasonable error message from from a request. If
     * the error is from the web service, there should be a message and code in
     * the response body encapsulated in XML.
     * 
     * @param con the connection from the failed request.
     */
    protected void handleError(HttpURLConnection con) {
        int http_code = 0;
        // Try and read the response body.
        try {
            http_code = con.getResponseCode();
            byte[] response = readResponse(con, null);
            l4j.debug("Error response: " + new String(response, "UTF-8"));
            SAXBuilder sb = new SAXBuilder();

            Document d = sb.build(new ByteArrayInputStream(response));

            String code = d.getRootElement().getChildText("Code");
            String message = d.getRootElement().getChildText("Message");

            if (code == null && message == null) {
                // not an error from ESU
                throw new EsuException(con.getResponseMessage(), http_code);
            }

            l4j.debug("Error: " + code + " message: " + message);
            throw new EsuException(message, http_code, Integer.parseInt(code));

        } catch (IOException e) {
            l4j.debug("Could not read error response body", e);
            // Just throw what we know from the response
            try {
                throw new EsuException(con.getResponseMessage(), http_code);
            } catch (IOException e1) {
                l4j.warn("Could not get response code/message!", e);
                throw new EsuException("Could not get response code", e,
                        http_code);
            }
        } catch (JDOMException e) {
            try {
                l4j.debug("Could not parse response body for " + http_code
                        + ": " + con.getResponseMessage(), e);
                throw new EsuException("Could not parse response body for "
                        + http_code + ": " + con.getResponseMessage(), e,
                        http_code);
            } catch (IOException e1) {
                throw new EsuException("Could not parse response body", e1,
                        http_code);
            }

        }

    }

    /**
     * Reads the response body and returns it in a byte array.
     * 
     * @param con the HTTP connection
     * @param buffer The buffer to use to read the response. The response buffer
     *            must be large enough to read the entire response or an error
     *            will be thrown.
     * @return the byte array containing the response body. Note that if you
     *         pass in a buffer, this will return the same buffer object unless
     *         the content length is indeterminate. Be sure to check the content
     *         length to know what data in the buffer is valid
     *         (from zero to contentLength). If the content length is
     *         indeterminate a new buffer will be returned that is the exact size
     *         of the response data.
     * @throws IOException if reading the response stream fails.
     */
    protected byte[] readResponse(HttpURLConnection con, byte[] buffer)
            throws IOException {
        InputStream in = null;
        if (con.getResponseCode() > 299) {
            in = con.getErrorStream();
            if (in == null) {
                in = con.getInputStream();
            }
        } else {
            in = con.getInputStream();
        }
        if (in == null) {
            // could not get stream
            return new byte[0];
        }
        try {
            byte[] output;
            int contentLength = con.getContentLength();
            // If we know the content length, read it directly into a buffer.
            if (contentLength != -1) {
                if (buffer != null && buffer.length < con.getContentLength()) {
                    throw new EsuException(
                            "The response buffer was not long enough to hold the response: "
                                    + buffer.length + "<"
                                    + con.getContentLength());
                }
                if (buffer != null) {
                    output = buffer;
                } else {
                    output = new byte[con.getContentLength()];
                }

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
                // Content length is indeterminate.
                l4j.debug("Content length is unknown.  Buffering output.");
                // use a ByteArrayOutputStream to collect the response.
                if (buffer == null) {
                    buffer = new byte[4096];
                }
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

    // Getters/Setters
    public Map<String, String> getCustomHeaders() {
        return customHeaders;
    }

    public void setCustomHeaders(Map<String, String> customHeaders) {
        this.customHeaders = customHeaders;
    }

    //---------- Features supported by the Atmos 2.0 REST API. ----------\\

    @Override
    public void hardLink( ObjectPath source, ObjectPath target ) {
        try {
            String resource = getResourcePath( context, source );
            String query = "hardlink";
            URL u = buildUrl( resource, query );
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

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
            configureRequest( con, "POST", headers );

            con.connect();

            // Check response
            if ( con.getResponseCode() > 299 ) {
                handleError( con );
            }

            con.disconnect();

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
    public ObjectId createObjectWithKeyFromSegment( String keyPool, String key, Acl acl, MetadataList metadata,
                                                    BufferSegment data, String mimeType, Checksum checksum ) {
        try {
            String resource = context + "/namespace/" + key;
            URL u = buildUrl( resource, null );
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            // Figure out the mimetype
            if ( mimeType == null ) {
                mimeType = "application/octet-stream";
            }

            headers.put( "Content-Type", mimeType );
            headers.put( "x-emc-uid", uid );
            headers.put( "x-emc-pool", keyPool );

            // Process metadata
            if ( metadata != null ) {
                processMetadata( metadata, headers );
            }

            l4j.debug( "meta " + headers.get( "x-emc-meta" ) );

            // Add acl
            if ( acl != null ) {
                processAcl( acl, headers );
            }

            // Process data
            if ( data == null ) {
                data = new BufferSegment( new byte[0] );
            }
            con.setFixedLengthStreamingMode( data.getSize() );
            con.setDoOutput( true );

            // Add date
            headers.put( "Date", getDateHeader() );

            // Compute checksum
            if ( checksum != null ) {
                checksum.update( data.getBuffer(), data.getOffset(), data.getSize() );
                headers.put( "x-emc-wschecksum", checksum.toString() );
            }

            // Sign request
            signRequest( "POST", resource, null, headers );
            configureRequest( con, "POST", headers );

            con.connect();

            // post data
            OutputStream out = null;
            try {
                out = con.getOutputStream();
                out.write( data.getBuffer(), data.getOffset(), data.getSize() );
                out.close();
            } catch ( IOException e ) {
                silentClose( out );
                con.disconnect();
                throw new EsuException( "Error posting data", e );
            }

            // Check response
            if ( con.getResponseCode() > 299 ) {
                handleError( con );
            }

            // The new object ID is returned in the location response header
            String location = con.getHeaderField( "location" );
            con.disconnect();

            // Parse the value out of the URL
            return getObjectId( location );
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
            URL u = buildUrl( resource, null );
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

            if ( data == null ) {
                throw new IllegalArgumentException( "Input stream is required" );
            }

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            // Figure out the mimetype
            if ( mimeType == null ) {
                mimeType = "application/octet-stream";
            }

            headers.put( "Content-Type", mimeType );
            headers.put( "x-emc-uid", uid );
            headers.put( "x-emc-pool", keyPool );

            // Process metadata
            if ( metadata != null ) {
                processMetadata( metadata, headers );
            }

            l4j.debug( "meta " + headers.get( "x-emc-meta" ) );

            // Add acl
            if ( acl != null ) {
                processAcl( acl, headers );
            }

            con.setFixedLengthStreamingMode( (int) length );
            con.setDoOutput( true );

            // Add date
            headers.put( "Date", getDateHeader() );

            // Sign request
            signRequest( "POST", resource, null, headers );
            configureRequest( con, "POST", headers );

            con.connect();

            // post data
            OutputStream out = null;
            byte[] buffer = new byte[128 * 1024];
            int read = 0;
            try {
                out = con.getOutputStream();
                while ( read < length ) {
                    // make sure we don't write past the content-length
                    int maxRead = (int) Math.min( (long) buffer.length, length - read );
                    int c = data.read( buffer, 0, maxRead );
                    if ( c == -1 ) {
                        throw new EsuException(
                                "EOF encountered reading data stream" );
                    }
                    out.write( buffer, 0, c );
                    read += c;
                }
                out.close();
            } catch ( IOException e ) {
                silentClose( out );
                con.disconnect();
                throw new EsuException( "Error posting data", e );
            }

            // Check response
            if ( con.getResponseCode() > 299 ) {
                handleError( con );
            }

            // The new object ID is returned in the location response header
            String location = con.getHeaderField( "location" );
            con.disconnect();

            // Parse the value out of the URL
            return getObjectId( location );
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
    public void deleteObjectWithKey( String keyPool, String key ) {
        try {
            String resource = context + "/namespace/" + key;
            URL u = buildUrl( resource, null );
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put( "x-emc-uid", uid );
            headers.put( "x-emc-pool", keyPool );

            // Add date
            headers.put( "Date", getDateHeader() );

            // Sign request
            signRequest( "DELETE", resource, null, headers );
            configureRequest( con, "DELETE", headers );

            con.connect();

            // Check response
            if ( con.getResponseCode() > 299 ) {
                handleError( con );
            }
            con.disconnect();

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
    public ObjectMetadata getAllMetadata( String keyPool, String key ) {
        try {
            String resource = context + "/namespace/" + key;
            URL u = buildUrl( resource, null );
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put( "x-emc-uid", uid );
            headers.put( "x-emc-pool", keyPool );

            if ( unicodeEnabled ) {
                headers.put( "x-emc-utf8", "true" );
            }

            // Add date
            headers.put( "Date", getDateHeader() );

            // Sign request
            signRequest( "HEAD", resource, null, headers );
            configureRequest( con, "HEAD", headers );

            con.connect();

            // Check response
            if ( con.getResponseCode() > 299 ) {
                handleError( con );
            }

            // Parse return headers. User grants are in x-emc-useracl and
            // group grants are in x-emc-groupacl
            Acl acl = new Acl();
            readAcl( acl, con.getHeaderField( "x-emc-useracl" ),
                     Grantee.GRANT_TYPE.USER );
            readAcl( acl, con.getHeaderField( "x-emc-groupacl" ),
                     Grantee.GRANT_TYPE.GROUP );

            // Parse return headers. Regular metadata is in x-emc-meta and
            // listable metadata is in x-emc-listable-meta
            MetadataList meta = new MetadataList();
            readMetadata( meta, con.getHeaderField( "x-emc-meta" ), false );
            readMetadata( meta, con.getHeaderField( "x-emc-listable-meta" ), true );

            ObjectMetadata om = new ObjectMetadata();
            om.setAcl( acl );
            om.setMetadata( meta );
            om.setMimeType( con.getContentType() );

            return om;

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
    public MetadataList getSystemMetadata( String keyPool, String key, MetadataTags tags ) {
        try {
            String resource = context + "/namespace/" + key;
            String query = "metadata/system";
            URL u = buildUrl( resource, query );
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put( "x-emc-uid", uid );
            headers.put( "x-emc-pool", keyPool );

            // process tags
            if ( tags != null ) {
                processTags( tags, headers );
            }

            // Add date
            headers.put( "Date", getDateHeader() );

            // Sign request
            signRequest( "GET", resource, query, headers );
            configureRequest( con, "GET", headers );

            con.connect();

            // Check response
            if ( con.getResponseCode() > 299 ) {
                handleError( con );
            }

            // Parse return headers. Regular metadata is in x-emc-meta and
            // listable metadata is in x-emc-listable-meta
            MetadataList meta = new MetadataList();
            readMetadata( meta, con.getHeaderField( "x-emc-meta" ), false );
            readMetadata( meta, con.getHeaderField( "x-emc-listable-meta" ), true );

            con.disconnect();
            return meta;

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
    public byte[] readObjectWithKey( String keyPool, String key, Extent extent, byte[] buffer, Checksum checksum ) {
        try {
            String resource = context + "/namespace/" + key;
            URL u = buildUrl( resource, null );
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put( "x-emc-uid", uid );
            headers.put( "x-emc-pool", keyPool );

            // Add date
            headers.put( "Date", getDateHeader() );

            // Add extent if needed
            if ( extent != null && !extent.equals( Extent.ALL_CONTENT ) ) {
                headers.put( extent.getHeaderName(), extent.toString() );
            }

            // Sign request
            signRequest( "GET", resource, null, headers );
            configureRequest( con, "GET", headers );

            con.connect();

            // Check response
            if ( con.getResponseCode() > 299 ) {
                handleError( con );
            }

            if ( buffer != null && extent != null ) {
                if ( extent.getSize() > (long) buffer.length ) {
                    throw new IllegalArgumentException(
                            "The buffer is smaller than the requested extent" );
                }
            }

            // The requested content is in the response body.
            byte[] data = readResponse( con, buffer );

            // See if a checksum was returned.
            String checksumStr = con.getHeaderField( "x-emc-wschecksum" );
            if ( checksumStr != null && checksum != null ) {
                l4j.debug( "Checksum header: " + checksumStr );
                checksum.setExpectedValue( checksumStr );
                if ( con.getContentLength() != -1 ) {
                    checksum.update( data, 0, con.getContentLength() );
                } else {
                    // readResponse should return a new content-sized buffer in this case
                    checksum.update( data, 0, data.length );
                }
            }

            con.disconnect();
            return data;

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
    public InputStream readObjectStreamWithKey( String keyPool, String key, Extent extent ) {
        try {
            String resource = context + "/namespace/" + key;
            URL u = buildUrl( resource, null );
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            headers.put( "x-emc-uid", uid );
            headers.put( "x-emc-pool", keyPool );

            // Add date
            headers.put( "Date", getDateHeader() );

            // Add extent if needed
            if ( extent != null && !extent.equals( Extent.ALL_CONTENT ) ) {
                headers.put( extent.getHeaderName(), extent.toString() );
            }

            // Sign request
            signRequest( "GET", resource, null, headers );
            configureRequest( con, "GET", headers );

            con.connect();

            // Check response
            if ( con.getResponseCode() > 299 ) {
                handleError( con );
            }

            return new HttpInputStreamWrapper( con.getInputStream(), con );

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
    public void updateObjectWithKeyFromStream( String keyPool, String key, Acl acl, MetadataList metadata,
                                               Extent extent, InputStream data, long length, String mimeType ) {
        try {
            String resource = context + "/namespace/" + key;
            URL u = buildUrl( resource, null );
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            // Figure out the mimetype
            if ( mimeType == null ) {
                mimeType = "application/octet-stream";
            }

            headers.put( "Content-Type", mimeType );
            headers.put( "x-emc-uid", uid );
            headers.put( "x-emc-pool", keyPool );

            // Process metadata
            if ( metadata != null ) {
                processMetadata( metadata, headers );
            }

            l4j.debug( "meta " + headers.get( "x-emc-meta" ) );

            // Add acl
            if ( acl != null ) {
                processAcl( acl, headers );
            }

            // Add extent if needed
            if ( extent != null && !extent.equals( Extent.ALL_CONTENT ) ) {
                headers.put( extent.getHeaderName(), extent.toString() );
            }

            con.setFixedLengthStreamingMode( (int) length );
            con.setDoOutput( true );

            // Add date
            headers.put( "Date", getDateHeader() );

            // Sign request
            signRequest( "PUT", resource, null, headers );
            configureRequest( con, "PUT", headers );

            con.connect();

            // post data
            OutputStream out = null;
            byte[] buffer = new byte[128 * 1024];
            int read = 0;
            try {
                out = con.getOutputStream();
                while ( read < length ) {
                    // make sure we don't write past the content-length
                    int maxRead = (int) Math.min( (long) buffer.length, length - read );
                    int c = data.read( buffer, 0, maxRead );
                    if ( c == -1 ) {
                        throw new EsuException(
                                "EOF encountered reading data stream" );
                    }
                    out.write( buffer, 0, c );
                    read += c;
                }
                out.close();
            } catch ( IOException e ) {
                silentClose( out );
                con.disconnect();
                throw new EsuException( "Error posting data", e );
            }

            // Check response
            if ( con.getResponseCode() > 299 ) {
                handleError( con );
            }
            con.disconnect();
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
    public void updateObjectWithKeyFromSegment( String keyPool, String key, Acl acl, MetadataList metadata,
                                                Extent extent, BufferSegment data, String mimeType,
                                                Checksum checksum ) {
        try {
            String resource = context + "/namespace/" + key;
            URL u = buildUrl( resource, null );
            HttpURLConnection con = (HttpURLConnection) u.openConnection();

            // Build headers
            Map<String, String> headers = new HashMap<String, String>();

            // Figure out the mimetype
            if ( mimeType == null ) {
                mimeType = "application/octet-stream";
            }

            headers.put( "Content-Type", mimeType );
            headers.put( "x-emc-uid", uid );
            headers.put( "x-emc-pool", keyPool );

            // Process metadata
            if ( metadata != null ) {
                processMetadata( metadata, headers );
            }

            l4j.debug( "meta " + headers.get( "x-emc-meta" ) );

            // Add acl
            if ( acl != null ) {
                processAcl( acl, headers );
            }

            // Add extent if needed
            if ( extent != null && !extent.equals( Extent.ALL_CONTENT ) ) {
                headers.put( extent.getHeaderName(), extent.toString() );
            }

            // Process data
            if ( data == null ) {
                data = new BufferSegment( new byte[0] );
            }
            con.setFixedLengthStreamingMode( data.getSize() );
            con.setDoOutput( true );

            // Add date
            headers.put( "Date", getDateHeader() );

            // Compute checksum
            if ( checksum != null ) {
                checksum.update( data.getBuffer(), data.getOffset(), data.getSize() );
                headers.put( "x-emc-wschecksum", checksum.toString() );
            }

            // Sign request
            signRequest( "PUT", resource, null, headers );
            configureRequest( con, "PUT", headers );

            con.connect();

            // post data
            OutputStream out = null;
            try {
                out = con.getOutputStream();
                out.write( data.getBuffer(), data.getOffset(), data.getSize() );
                out.close();
            } catch ( IOException e ) {
                silentClose( out );
                con.disconnect();
                throw new EsuException( "Error posting data", e );
            }

            // Check response
            if ( con.getResponseCode() > 299 ) {
                handleError( con );
            }
            con.disconnect();
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
}
