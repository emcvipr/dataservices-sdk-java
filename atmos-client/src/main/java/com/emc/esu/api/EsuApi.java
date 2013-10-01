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
package com.emc.esu.api;

import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.List;

/**
 * This interface defines the basic operations available through the ESU web
 * services.
 * @deprecated Use {@link com.emc.atmos.api.AtmosApi} instead
 */
@Deprecated
public interface EsuApi {

    boolean isUnicodeEnabled();

    void setUnicodeEnabled(boolean unicodeEnabled);

    /**
     * Creates a new object in the cloud.
     * @param acl Access control list for the new object.  May be null
     * to use a default ACL
     * @param metadata Metadata for the new object.  May be null for
     * no metadata.
     * @param data The initial contents of the object.  May be appended
     * to later.  May be null to create an object with no content.
     * @param mimeType the MIME type of the content.  Optional,
     * may be null.  If data is non-null and mimeType is null, the MIME
     * type will default to application/octet-stream.
     * @return Identifier of the newly created object.
     * @throws EsuException if the request fails.
     */
    ObjectId createObject( Acl acl, MetadataList metadata,
            byte[] data, String mimeType );

    /**
     * Creates a new object in the cloud.
     * @param acl Access control list for the new object.  May be null
     * to use a default ACL
     * @param metadata Metadata for the new object.  May be null for
     * no metadata.
     * @param data The initial contents of the object.  May be appended
     * to later.  May be null to create an object with no content.
     * @param mimeType the MIME type of the content.  Optional,
     * may be null.  If data is non-null and mimeType is null, the MIME
     * type will default to application/octet-stream.
     * @param checksum if not null, use the Checksum object to compute
     * the checksum for the create object request.  If appending
     * to the object with subsequent requests, use the same
     * checksum object for each request.
     * @return Identifier of the newly created object.
     * @throws EsuException if the request fails.
     */
    ObjectId createObject( Acl acl, MetadataList metadata,
            byte[] data, String mimeType, Checksum checksum );

    /**
     * Creates a new object in the cloud.
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
    ObjectId createObjectFromStream( Acl acl, MetadataList metadata,
            InputStream data, long length, String mimeType );

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
    ObjectId createObjectFromStreamOnPath( ObjectPath path, Acl acl, MetadataList metadata,
            InputStream data, long length, String mimeType );

    /**
     * Creates a new object in the cloud on the specified path.
     * @param path The path to create the object on.
     * @param acl Access control list for the new object.  May be null
     * to use a default ACL
     * @param metadata Metadata for the new object.  May be null for
     * no metadata.
     * @param data The initial contents of the object.  May be appended
     * to later.  May be null to create an object with no content.
     * @param mimeType the MIME type of the content.  Optional,
     * may be null.  If data is non-null and mimeType is null, the MIME
     * type will default to application/octet-stream.
     * @return the ObjectId of the newly-created object for references by ID.
     * @throws EsuException if the request fails.
     */
    ObjectId createObjectOnPath( ObjectPath path, Acl acl,
    		MetadataList metadata,
            byte[] data, String mimeType );
    /**
     * Creates a new object in the cloud on the specified path.
     * @param path The path to create the object on.
     * @param acl Access control list for the new object.  May be null
     * to use a default ACL
     * @param metadata Metadata for the new object.  May be null for
     * no metadata.
     * @param data The initial contents of the object.  May be appended
     * to later.  May be null to create an object with no content.
     * @param mimeType the MIME type of the content.  Optional,
     * may be null.  If data is non-null and mimeType is null, the MIME
     * type will default to application/octet-stream.
     * @param checksum if not null, use the Checksum object to compute
     * the checksum for the create object request.  If appending
     * to the object with subsequent requests, use the same
     * checksum object for each request.
     * @return the ObjectId of the newly-created object for references by ID.
     * @throws EsuException if the request fails.
     */
    ObjectId createObjectOnPath( ObjectPath path, Acl acl,
    		MetadataList metadata,
            byte[] data, String mimeType, Checksum checksum );

    /**
     * Creates a new object in the cloud using a BufferSegment.
     * @param acl Access control list for the new object.  May be null
     * to use a default ACL
     * @param metadata Metadata for the new object.  May be null for
     * no metadata.
     * @param data The initial contents of the object.  May be appended
     * to later.  May be null to create an object with no content.
     * @param mimeType the MIME type of the content.  Optional,
     * may be null.  If data is non-null and mimeType is null, the MIME
     * type will default to application/octet-stream.
     * @return Identifier of the newly created object.
     * @throws EsuException if the request fails.
     */
    ObjectId createObjectFromSegment( Acl acl, MetadataList metadata,
            BufferSegment data, String mimeType );

    /**
     * Creates a new object in the cloud using a BufferSegment.
     * @param acl Access control list for the new object.  May be null
     * to use a default ACL
     * @param metadata Metadata for the new object.  May be null for
     * no metadata.
     * @param data The initial contents of the object.  May be appended
     * to later.  May be null to create an object with no content.
     * @param mimeType the MIME type of the content.  Optional,
     * may be null.  If data is non-null and mimeType is null, the MIME
     * type will default to application/octet-stream.
     * @param checksum if not null, use the Checksum object to compute
     * the checksum for the create object request.  If appending
     * to the object with subsequent requests, use the same
     * checksum object for each request.
     * @return Identifier of the newly created object.
     * @throws EsuException if the request fails.
     */
    ObjectId createObjectFromSegment( Acl acl, MetadataList metadata,
            BufferSegment data, String mimeType, Checksum checksum );

    /**
     * Creates a new object in the cloud using a BufferSegment on the
     * given path.
     * @param path the path to create the object on.
     * @param acl Access control list for the new object.  May be null
     * to use a default ACL
     * @param metadata Metadata for the new object.  May be null for
     * no metadata.
     * @param data The initial contents of the object.  May be appended
     * to later.  May be null to create an object with no content.
     * @param mimeType the MIME type of the content.  Optional,
     * may be null.  If data is non-null and mimeType is null, the MIME
     * type will default to application/octet-stream.
     * @return the ObjectId of the newly-created object for references by ID.
     * @throws EsuException if the request fails.
     */
    ObjectId createObjectFromSegmentOnPath( ObjectPath path,
    		Acl acl, MetadataList metadata,
            BufferSegment data, String mimeType );

    /**
     * Creates a new object in the cloud using a BufferSegment on the
     * given path.
     * @param path the path to create the object on.
     * @param acl Access control list for the new object.  May be null
     * to use a default ACL
     * @param metadata Metadata for the new object.  May be null for
     * no metadata.
     * @param data The initial contents of the object.  May be appended
     * to later.  May be null to create an object with no content.
     * @param mimeType the MIME type of the content.  Optional,
     * may be null.  If data is non-null and mimeType is null, the MIME
     * type will default to application/octet-stream.
     * @param checksum if not null, use the Checksum object to compute
     * the checksum for the create object request.  If appending
     * to the object with subsequent requests, use the same
     * checksum object for each request.
     * @return the ObjectId of the newly-created object for references by ID.
     * @throws EsuException if the request fails.
     */
    ObjectId createObjectFromSegmentOnPath( ObjectPath path,
    		Acl acl, MetadataList metadata,
            BufferSegment data, String mimeType, Checksum checksum );


    /**
     * Updates an object in the cloud.
     * @param id The ID of the object to update
     * @param acl Access control list for the new object. Optional, default
     * is NULL to leave the ACL unchanged.
     * @param metadata Metadata list for the new object.  Optional,
     * default is NULL for no changes to the metadata.
     * @param data The new contents of the object.  May be appended
     * to later. Optional, default is NULL (no content changes).
     * @param extent portion of the object to update.  May be null to indicate
     * the whole object is to be replaced.  If not null, the extent size must
     * match the data size.
     * @param mimeType the MIME type of the content.  Optional,
     * may be null.  If data is non-null and mimeType is null, the MIME
     * type will default to application/octet-stream.
     * @throws EsuException if the request fails.
     */
    void updateObject( Identifier id, Acl acl, MetadataList metadata,
            Extent extent, byte[] data, String mimeType );

    /**
     * Updates an object in the cloud.
     * @param id The ID of the object to update
     * @param acl Access control list for the new object. Optional, default
     * is NULL to leave the ACL unchanged.
     * @param metadata Metadata list for the new object.  Optional,
     * default is NULL for no changes to the metadata.
     * @param data The new contents of the object.  May be appended
     * to later. Optional, default is NULL (no content changes).
     * @param extent portion of the object to update.  May be null to indicate
     * the whole object is to be replaced.  If not null, the extent size must
     * match the data size.
     * @param mimeType the MIME type of the content.  Optional,
     * may be null.  If data is non-null and mimeType is null, the MIME
     * type will default to application/octet-stream.
     * @param checksum if not null, use the Checksum object to compute
     * the checksum for the update object request.  If appending
     * to the object with subsequent requests, use the same
     * checksum object for each request.
     * @throws EsuException if the request fails.
     */
    void updateObject( Identifier id, Acl acl, MetadataList metadata,
            Extent extent, byte[] data, String mimeType, Checksum checksum );

    /**
     * Updates an object in the cloud.
     * @param id The ID of the object to update
     * @param acl Access control list for the new object. Optional, default
     * is NULL to leave the ACL unchanged.
     * @param metadata Metadata list for the new object.  Optional,
     * default is NULL for no changes to the metadata.
     * @param data The new contents of the object.  May be appended
     * to later. Optional, default is NULL (no content changes).
     * @param extent portion of the object to update.  May be null to indicate
     * the whole object is to be replaced.  If not null, the extent size must
     * match the data size.
     * @param length The length of the stream in bytes.  If the stream
     * is longer than the length, only length bytes will be written.  If
     * the stream is shorter than the length, an error will occur.
     * @param mimeType the MIME type of the content.  Optional,
     * may be null.  If data is non-null and mimeType is null, the MIME
     * type will default to application/octet-stream.
     * @throws EsuException if the request fails.
     */
    void updateObjectFromStream( Identifier id, Acl acl, MetadataList metadata,
            Extent extent, InputStream data, long length, String mimeType );

    /**
     * Updates an object in the cloud using a portion of a buffer.
     * @param id The ID of the object to update
     * @param acl Access control list for the new object. Optional, default
     * is NULL to leave the ACL unchanged.
     * @param metadata Metadata list for the new object.  Optional,
     * default is NULL for no changes to the metadata.
     * @param data The new contents of the object.  May be appended
     * to later. Optional, default is NULL (no content changes).
     * @param extent portion of the object to update.  May be null to indicate
     * the whole object is to be replaced.  If not null, the extent size must
     * match the data size.
     * @param mimeType the MIME type of the content.  Optional,
     * may be null.  If data is non-null and mimeType is null, the MIME
     * type will default to application/octet-stream.
     * @throws EsuException if the request fails.
     */
    void updateObjectFromSegment( Identifier id, Acl acl, MetadataList metadata,
            Extent extent, BufferSegment data, String mimeType );

    /**
     * Updates an object in the cloud using a portion of a buffer.
     * @param id The ID of the object to update
     * @param acl Access control list for the new object. Optional, default
     * is NULL to leave the ACL unchanged.
     * @param metadata Metadata list for the new object.  Optional,
     * default is NULL for no changes to the metadata.
     * @param data The new contents of the object.  May be appended
     * to later. Optional, default is NULL (no content changes).
     * @param extent portion of the object to update.  May be null to indicate
     * the whole object is to be replaced.  If not null, the extent size must
     * match the data size.
     * @param mimeType the MIME type of the content.  Optional,
     * may be null.  If data is non-null and mimeType is null, the MIME
     * type will default to application/octet-stream.
     * @param checksum if not null, use the Checksum object to compute
     * the checksum for the update object request.  If appending
     * to the object with subsequent requests, use the same
     * checksum object for each request.
     * @throws EsuException if the request fails.
     */
    void updateObjectFromSegment( Identifier id, Acl acl, MetadataList metadata,
            Extent extent, BufferSegment data, String mimeType, Checksum checksum );

    /**
     * Writes the metadata into the object. If the tag does not exist, it is
     * created and set to the corresponding value. If the tag exists, the
     * existing value is replaced.
     * @param id the identifier of the object to update
     * @param metadata metadata to write to the object.
     */
    void setUserMetadata( Identifier id, MetadataList metadata );

    /**
     * Sets (overwrites) the ACL on the object.
     * @param id the identifier of the object to change the ACL on.
     * @param acl the new ACL for the object.
     */
    void setAcl( Identifier id, Acl acl );

    /**
     * Deletes an object from the cloud.
     * @param id the identifier of the object to delete.
     */
    void deleteObject( Identifier id );

    /**
     * Deletes a version of an object from the cloud.
     * @param id the identifier of the object to delete.
     */
    void deleteVersion( ObjectId id );

    /**
     * Fetches the user metadata for the object.
     * @param id the identifier of the object whose user metadata
     * to fetch.
     * @param tags A list of user metadata tags to fetch.  Optional.  If null,
     * all user metadata will be fetched.
     * @return The list of user metadata for the object.
     */
    MetadataList getUserMetadata( Identifier id, MetadataTags tags );

    /**
     * Fetches the system metadata for the object.
     * @param id the identifier of the object whose system metadata
     * to fetch.
     * @param tags A list of system metadata tags to fetch.  Optional.
     * Default value is null to fetch all system metadata.
     * @return The list of system metadata for the object.
     */
    MetadataList getSystemMetadata( Identifier id, MetadataTags tags );

    /**
     * Reads an object's content.
     * @param id the identifier of the object whose content to read.
     * @param extent the portion of the object data to read.  Optional.
     * Default is null to read the entire object.
     * @param buffer the buffer to use to read the extent.  Must be large
     * enough to read the response or an error will be thrown.  If null,
     * a buffer will be allocated to hold the response data.  If you pass
     * a buffer that is larger than the extent, only extent.getSize() bytes
     * will be valid.
     * @return the object data read as a byte array.
     */
    byte[] readObject( Identifier id, Extent extent, byte[] buffer );

    /**
     * Reads an object's content.
     * @param id the identifier of the object whose content to read.
     * @param extent the portion of the object data to read.  Optional.
     * Default is null to read the entire object.
     * @param buffer the buffer to use to read the extent.  Must be large
     * enough to read the response or an error will be thrown.  If null,
     * a buffer will be allocated to hold the response data.  If you pass
     * a buffer that is larger than the extent, only extent.getSize() bytes
     * will be valid.
     * @param checksum if not null, the given checksum object will be used
     * to verify checksums during the read operation.  Note that only erasure
     * coded objects will return checksums *and* if you're reading the object
     * in chunks, you'll have to read the data back sequentially to keep
     * the checksum consistent.  If the read operation does not return
     * a checksum from the server, the checksum operation will be skipped.
     * @return the object data read as a byte array.
     */
    byte[] readObject( Identifier id, Extent extent, byte[] buffer, Checksum checksum );

    /**
     * Reads an object's content and returns an InputStream to read the content.
     * Since the input stream is linked to the HTTP connection, it is imperative
     * that you close the input stream as soon as you are done with the stream
     * to release the underlying connection.
     * @param id the identifier of the object whose content to read.
     * @param extent the portion of the object data to read.  Optional.
     * Default is null to read the entire object.
     * @return an InputStream to read the object data.
     */
    InputStream readObjectStream( Identifier id, Extent extent );

    /**
     * Returns an object's ACL
     * @param id the identifier of the object whose ACL to read
     * @return the object's ACL
     */
    Acl getAcl( Identifier id );

    /**
     * Deletes metadata items from an object.
     * @param id the identifier of the object whose metadata to
     * delete.
     * @param tags the list of metadata tags to delete.
     */
    void deleteUserMetadata( Identifier id, MetadataTags tags );

    /**
     * Lists the versions of an object.
     * @param id the object whose versions to list.
     * @return The list of versions of the object.  If the object does
     * not have any versions, the array will be empty.
     * @deprecated this may not return all results if there are more than 4096
     * versions.  Use listVersions(ObjectId, ListOptions) instead to paginate
     * results.
     */
    List<Identifier> listVersions( Identifier id );

    /**
     * Lists the versions of an object.
     * @param id the object whose versions to list.
     * @return The list of versions of the object.  If the object does
     * not have any versions, the array will be empty.
     */
    List<Version> listVersions( ObjectId id, ListOptions options );

    /**
     * Creates a new immutable version of an object.
     * @param id the object to version
     * @return the id of the newly created version
     */
    ObjectId versionObject( Identifier id );

    /**
     * Lists all objects with the given tag.
     * @param tag the tag to search for
     * @return The list of objects with the given tag.  If no objects
     * are found the List will be empty.
     * @throws EsuException if there is an error loading the object list
     * @deprecated Use the version with ListOptions to control the result
     * count and handle large result sets.
     */
    List<Identifier> listObjects( MetadataTag tag );

    /**
     * Lists all objects with the given tag.
     * @param tag the tag to search for
     * @param options options for returning the list
     * @return The list of objects with the given tag.  If no objects
     * are found the List will be empty.
     * @throws EsuException if there is an error loading the object list
     */
    List<ObjectResult> listObjects( MetadataTag tag, ListOptions options );

    /**
     * Lists all objects with the given tag.
     * @param tag the tag to search for
     * @return The list of objects with the given tag.  If no objects
     * are found the List will be empty.
     * @throws EsuException if there is an error loading the object list
     * @deprecated Use the version with ListOptions to control the result
     * count and handle large result sets.
     */
    List<Identifier> listObjects( String tag );

    /**
     * Lists all objects with the given tag.
     * @param tag the tag to search for
     * @param options options for returning the list
     * @return The list of objects with the given tag.  If no objects
     * are found the List will be empty.
     * @throws EsuException if there is an error loading the object list
     */
    List<ObjectResult> listObjects( String tag, ListOptions options );

    /**
     * Lists all objects with the given tag and returns both their
     * IDs and their metadata.
     * @param tag the tag to search for
     * @return The list of objects with the given tag.  If no objects
     * are found the List will be empty.
     * @throws EsuException if there is an error loading the object list
     * @deprecated Use the version of listObjects with ListOptions to include
     * metadata.
     */
    List<ObjectResult> listObjectsWithMetadata( MetadataTag tag );

    /**
     * Lists all objects with the given tag and returns both their
     * IDs and their metadata.
     * @param tag the tag to search for
     * @return The list of objects with the given tag.  If no objects
     * are found the List will be empty.
     * @throws EsuException if there is an error loading the object list
     * @deprecated Use the version of listObjects with ListOptions to include
     * metadata.
     */
    List<ObjectResult> listObjectsWithMetadata( String tag );

    /**
     * Returns a list of the tags that are listable the current user's tennant.
     * @param tag optional.  If specified, the list will be limited to the tags
     * under the specified tag.  If null, only top level tags will be returned.
     * @return the list of listable tags.
     */
    MetadataTags getListableTags( MetadataTag tag );

    /**
     * Returns a list of the tags that are listable the current user's tennant.
     * @param tag optional.  If specified, the list will be limited to the tags
     * under the specified tag.  If null, only top level tags will be returned.
     * @return the list of listable tags.
     */
    MetadataTags getListableTags( String tag );


    /**
     * Returns the list of user metadata tags assigned to the object.
     * @param id the object whose metadata tags to list
     * @return the list of user metadata tags assigned to the object
     */
    MetadataTags listUserMetadataTags( Identifier id );

    /**
     * Executes a query for objects matching the specified XQuery string.
     * @param xquery the XQuery string to execute against the cloud.
     * @return the list of objects matching the query.  If no objects
     * are found, the array will be empty.
     */
    List<ObjectId> queryObjects( String xquery );

    /**
     * Lists the contents of a directory.
     * @param path the path to list.  Must be a directory.
     * @return the directory entries in the directory.
     * @deprecated Use the version with ListOptions to control the result
     * count and handle large result sets.
     */
    List<DirectoryEntry> listDirectory( ObjectPath path );

    /**
     * Lists the contents of a directory.
     * @param path the path to list.  Must be a directory.
     * @param options options for returning the list.
     * @return the directory entries in the directory.
     */
    List<DirectoryEntry> listDirectory( ObjectPath path, ListOptions options );

    /**
     * Returns all of an object's metadata and its ACL in
     * one call.
     * @param id the object's identifier.
     * @return the object's metadata
     */
    ObjectMetadata getAllMetadata( Identifier id );

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
    URL getShareableUrl( Identifier id, Date expiration );


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
    void rename(ObjectPath source, ObjectPath destination, boolean force);

    /**
     * Restores a version of an object to the base version (i.e. "promote" an
     * old version to the current version).
     * @param id Base object ID (target of the restore)
     * @param vId Version object ID to restore
     */
    void restoreVersion( ObjectId id, ObjectId vId );

    /**
     * Gets the current Atmos server information.  Currently, this simply
     * returns the version of Atmos that is running.
     * @return the ServiceInformation object
     */
    ServiceInformation getServiceInformation();

    /**
     * Get information about an object's state including
     * replicas, expiration, and retention.
     * @param id the object identifier
     * @return and ObjectInfo object containing the state information
     */
    ObjectInfo getObjectInfo( Identifier id );

    //---------- Features supported by the Atmos 2.0 REST API. ----------\\

    /**
     * Unsupported.
     */
    void hardLink( ObjectPath source, ObjectPath target );

    /**
     * Creates a shareable URL that sets the Content-Disposition header when
     * streaming the content.  Requires the browser-compat feature, check
     * ServiceInformation for supported features.
     *
     * @param id          the ID to create the Shareable URL for.
     * @param expiration  The expration timestamp for the URL
     * @param disposition the value of the Content-Disposition header, e.g.
     *                    "attachment; filename=\"filename.txt\""
     * @return the new shareable URL.
     */
    URL getShareableUrl( Identifier id, Date expiration, String disposition );

    /**
     * Creates a new object in the cloud.
     *
     * @param keyPool  A name which represents the keyspace for this key. A pool is analogous
     *                 to an S3 bucket and two objects with the same name can exist in different pools.
     * @param key      The key that uniquely identifies this object within its keyPool.
     * @param acl      Access control list for the new object.  May be null
     *                 to use a default ACL
     * @param metadata Metadata for the new object.  May be null for
     *                 no metadata.
     * @param data     The initial contents of the object.  May be appended
     *                 to later.  May be null to create an object with no content.
     * @param length   The length of the stream in bytes.  If the stream
     *                 is longer than the length, only length bytes will be written.  If
     *                 the stream is shorter than the length, an error will occur.
     * @param mimeType the MIME type of the content.  Optional,
     *                 may be null.  If data is non-null and mimeType is null, the MIME
     *                 type will default to application/octet-stream.
     * @return Identifier of the newly created object.
     * @throws EsuException if the request fails.
     */
    ObjectId createObjectWithKey( String keyPool, String key, Acl acl, MetadataList metadata,
                                  byte[] data, long length, String mimeType );

    /**
     * Creates a new object in the cloud.
     *
     * @param keyPool  A name which represents the keyspace for this key. A pool is analogous
     *                 to an S3 bucket and two objects with the same name can exist in different pools.
     * @param key      The key that uniquely identifies this object within its keyPool.
     * @param acl      Access control list for the new object.  May be null
     *                 to use a default ACL
     * @param metadata Metadata for the new object.  May be null for
     *                 no metadata.
     * @param data     The initial contents of the object.  May be appended
     *                 to later.  May be null to create an object with no content.
     * @param length   The length of the stream in bytes.  If the stream
     *                 is longer than the length, only length bytes will be written.  If
     *                 the stream is shorter than the length, an error will occur.
     * @param mimeType the MIME type of the content.  Optional,
     *                 may be null.  If data is non-null and mimeType is null, the MIME
     *                 type will default to application/octet-stream.
     * @param checksum if not null, use the Checksum object to compute
     *                 the checksum for the create object request.  If appending
     *                 to the object with subsequent requests, use the same
     *                 checksum object for each request.
     * @return Identifier of the newly created object.
     * @throws EsuException if the request fails.
     */
    ObjectId createObjectWithKey( String keyPool, String key, Acl acl, MetadataList metadata,
                                  byte[] data, long length, String mimeType, Checksum checksum );

    /**
     * Creates a new object in the cloud using a BufferSegment.
     *
     * @param keyPool  A name which represents the keyspace for this key. A pool is analogous
     *                 to an S3 bucket and two objects with the same name can exist in different pools.
     * @param key      The key that uniquely identifies this object within its keyPool.
     * @param acl      Access control list for the new object.  May be null
     *                 to use a default ACL
     * @param metadata Metadata for the new object.  May be null for
     *                 no metadata.
     * @param data     The initial contents of the object.  May be appended
     *                 to later.  May be null to create an object with no content.
     * @param mimeType the MIME type of the content.  Optional,
     *                 may be null.  If data is non-null and mimeType is null, the MIME
     *                 type will default to application/octet-stream.
     * @return Identifier of the newly created object.
     * @throws EsuException if the request fails.
     */
    ObjectId createObjectWithKeyFromSegment( String keyPool, String key, Acl acl, MetadataList metadata,
                                             BufferSegment data, String mimeType );

    /**
     * Creates a new object in the cloud using a BufferSegment.
     *
     * @param keyPool  A name which represents the keyspace for this key. A pool is analogous
     *                 to an S3 bucket and two objects with the same name can exist in different pools.
     * @param key      The key that uniquely identifies this object within its keyPool.
     * @param acl      Access control list for the new object.  May be null
     *                 to use a default ACL
     * @param metadata Metadata for the new object.  May be null for
     *                 no metadata.
     * @param data     The initial contents of the object.  May be appended
     *                 to later.  May be null to create an object with no content.
     * @param mimeType the MIME type of the content.  Optional,
     *                 may be null.  If data is non-null and mimeType is null, the MIME
     *                 type will default to application/octet-stream.
     * @param checksum if not null, use the Checksum object to compute
     *                 the checksum for the create object request.  If appending
     *                 to the object with subsequent requests, use the same
     *                 checksum object for each request.
     * @return Identifier of the newly created object.
     * @throws EsuException if the request fails.
     */
    ObjectId createObjectWithKeyFromSegment( String keyPool, String key, Acl acl, MetadataList metadata,
                                             BufferSegment data, String mimeType, Checksum checksum );

    /**
     * Creates a new object in the cloud.
     *
     * @param keyPool  A name which represents the keyspace for this key. A pool is analogous
     *                 to an S3 bucket and two objects with the same name can exist in different pools.
     * @param key      The key that uniquely identifies this object within its keyPool.
     * @param acl      Access control list for the new object.  May be null
     *                 to use a default ACL
     * @param metadata Metadata for the new object.  May be null for
     *                 no metadata.
     * @param data     The initial contents of the object.  May be appended
     *                 to later.  The stream will NOT be closed at the end of the request.
     * @param length   The length of the stream in bytes.  If the stream
     *                 is longer than the length, only length bytes will be written.  If
     *                 the stream is shorter than the length, an error will occur.
     * @param mimeType the MIME type of the content.  Optional,
     *                 may be null.  If data is non-null and mimeType is null, the MIME
     *                 type will default to application/octet-stream.
     * @return Identifier of the newly created object.
     * @throws EsuException if the request fails.
     */
    ObjectId createObjectWithKeyFromStream( String keyPool, String key, Acl acl, MetadataList metadata,
                                            InputStream data, long length, String mimeType );

    /**
     * Deletes an object from the cloud.
     *
     * @param keyPool A name which represents the keyspace for this key. A pool is analogous
     *                to an S3 bucket and two objects with the same name can exist in different pools.
     * @param key     The key that uniquely identifies this object within its keyPool.
     */
    void deleteObjectWithKey( String keyPool, String key );

    /**
     * Returns all of an object's metadata and its ACL in
     * one call.
     *
     * @param keyPool A name which represents the keyspace for this key. A pool is analogous
     *                to an S3 bucket and two objects with the same name can exist in different pools.
     * @param key     The key that uniquely identifies this object within its keyPool.
     * @return the object's metadata
     */
    ObjectMetadata getAllMetadata( String keyPool, String key );

    /**
     * Fetches the system metadata for the object.
     *
     * @param keyPool A name which represents the keyspace for this key. A pool is analogous
     *                to an S3 bucket and two objects with the same name can exist in different pools.
     * @param key     The key that uniquely identifies this object within its keyPool.
     * @param tags    A list of system metadata tags to fetch.  Optional.
     *                Default value is null to fetch all system metadata.
     * @return The list of system metadata for the object.
     */
    MetadataList getSystemMetadata( String keyPool, String key, MetadataTags tags );

    /**
     * Reads an object's content.
     *
     * @param keyPool A name which represents the keyspace for this key. A pool is analogous
     *                to an S3 bucket and two objects with the same name can exist in different pools.
     * @param key     The key that uniquely identifies this object within its keyPool.
     * @param extent  the portion of the object data to read.  Optional.
     *                Default is null to read the entire object.
     * @param buffer  the buffer to use to read the extent.  Must be large
     *                enough to read the response or an error will be thrown.  If null,
     *                a buffer will be allocated to hold the response data.  If you pass
     *                a buffer that is larger than the extent, only extent.getSize() bytes
     *                will be valid.
     * @return the object data read as a byte array.
     */
    byte[] readObjectWithKey( String keyPool, String key, Extent extent, byte[] buffer );

    /**
     * Reads an object's content.
     *
     * @param keyPool  A name which represents the keyspace for this key. A pool is analogous
     *                 to an S3 bucket and two objects with the same name can exist in different pools.
     * @param key      The key that uniquely identifies this object within its keyPool.
     * @param extent   the portion of the object data to read.  Optional.
     *                 Default is null to read the entire object.
     * @param buffer   the buffer to use to read the extent.  Must be large
     *                 enough to read the response or an error will be thrown.  If null,
     *                 a buffer will be allocated to hold the response data.  If you pass
     *                 a buffer that is larger than the extent, only extent.getSize() bytes
     *                 will be valid.
     * @param checksum if not null, the given checksum object will be used
     *                 to verify checksums during the read operation.  Note that only erasure
     *                 coded objects will return checksums *and* if you're reading the object
     *                 in chunks, you'll have to read the data back sequentially to keep
     *                 the checksum consistent.  If the read operation does not return
     *                 a checksum from the server, the checksum operation will be skipped.
     * @return the object data read as a byte array.
     */
    byte[] readObjectWithKey( String keyPool, String key, Extent extent, byte[] buffer, Checksum checksum );

    /**
     * Reads an object's content and returns an InputStream to read the content.
     * Since the input stream is linked to the HTTP connection, it is imperative
     * that you close the input stream as soon as you are done with the stream
     * to release the underlying connection.
     *
     * @param keyPool A name which represents the keyspace for this key. A pool is analogous
     *                to an S3 bucket and two objects with the same name can exist in different pools.
     * @param key     The key that uniquely identifies this object within its keyPool.
     * @param extent  the portion of the object data to read.  Optional.
     *                Default is null to read the entire object.
     * @return an InputStream to read the object data.
     */
    InputStream readObjectStreamWithKey( String keyPool, String key, Extent extent );

    /**
     * Updates an object in the cloud.
     *
     * @param keyPool  A name which represents the keyspace for this key. A pool is analogous
     *                 to an S3 bucket and two objects with the same name can exist in different pools.
     * @param key      The key that uniquely identifies this object within its keyPool.
     * @param acl      Access control list for the new object. Optional, default
     *                 is NULL to leave the ACL unchanged.
     * @param metadata Metadata list for the new object.  Optional,
     *                 default is NULL for no changes to the metadata.
     * @param data     The new contents of the object.  May be appended
     *                 to later. Optional, default is NULL (no content changes).
     * @param extent   portion of the object to update.  May be null to indicate
     *                 the whole object is to be replaced.  If not null, the extent size must
     *                 match the data size.
     * @param mimeType the MIME type of the content.  Optional,
     *                 may be null.  If data is non-null and mimeType is null, the MIME
     *                 type will default to application/octet-stream.
     * @throws EsuException if the request fails.
     */
    void updateObjectWithKey( String keyPool, String key, Acl acl, MetadataList metadata,
                              Extent extent, byte[] data, String mimeType );

    /**
     * Updates an object in the cloud.
     *
     * @param keyPool  A name which represents the keyspace for this key. A pool is analogous
     *                 to an S3 bucket and two objects with the same name can exist in different pools.
     * @param key      The key that uniquely identifies this object within its keyPool.
     * @param acl      Access control list for the new object. Optional, default
     *                 is NULL to leave the ACL unchanged.
     * @param metadata Metadata list for the new object.  Optional,
     *                 default is NULL for no changes to the metadata.
     * @param data     The new contents of the object.  May be appended
     *                 to later. Optional, default is NULL (no content changes).
     * @param extent   portion of the object to update.  May be null to indicate
     *                 the whole object is to be replaced.  If not null, the extent size must
     *                 match the data size.
     * @param mimeType the MIME type of the content.  Optional,
     *                 may be null.  If data is non-null and mimeType is null, the MIME
     *                 type will default to application/octet-stream.
     * @param checksum if not null, use the Checksum object to compute
     *                 the checksum for the update object request.  If appending
     *                 to the object with subsequent requests, use the same
     *                 checksum object for each request.
     * @throws EsuException if the request fails.
     */
    void updateObjectWithKey( String keyPool, String key, Acl acl, MetadataList metadata,
                              Extent extent, byte[] data, String mimeType, Checksum checksum );

    /**
     * Updates an object in the cloud.
     *
     * @param keyPool  A name which represents the keyspace for this key. A pool is analogous
     *                 to an S3 bucket and two objects with the same name can exist in different pools.
     * @param key      The key that uniquely identifies this object within its keyPool.
     * @param acl      Access control list for the new object. Optional, default
     *                 is NULL to leave the ACL unchanged.
     * @param metadata Metadata list for the new object.  Optional,
     *                 default is NULL for no changes to the metadata.
     * @param data     The new contents of the object.  May be appended
     *                 to later. Optional, default is NULL (no content changes).
     * @param extent   portion of the object to update.  May be null to indicate
     *                 the whole object is to be replaced.  If not null, the extent size must
     *                 match the data size.
     * @param length   The length of the stream in bytes.  If the stream
     *                 is longer than the length, only length bytes will be written.  If
     *                 the stream is shorter than the length, an error will occur.
     * @param mimeType the MIME type of the content.  Optional,
     *                 may be null.  If data is non-null and mimeType is null, the MIME
     *                 type will default to application/octet-stream.
     * @throws EsuException if the request fails.
     */
    void updateObjectWithKeyFromStream( String keyPool, String key, Acl acl, MetadataList metadata,
                                        Extent extent, InputStream data, long length, String mimeType );

    /**
     * Updates an object in the cloud using a portion of a buffer.
     *
     * @param keyPool  A name which represents the keyspace for this key. A pool is analogous
     *                 to an S3 bucket and two objects with the same name can exist in different pools.
     * @param key      The key that uniquely identifies this object within its keyPool.
     * @param acl      Access control list for the new object. Optional, default
     *                 is NULL to leave the ACL unchanged.
     * @param metadata Metadata list for the new object.  Optional,
     *                 default is NULL for no changes to the metadata.
     * @param data     The new contents of the object.  May be appended
     *                 to later. Optional, default is NULL (no content changes).
     * @param extent   portion of the object to update.  May be null to indicate
     *                 the whole object is to be replaced.  If not null, the extent size must
     *                 match the data size.
     * @param mimeType the MIME type of the content.  Optional,
     *                 may be null.  If data is non-null and mimeType is null, the MIME
     *                 type will default to application/octet-stream.
     * @throws EsuException if the request fails.
     */
    void updateObjectWithKeyFromSegment( String keyPool, String key, Acl acl, MetadataList metadata,
                                         Extent extent, BufferSegment data, String mimeType );

    /**
     * Updates an object in the cloud using a portion of a buffer.
     *
     * @param keyPool  A name which represents the keyspace for this key. A pool is analogous
     *                 to an S3 bucket and two objects with the same name can exist in different pools.
     * @param key      The key that uniquely identifies this object within its keyPool.
     * @param acl      Access control list for the new object. Optional, default
     *                 is NULL to leave the ACL unchanged.
     * @param metadata Metadata list for the new object.  Optional,
     *                 default is NULL for no changes to the metadata.
     * @param data     The new contents of the object.  May be appended
     *                 to later. Optional, default is NULL (no content changes).
     * @param extent   portion of the object to update.  May be null to indicate
     *                 the whole object is to be replaced.  If not null, the extent size must
     *                 match the data size.
     * @param mimeType the MIME type of the content.  Optional,
     *                 may be null.  If data is non-null and mimeType is null, the MIME
     *                 type will default to application/octet-stream.
     * @param checksum if not null, use the Checksum object to compute
     *                 the checksum for the update object request.  If appending
     *                 to the object with subsequent requests, use the same
     *                 checksum object for each request.
     * @throws EsuException if the request fails.
     */
    void updateObjectWithKeyFromSegment( String keyPool, String key, Acl acl, MetadataList metadata,
                                         Extent extent, BufferSegment data, String mimeType, Checksum checksum );
}
