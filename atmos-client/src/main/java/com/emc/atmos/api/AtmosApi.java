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
package com.emc.atmos.api;

import com.emc.atmos.api.bean.*;
import com.emc.atmos.api.request.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Date;
import java.util.Map;
import java.util.Set;

/**
 * This interface defines the basic operations available through the Atmos REST web service.
 */
public interface AtmosApi {
    ServiceInformation getServiceInformation();

    /**
     * Calculates rough clock skew. Implementations will likely use the date response header from an inert HTTP
     * request.
     *
     * @return The clock skew between the target Atmos node and the local machine.
     */
    long calculateServerClockSkew();

    /**
     * Creates a new object in the cloud.
     *
     * @param content     The initial content of the object. May be appended to later. May be null to create an
     *                    object with no content. The content object can be any object type that is supported by the
     *                    implementation.
     * @param contentType the MIME type of the content. Optional, may be null. If null, defaults to
     *                    application/octet-stream.
     *
     * @return ObjectId of the newly created object.
     */
    ObjectId createObject( Object content, String contentType );

    /**
     * Creates a new object in the cloud with the specified identifier.
     *
     * @param identifier  The identifier to use for the new object. This may be an ObjectPath or an ObjectKey.
     * @param content     The initial content of the object. May be appended to later. May be null to create an
     *                    object with no content. The content object can be any object type that is supported by the
     *                    implementation.
     * @param contentType the MIME type of the content. Optional, may be null. If null, defaults to
     *                    application/octet-stream.
     *
     * @return ObjectId of the newly created object.
     */
    ObjectId createObject( ObjectIdentifier identifier, Object content, String contentType );

    /**
     * Creates a new object in the cloud using all of the options provided in the request object.
     *
     * @param request The request object (click on the class name to get more information).
     *
     * @return The response received from Atmos. This object contains the ObjectId of the newly created object plus
     *         additional details about the response, such as headers (click on the class name to get more
     *         information).
     */
    CreateObjectResponse createObject( CreateObjectRequest request );


    /**
     * Reads an object's entire content from the cloud.
     *
     * @param identifier The identifier of the object to read. May be any ObjectIdentifier.
     * @param objectType The type of object to return. This can be any object type supported by the implementation.
     *
     * @return An object (of type objectType) representing the content of the object in the cloud.
     *
     * @throws IOException if an exception occurs while reading the object. Note that IOExceptions are generally
     *                     retried automatically (configured in {@link AtmosConfig}).
     */
    <T> T readObject( ObjectIdentifier identifier, Class<T> objectType ) throws IOException;

    /**
     * Reads an object's content from the cloud.
     *
     * @param identifier The identifier of the object to read. May be any ObjectIdentifier.
     * @param range      (optional) The range of bytes to read from the object. A null value will read the entire
     *                   object.
     * @param objectType The type of object to return. This can be any object type supported by the implementation.
     *
     * @return An object (of type objectType) representing the content of the object in the cloud.
     *
     * @throws IOException if an exception occurs while reading the object. Note that IOExceptions are generally
     *                     retried automatically (configured in {@link AtmosConfig}).
     */
    <T> T readObject( ObjectIdentifier identifier, Range range, Class<T> objectType ) throws IOException;

    /**
     * Reads an object's content from the cloud using all of the options provided in the request object.
     *
     * @param request    The request object (click on the class name to get more information)
     * @param objectType The type of object to return. This can be any object type supported by the implementation.
     *
     * @return The response received from Atmos. This object contains an object (of type objectType) representing the
     *         content of the object in the cloud, plus additional details about the response, such as headers (click
     *         on the class name to get more information).
     *
     * @throws IOException if an exception occurs while reading the object. Note that IOExceptions are generally
     *                     retried automatically (configured in {@link AtmosConfig}).
     */
    <T> ReadObjectResponse<T> readObject( ReadObjectRequest request, Class<T> objectType )
            throws IOException;

    /**
     * Provides an InputStream to read an object's content from the cloud.
     *
     * @param identifier The identifier of the object to read. May be any ObjectIdentifier.
     * @param range      (optional) The range of bytes to read from the object. A null value will read the entire
     *                   object.
     *
     * @return The response received from Atmos. This object contains the InputStream to read the object from the
     *         cloud, plus additional details about the response, such as headers (click on the class name to get more
     *         information).
     */
    ReadObjectResponse<InputStream> readObjectStream( ObjectIdentifier identifier, Range range );

    /**
     * Updates an object's content in the cloud.
     *
     * @param identifier The identifier of the object to update. May be any ObjectIdentifier.
     * @param content    The new content of the object. The content object can be any object type that is supported by
     *                   the implementation.
     */
    void updateObject( ObjectIdentifier identifier, Object content );

    /**
     * Updates an object's content in the cloud.
     *
     * @param identifier The identifier of the object to update. May be any ObjectIdentifier.
     * @param content    The new content of the object. The content object can be any object type that is supported by
     *                   the implementation.
     * @param range      (optional) The portion of the object to update (expressed as a range of bytes). A null value
     *                   will overwrite the entire object.
     */
    void updateObject( ObjectIdentifier identifier, Object content, Range range );

    /**
     * Updates an object's content in the cloud using all of the options provided in the request object.
     *
     * @param request The request object (click on the class name to get more information)
     *
     * @return The response received from Atmos. This object contains details about the response, such as headers
     *         (click on the class name to get more information).
     */
    BasicResponse updateObject( UpdateObjectRequest request );

    /**
     * Deletes an object from the cloud
     *
     * @param identifier The identifier of the object to delete. May be any ObjectIdentifier.
     */
    void delete( ObjectIdentifier identifier );

    /**
     * Creates a directory in the subtenant namespace using the specified path.
     *
     * @param path The path of the directory to create.
     *
     * @return The ObjectId of the newly created directory.
     */
    ObjectId createDirectory( ObjectPath path );

    /**
     * Creates a directory in the subtenant namespace using the specified path, acl and metadata.
     *
     * @param path     The path of the directory to create.
     * @param acl      The ACL to assign to the new directory.
     * @param metadata The metadata to associate with the new directory.
     *
     * @return The ObjectId of the newly created directory.
     */
    ObjectId createDirectory( ObjectPath path, Acl acl, Metadata... metadata );

    /**
     * Lists the contents of a directory in the namespace.
     *
     * @param request The request object (click on the class name to get more information)
     *
     * @return The response received from Atmos. This object contains the list of directory entries, plus details about
     *         the response, such as headers (click on the class name to get more information). Note that if a paging
     *         token is present in the response, it will be set on the request object.
     */
    ListDirectoryResponse listDirectory( ListDirectoryRequest request );

    /**
     * Moves an object in the namespace to a new location.
     *
     * @param oldPath   The existing path of the object.
     * @param newPath   The new path that the object should have.
     * @param overwrite If true and an object already exists at newPath, this call will overwrite that object.
     */
    void move( ObjectPath oldPath, ObjectPath newPath, boolean overwrite );

    /**
     * Lists the names of all metadata associated with the specified object.
     *
     * @param identifier The identifier of the object. Can be any ObjectIdentifier.
     *
     * @return A list of the names of all user metadata associated with the object.
     */
    Map<String, Boolean> getUserMetadataNames( ObjectIdentifier identifier );

    /**
     * Gets the user metadata associated with the specified object.
     *
     * @param identifier    The identifier of the object. Can be any ObjectIdentifier.
     * @param metadataNames (optional) Constrains the result to include only the metadata named in this list.
     *
     * @return A map of metadata names to Metadata objects.
     */
    Map<String, Metadata> getUserMetadata( ObjectIdentifier identifier, String... metadataNames );

    /**
     * Gets the system metadata associated with the specified object.
     *
     * @param identifier    The identifier of the object. Can be any ObjectIdentifier.
     * @param metadataNames (optional) Constrains the result to include only the metadata named in this list.
     *
     * @return A map of metadata names to Metadata objects.
     */
    Map<String, Metadata> getSystemMetadata( ObjectIdentifier identifier, String... metadataNames );

    /**
     * Determines whether an object exists with the specified identifier.  Implementations will probably make
     * a get-system-metadata call and return false if the server returns a 404.
     *
     * @param identifier The identifier of the object. Can be any ObjectIdentifier.
     * @return true if an object exists in the cloud with the specified identifier.
     */
    boolean objectExists( ObjectIdentifier identifier );

    /**
     * Gets an object's metadata, ACL and content-type all in one call.
     *
     * @param identifier The identifier of the object. Can be any ObjectIdentifier.
     *
     * @return Object metadata (click on the class for more information).
     */
    ObjectMetadata getObjectMetadata( ObjectIdentifier identifier );

    /**
     * Adds or replaces user metadata associated with the specified object. Note that this method will not delete
     * metadata. That must be done via {@link #deleteUserMetadata(ObjectIdentifier, String...)}.
     *
     * @param identifier The identifier of the object. Can be any ObjectIdentifier.
     * @param metadata   The metadata to add to or replace on the object.
     */
    void setUserMetadata( ObjectIdentifier identifier, Metadata... metadata );

    /**
     * Deletes metadata associated with the specified object.
     *
     * @param identifier The identifier of the object. Can be any ObjectIdentifier.
     * @param names      The names of the metadata to delete/remove from the object.
     */
    void deleteUserMetadata( ObjectIdentifier identifier, String... names );

    /**
     * Lists the children of the specified metadata path in the hierarchy of listable metadata for the subtenant. For
     * example, if a hierarchy of listable metadata exists as such: /ford/mustang/gt, then listing
     * "ford" would probably yield "mustang", "f150", "crown victoria", etc.
     *
     * @param metadataName The metadata name (sometime called a "tag") whose children should be returned. If null, all
     *                     root-level names will be returned (i.e. "ford", "chevy", "chrysler").
     *
     * @return The names of all children of the specified listable metadata path.
     */
    Set<String> listMetadata( String metadataName );

    /**
     * Lists all objects that are assigned the specified listable metadata using all of the options provided in the
     * request object.
     *
     * @param request The request object (click on the class name to get more information)
     *
     * @return The response received from Atmos. This object contains the list of object entries, plus details about
     *         the response, such as headers (click on the class name to get more information). Note that if a paging
     *         token is present in the response, it will be set on the request object.
     */
    ListObjectsResponse listObjects( ListObjectsRequest request );

    /**
     * Gets the ACL for the specified object.
     *
     * @param identifier The identifier of the object. Can be any ObjectIdentifier.
     *
     * @return The ACL for the specified object.
     */
    Acl getAcl( ObjectIdentifier identifier );

    /**
     * Sets the ACL for the specified object.
     *
     * @param identifier The identifier of the object. Can be any ObjectIdentifier.
     * @param acl        The new ACL for the specified object.
     */
    void setAcl( ObjectIdentifier identifier, Acl acl );

    /**
     * Gets storage information for the specified object.
     *
     * @param identifier The identifier of the object. Can be any ObjectIdentifier.
     *
     * @return Storage information for the specified object.
     */
    ObjectInfo getObjectInfo( ObjectIdentifier identifier );

    /**
     * Creates a new immutable version (snapshot) of the specified object from its current state.
     *
     * @param identifier The identifier of the object. Can be any ObjectIdentifier.
     *
     * @return The ObjectId of the new version. Use this identifier to retrieve the snapshot created by this call.
     */
    ObjectId createVersion( ObjectIdentifier identifier );

    /**
     * Lists all versions (snapshots) of the specified object using all of the options provided in the request object.
     *
     * @param request The request object (click on the class name to get more information)
     *
     * @return The response received from Atmos. This object contains the list of object versions, plus details about
     *         the response, such as headers (click on the class name to get more information). Note that if a paging
     *         token is present in the response, it will be set on the request object.
     */
    ListVersionsResponse listVersions( ListVersionsRequest request );

    /**
     * Restores the specified version (snapshot) of the specified object so that the object's current state reflects
     * that of the version. Note this will overwrite the current state of the object.
     *
     * @param objectId  The ObjectId of the object.
     * @param versionId The ObjectId of the version (must be a version of the specified object).
     */
    void restoreVersion( ObjectId objectId, ObjectId versionId );

    /**
     * Deletes the specified object version.
     *
     * @param versionId The ObjectId of the version to delete (must point to a version and not an object).
     */
    void deleteVersion( ObjectId versionId );

    /**
     * Constructs a pre-signed URL to an object, which anyone can then use to retrieve the object. The URL expires at
     * the specified time and is tamper proof. However, it does reveal the full tokenId of the user. If this is
     * undesirable and the Atmos cloud is at version 2.1 and above, look at
     * {@link #createAccessToken(com.emc.atmos.api.request.CreateAccessTokenRequest)}.
     *
     * @param identifier     The identifier of the object. Can be any ObjectIdentifier.
     * @param expirationDate The date at which the generated URL will no longer be valid.
     *
     * @return A public URL that will retrieve the contents of the specified object until expirationDate.
     *
     * @throws MalformedURLException if the configured Atmos endpoint is syntactically invalid.
     */
    URL getShareableUrl( ObjectIdentifier identifier, Date expirationDate ) throws MalformedURLException;

    /**
     * Constructs a shareable URL with a specific content-disposition. The disposition value will be set in the
     * Content-Disposition header of the response when the URL is accessed. Requires the browser-compat feature, check
     * ServiceInformation for supported features.
     *
     * @param identifier     The identifier of the object. Can be any ObjectIdentifier.
     * @param expirationDate The date at which the generated URL will no longer be valid.
     * @param disposition    the value of the Content-Disposition header, e.g.
     *                       "attachment; filename=\"filename.txt\""
     *
     * @return A public URL that will retrieve the contents of the specified object and set the specified
     *         content-disposition until expirationDate.
     *
     * @throws MalformedURLException if the configured Atmos endpoint is syntactically invalid.
     */
    URL getShareableUrl( ObjectIdentifier identifier, Date expirationDate, String disposition )
            throws MalformedURLException;

    /**
     * Creates an anonymous access token using all of the options provided in the request object.
     *
     * @param request The request object (click on the class name to get more information)
     *
     * @return The response received from Atmos. This object contains the access token URL, plus details about
     *         the response, such as headers (click on the class name to get more information).
     *
     * @throws MalformedURLException if the configured Atmos endpoint is syntactically invalid.
     */
    CreateAccessTokenResponse createAccessToken( CreateAccessTokenRequest request )
            throws MalformedURLException;

    /**
     * Retrieves details about the specified access token. Implementation simply extracts the token ID from the URL and
     * calls {@link #getAccessToken(String)}.
     *
     * @param url The URL of the access token.
     *
     * @return The response received from Atmos. This object contains the AccessToken, plus details about
     *         the response, such as headers (click on the class name to get more information).
     */
    GetAccessTokenResponse getAccessToken( URL url );

    /**
     * Retrieves details about the specified access token.
     *
     * @param accessTokenId The ID of the access token.
     *
     * @return The response received from Atmos. This object contains the AccessToken, plus details about
     *         the response, such as headers (click on the class name to get more information).
     */
    GetAccessTokenResponse getAccessToken( String accessTokenId );

    /**
     * Deletes the specified access token. Implementation simply extracts the token ID from the URL and calls {@link
     * #deleteAccessToken(String)}.
     *
     * @param url The URL of the access token.
     */
    void deleteAccessToken( URL url );

    /**
     * Deletes the specified access token.
     *
     * @param accessTokenId The ID of the access token.
     */
    void deleteAccessToken( String accessTokenId );

    /**
     * Lists all access tokens owned by the user using all of the options provided in the request object.
     *
     * @param request The request object (click on the class name to get more information)
     *
     * @return The response received from Atmos. This object contains the list of access tokens, plus details about
     *         the response, such as headers (click on the class name to get more information). Note that if a paging
     *         token is present in the response, it will be set on the request object.
     */
    ListAccessTokensResponse listAccessTokens( ListAccessTokensRequest request );

    /**
     * Pre-signs a request with a specified expiration time. The pre-signed request can be executed at a later time via
     * the {@link #execute(com.emc.atmos.api.request.PreSignedRequest, Class, Object)} method. This feature is useful
     * if you intend to serialize the pre-signed request to some other system which does not have access to Atmos
     * credentials.
     *
     * @param request    the request to pre-sign (can be executed at a later time)
     * @param expiration the date at which the pre-signed request becomes invalid and will no longer be accepted
     *
     * @return a pre-signed request that can be executed at a later time and expires at <code>expiration</code>
     *
     * @throws java.net.MalformedURLException if the configured Atmos endpoint is syntactically invalid.
     */
    PreSignedRequest preSignRequest( Request request, Date expiration ) throws MalformedURLException;

    /**
     * Executes a pre-signed request, sending the specified content as a body (if provided) and returning the specified
     * resultType (if provided). The content object and result type can be any object type that is supported by the
     * implementation.
     *
     * @param request    A pre-signed request generated by calling
     *                   {@link #preSignRequest(com.emc.atmos.api.request.Request, java.util.Date)}.
     * @param resultType (optional) The type of object to return. This can be any object type supported by the
     *                   implementation.
     * @param content    (optional) The body content to send in the request (i.e. the object content for a
     *                   create-object request).  Can be any object type supported by the implementation.
     *
     * @return The response received from Atmos. This object contains an object (of type resultType) representing the
     *         content of the response (i.e. the object content of a read-object request), plus additional details
     *         about the response, such as headers (click on the class name to get more information).
     *
     * @throws URISyntaxException if the URL in the pre-signed request is syntactically invalid.
     */
    <T> GenericResponse<T> execute( PreSignedRequest request, Class<T> resultType, Object content )
            throws URISyntaxException;
    
    /**
     * Creates a new Atmos Subtenant in EMC ViPR.  This operation is not supported on
     * pure Atmos systems.
     * @param request The {@link CreateSubtenantRequest} containing the parameters for
     * the new subtenant.
     * @return The ID of the new subtenant, e.g. "75077194912140aaa95911c237103695"
     */
    String createSubtenant(CreateSubtenantRequest request);
}
