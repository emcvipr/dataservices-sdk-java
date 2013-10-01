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
package com.emc.atmos.api.request;

import com.emc.atmos.api.*;
import com.emc.atmos.api.bean.Metadata;

import java.util.*;

/**
 * Represents a write object request (create or update).
 *
 * @param <T> Represents the implementation type. Allows a consistent builder interface throughout the request
 *            hierarchy. Parameterize concrete subclasses with their own type and implement {@link #me()} to return
 *            "this". In abstract subclasses, return me() in builder methods.
 */
public abstract class PutObjectRequest<T extends PutObjectRequest<T>> extends ObjectRequest<T>
        implements ContentRequest {
    protected Object content;
    protected long contentLength;
    protected String contentType;
    protected Acl acl;
    protected Map<String, Metadata> userMetadata;
    protected ChecksumValue wsChecksum;
    private ChecksumAlgorithm serverGeneratedChecksumAlgorithm;

    public PutObjectRequest() {
        userMetadata = new TreeMap<String, Metadata>();
    }

    @Override
    public Map<String, List<Object>> generateHeaders() {
        Map<String, List<Object>> headers = super.generateHeaders();

        // enable UTF-8
        if ( !getUserMetadata().isEmpty() ) RestUtil.addValue( headers, RestUtil.XHEADER_UTF8, "true" );

        // metadata
        for ( Metadata metadata : getUserMetadata() ) {
            if ( metadata.isListable() )
                RestUtil.addValue( headers, RestUtil.XHEADER_LISTABLE_META, metadata.toASCIIString() );
            else
                RestUtil.addValue( headers, RestUtil.XHEADER_META, metadata.toASCIIString() );
        }

        // acl
        if ( acl != null ) {
            headers.put( RestUtil.XHEADER_USER_ACL, acl.getUserAclHeader() );
            headers.put( RestUtil.XHEADER_GROUP_ACL, acl.getGroupAclHeader() );
        }

        // wschecksum
        if ( wsChecksum != null ) {
            RestUtil.addValue( headers, RestUtil.XHEADER_WSCHECKSUM, wsChecksum );
        }

        // server-generated checksum
        if ( serverGeneratedChecksumAlgorithm != null )
            RestUtil.addValue( headers, RestUtil.XHEADER_GENERATE_CHECKSUM, serverGeneratedChecksumAlgorithm );

        return headers;
    }

    /**
     * Non-directory object writes support the Expect: 100-continue feature.
     */
    @Override
    public boolean supports100Continue() {
        if ( getIdentifier() instanceof ObjectPath && ((ObjectPath) getIdentifier()).isDirectory() ) return false;
        return true;
    }

    /**
     * Builder method for {@link #setContent(Object)}
     */
    public T content( Object content ) {
        setContent( content );
        return me();
    }

    /**
     * Builder method for {@link #setContentLength(long)}
     */
    public T contentLength( long contentLength ) {
        setContentLength( contentLength );
        return me();
    }

    /**
     * Builder method for {@link #setContentType(String)}
     */
    public T contentType( String contentType ) {
        setContentType( contentType );
        return me();
    }

    /**
     * Builder method for {@link #setAcl(com.emc.atmos.api.Acl)}
     */
    public T acl( Acl acl ) {
        setAcl( acl );
        return me();
    }

    /**
     * Builder method for {@link #setUserMetadata(java.util.Collection)}
     */
    public T userMetadata( Metadata... userMetadata ) {
        if ( userMetadata == null || (userMetadata.length == 1 && userMetadata[0] == null) )
            userMetadata = new Metadata[0];
        setUserMetadata( Arrays.asList( userMetadata ) );
        return me();
    }

    /**
     * Builder method for {@link #setWsChecksum(com.emc.atmos.api.ChecksumValue)}
     */
    public T wsChecksum( ChecksumValue wsChecksum ) {
        setWsChecksum( wsChecksum );
        return me();
    }

    /**
     * Builder method for {@link #setServerGeneratedChecksumAlgorithm(com.emc.atmos.api.ChecksumAlgorithm)}
     */
    public T serverGeneratedChecksumAlgorithm( ChecksumAlgorithm serverGeneratedChecksumAlgorithm ) {
        setServerGeneratedChecksumAlgorithm( serverGeneratedChecksumAlgorithm );
        return me();
    }

    /**
     * Returns the object content for this request.
     */
    public Object getContent() {
        return content;
    }

    /**
     * Returns the content-length (byte size) for this request.
     */
    public long getContentLength() {
        return contentLength;
    }

    /**
     * Returns the content-type for this request.
     */
    public String getContentType() {
        return contentType;
    }

    /**
     * Returns the ACL to assign to the object.
     */
    public Acl getAcl() {
        return acl;
    }

    /**
     * Returns the user metadata to assign to the object.
     */
    public Set<Metadata> getUserMetadata() {
        return new HashSet<Metadata>( userMetadata.values() );
    }

    /**
     * Returns the ws-checksum value to use in this request.
     */
    public ChecksumValue getWsChecksum() {
        return wsChecksum;
    }

    /**
     * Gets the algorithm the Atmos server should use to generate a checksum for the content in this request.
     */
    public ChecksumAlgorithm getServerGeneratedChecksumAlgorithm() {
        return serverGeneratedChecksumAlgorithm;
    }

    /**
     * Sets the object content for this request.
     */
    public void setContent( Object content ) {
        this.content = content;
    }

    /**
     * Sets the content-length (byte size) of the object content.
     */
    public void setContentLength( long contentLength ) {
        this.contentLength = contentLength;
    }

    /**
     * Sets the content-type for this request.
     */
    public void setContentType( String contentType ) {
        this.contentType = contentType;
    }

    /**
     * Sets the ACL to assign to the object.
     */
    public void setAcl( Acl acl ) {
        this.acl = acl;
    }

    /**
     * Sets the user metadata to assign to the object. Note that this can only add or modify metadata on update
     * operations. You must use {@link com.emc.atmos.api.AtmosApi#deleteUserMetadata(com.emc.atmos.api.ObjectIdentifier,
     * String...)} to remove metadata from an object.
     */
    public void setUserMetadata( Collection<Metadata> userMetadata ) {
        this.userMetadata.clear();
        for ( Metadata metadata : userMetadata ) {
            this.userMetadata.put( metadata.getName(), metadata );
        }
    }

    /**
     * Sets the ws-checksum value to use for this request. Note that although a RunningChecksum may be provided here,
     * this is a ChecksumValue. Checksums are not automatically updated as they were in EsuApi. The value provided here
     * must reflect what will be sent over the wire (you must manually update your checksums).
     *
     * @see com.emc.atmos.api.ChecksumValue
     * @see com.emc.atmos.api.RunningChecksum
     */
    public void setWsChecksum( ChecksumValue wsChecksum ) {
        this.wsChecksum = wsChecksum;
    }

    /**
     * Sets the algorithm the Atmos server should use to generate a checksum for the content in this request. Note that
     * although this value will be returned in GET requests for the object, it is not maintained by the server and it
     * only represents the checksum for the content in the last write request on the object. If the last write request
     * was a range or if this flag was not set, the checksum is invalid. It is best to ignore the persisted value and
     * only use this feature to verify each write request individually.
     */
    public void setServerGeneratedChecksumAlgorithm( ChecksumAlgorithm serverGeneratedChecksumAlgorithm ) {
        this.serverGeneratedChecksumAlgorithm = serverGeneratedChecksumAlgorithm;
    }
}
