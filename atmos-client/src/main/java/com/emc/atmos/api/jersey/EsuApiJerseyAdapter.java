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
package com.emc.atmos.api.jersey;

import com.emc.atmos.AtmosException;
import com.emc.atmos.api.*;
import com.emc.atmos.api.bean.*;
import com.emc.atmos.api.request.*;
import com.emc.esu.api.Acl;
import com.emc.esu.api.BufferSegment;
import com.emc.esu.api.*;
import com.emc.esu.api.DirectoryEntry;
import com.emc.esu.api.Metadata;
import com.emc.esu.api.ObjectId;
import com.emc.esu.api.ObjectInfo;
import com.emc.esu.api.ObjectMetadata;
import com.emc.esu.api.ObjectPath;
import com.emc.esu.api.ServiceInformation;
import com.emc.esu.api.rest.AbstractEsuRestApi;
import com.emc.util.StreamUtil;
import org.apache.commons.codec.binary.Base64;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.*;
import java.net.URISyntaxException;
import java.util.*;

public class EsuApiJerseyAdapter extends AbstractEsuRestApi {
    private JAXBContext jaxbContext;
    private ThreadLocal<Marshaller> marshaller = new ThreadLocal<Marshaller>();
    private AtmosApi adaptee;

    public EsuApiJerseyAdapter( AtmosConfig config )
            throws URISyntaxException, UnsupportedEncodingException, JAXBException {
        super( config.getEndpoints()[0].getHost(), config.getEndpoints()[0].getPort(), config.getTokenId(),
               new String( Base64.encodeBase64( config.getSecretKey() ), "UTF-8" ) );
        adaptee = new AtmosApiClient( config );
        jaxbContext = JAXBContext.newInstance( com.emc.atmos.api.bean.ObjectInfo.class );
    }

    @Override
    public long calculateServerOffset() {
        return adaptee.calculateServerClockSkew();
    }

    @Override
    public ObjectId createObjectFromStream( Acl acl, MetadataList metadata, InputStream data,
                                            long length, String mimeType ) {
        try {
            CreateObjectRequest request = new CreateObjectRequest();
            request.acl( adaptAcl( acl ) ).userMetadata( adaptMetadata( metadata ) )
                   .content( data ).contentLength( length ).contentType( mimeType );
            return (ObjectId) adaptIdentifier( adaptee.createObject( request ).getObjectId() );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public ObjectId createObjectFromStreamOnPath( ObjectPath path, Acl acl, MetadataList metadata,
                                                  InputStream data, long length, String mimeType ) {
        try {
            CreateObjectRequest request = new CreateObjectRequest();
            request.identifier( adaptIdentifier( path ) ).acl( adaptAcl( acl ) )
                   .userMetadata( adaptMetadata( metadata ) ).content( data ).contentLength( length )
                   .contentType( mimeType );
            return (ObjectId) adaptIdentifier( adaptee.createObject( request ).getObjectId() );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public ObjectId createObjectFromSegment( Acl acl, MetadataList metadata, BufferSegment data,
                                             String mimeType, Checksum checksum ) {
        try {
            CreateObjectRequest request = new CreateObjectRequest();
            request.acl( adaptAcl( acl ) ).userMetadata( adaptMetadata( metadata ) ).content( adaptBuffer( data ) )
                   .contentType( mimeType ).wsChecksum( adaptChecksum( checksum, data ) );
            return (ObjectId) adaptIdentifier( adaptee.createObject( request ).getObjectId() );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public ObjectId createObjectFromSegmentOnPath( ObjectPath path, Acl acl, MetadataList metadata,
                                                   BufferSegment data, String mimeType, Checksum checksum ) {
        try {
            CreateObjectRequest request = new CreateObjectRequest();
            request.identifier( adaptIdentifier( path ) ).acl( adaptAcl( acl ) )
                   .userMetadata( adaptMetadata( metadata ) ).content( adaptBuffer( data ) ).contentType( mimeType )
                   .wsChecksum( adaptChecksum( checksum, data ) );
            return (ObjectId) adaptIdentifier( adaptee.createObject( request ).getObjectId() );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public void updateObjectFromStream( Identifier id, Acl acl, MetadataList metadata, Extent extent,
                                        InputStream data, long length, String mimeType ) {
        try {
            UpdateObjectRequest request = new UpdateObjectRequest();
            request.identifier( adaptIdentifier( id ) ).acl( adaptAcl( acl ) ).userMetadata( adaptMetadata( metadata ) )
                   .range( adaptExtent( extent ) ).content( data ).contentLength( length ).contentType( mimeType );
            adaptee.updateObject( request );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public void updateObjectFromSegment( Identifier id, Acl acl, MetadataList metadata, Extent extent,
                                         BufferSegment data, String mimeType, Checksum checksum ) {
        try {
            UpdateObjectRequest request = new UpdateObjectRequest();
            request.identifier( adaptIdentifier( id ) ).acl( adaptAcl( acl ) ).userMetadata( adaptMetadata( metadata ) )
                   .range( adaptExtent( extent ) ).content( adaptBuffer( data ) ).contentType( mimeType )
                   .wsChecksum( adaptChecksum( checksum, data ) );
            adaptee.updateObject( request );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public void setUserMetadata( Identifier id, MetadataList metadata ) {
        try {
            adaptee.setUserMetadata( adaptIdentifier( id ), adaptMetadata( metadata ) );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public void setAcl( Identifier id, Acl acl ) {
        try {
            adaptee.setAcl( adaptIdentifier( id ), adaptAcl( acl ) );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public void deleteObject( Identifier id ) {
        try {
            adaptee.delete( adaptIdentifier( id ) );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public void deleteVersion( ObjectId id ) {
        try {
            adaptee.deleteVersion( (com.emc.atmos.api.ObjectId) adaptIdentifier( id ) );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public MetadataList getUserMetadata( Identifier id, MetadataTags tags ) {
        try {
            return adaptMetadata( adaptee.getUserMetadata( adaptIdentifier( id ), adaptNames( tags ) ) );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public MetadataList getSystemMetadata( Identifier id, MetadataTags tags ) {
        try {
            return adaptMetadata( adaptee.getSystemMetadata( adaptIdentifier( id ), adaptNames( tags ) ) );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public byte[] readObject( Identifier id, Extent extent, byte[] buffer, Checksum checksum ) {
        try {
            return readObject( adaptIdentifier( id ), extent, buffer, checksum );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public InputStream readObjectStream( Identifier id, Extent extent ) {
        try {
            return adaptee.readObjectStream( adaptIdentifier( id ), adaptExtent( extent ) ).getObject();
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public Acl getAcl( Identifier id ) {
        try {
            return adaptAcl( adaptee.getAcl( adaptIdentifier( id ) ) );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public void deleteUserMetadata( Identifier id, MetadataTags tags ) {
        try {
            adaptee.deleteUserMetadata( adaptIdentifier( id ), adaptNames( tags ) );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public List<Identifier> listVersions( Identifier id ) {
        try {
            List<Identifier> identifiers = new ArrayList<Identifier>();
            List<Version> versions = listVersions( (ObjectId) id, null );
            for ( Version version : versions ) {
                identifiers.add( version.getId() );
            }
            return identifiers;
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public List<Version> listVersions( ObjectId id, ListOptions options ) {
        try {
            List<Version> versions = new ArrayList<Version>();
            ListVersionsRequest request = new ListVersionsRequest();
            request.setObjectId( (com.emc.atmos.api.ObjectId) adaptIdentifier( id ) );
            if ( options != null ) request.limit( options.getLimit() ).token( options.getToken() );
            ListVersionsResponse response = adaptee.listVersions( request );
            if ( response.getVersions() != null ) {
                for ( ObjectVersion version : response.getVersions() ) {
                    versions.add( adaptVersion( version ) );
                }
            }
            if ( options != null ) options.setToken( request.getToken() );
            return versions;
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public ObjectId versionObject( Identifier id ) {
        try {
            return (ObjectId) adaptIdentifier( adaptee.createVersion( adaptIdentifier( id ) ) );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public List<ObjectResult> listObjects( String tag, ListOptions options ) {
        try {
            List<ObjectResult> results = new ArrayList<ObjectResult>();
            ListObjectsRequest request = new ListObjectsRequest().metadataName( tag );
            if ( options != null ) {
                request.limit( options.getLimit() );
                request.token( options.getToken() ).includeMetadata( options.isIncludeMetadata() );
                if ( options.getUserMetadata() != null )
                    request.userMetadataNames( options.getUserMetadata()
                                                      .toArray( new String[options.getUserMetadata().size()] ) );
                if ( options.getSystemMetadata() != null )
                    request.systemMetadataNames( options.getSystemMetadata()
                                                        .toArray( new String[options.getSystemMetadata().size()] ) );
            }
            for ( ObjectEntry entry : adaptee.listObjects( request ).getEntries() ) {
                results.add( adaptObjectEntry( entry ) );
            }
            if ( options != null ) options.setToken( request.getToken() );
            return results;
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public MetadataTags getListableTags( MetadataTag tag ) {
        try {
            return getListableTags( tag.getName() );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public MetadataTags getListableTags( String tag ) {
        try {
            return adaptTags( adaptee.listMetadata( tag ) );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public MetadataTags listUserMetadataTags( Identifier id ) {
        try {
            return adaptTags( adaptee.getUserMetadataNames( adaptIdentifier( id ) ) );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public List<ObjectId> queryObjects( String xquery ) {
        throw new UnsupportedOperationException( "This implementation does not support object query" );
    }

    @Override
    public List<DirectoryEntry> listDirectory( ObjectPath path, ListOptions options ) {
        try {
            List<DirectoryEntry> entries = new ArrayList<DirectoryEntry>();
            ListDirectoryRequest request = new ListDirectoryRequest();
            request.path( (com.emc.atmos.api.ObjectPath) adaptIdentifier( path ) );
            if ( options != null ) {
                request.limit( options.getLimit() ).token( options.getToken() )
                       .includeMetadata( options.isIncludeMetadata() );
                if ( options.getUserMetadata() != null )
                    request.userMetadataNames( options.getUserMetadata()
                                                      .toArray( new String[options.getUserMetadata().size()] ) );
                if ( options.getSystemMetadata() != null )
                    request.systemMetadataNames( options.getSystemMetadata()
                                                        .toArray( new String[options.getSystemMetadata().size()] ) );
            }
            for ( com.emc.atmos.api.bean.DirectoryEntry entry : adaptee.listDirectory( request ).getEntries() ) {
                entries.add( adaptDirectoryEntry( entry, (com.emc.atmos.api.ObjectPath) adaptIdentifier( path ) ) );
            }
            if ( options != null ) options.setToken( request.getToken() );
            return entries;
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public ObjectMetadata getAllMetadata( Identifier id ) {
        try {
            return adaptObjectMetadata( adaptee.getObjectMetadata( adaptIdentifier( id ) ) );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public void rename( ObjectPath source, ObjectPath destination, boolean force ) {
        try {
            adaptee.move( (com.emc.atmos.api.ObjectPath) adaptIdentifier( source ),
                          (com.emc.atmos.api.ObjectPath) adaptIdentifier( destination ),
                          force );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public void restoreVersion( ObjectId id, ObjectId vId ) {
        try {
            adaptee.restoreVersion( (com.emc.atmos.api.ObjectId) adaptIdentifier( id ),
                                    (com.emc.atmos.api.ObjectId) adaptIdentifier( vId ) );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public ServiceInformation getServiceInformation() {
        try {
            return adaptServiceInformation( adaptee.getServiceInformation() );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public ObjectInfo getObjectInfo( Identifier id ) {
        try {
            return adaptObjectInfo( adaptee.getObjectInfo( adaptIdentifier( id ) ) );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        } catch ( JAXBException e ) {
            throw new RuntimeException( "Could not marshall result to XML", e );
        }
    }

    @Override
    public void hardLink( ObjectPath source, ObjectPath target ) {
        throw new UnsupportedOperationException( "This implementation does not support hard link" );
    }

    @Override
    public ObjectId createObjectWithKeyFromSegment( String keyPool, String key, Acl acl, MetadataList metadata,
                                                    BufferSegment data, String mimeType, Checksum checksum ) {
        try {
            CreateObjectRequest request = new CreateObjectRequest().identifier( new ObjectKey( keyPool, key ) );
            request.acl( adaptAcl( acl ) ).userMetadata( adaptMetadata( metadata ) ).content( adaptBuffer( data ) );
            request.contentType( mimeType ).wsChecksum( adaptChecksum( checksum, data ) );
            return (ObjectId) adaptIdentifier( adaptee.createObject( request ).getObjectId() );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public ObjectId createObjectWithKeyFromStream( String keyPool, String key, Acl acl, MetadataList metadata,
                                                   InputStream data, long length, String mimeType ) {
        try {
            CreateObjectRequest request = new CreateObjectRequest().identifier( new ObjectKey( keyPool, key ) );
            request.acl( adaptAcl( acl ) ).userMetadata( adaptMetadata( metadata ) ).content( data );
            request.contentLength( length ).contentType( mimeType );
            return (ObjectId) adaptIdentifier( adaptee.createObject( request ).getObjectId() );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public void deleteObjectWithKey( String keyPool, String key ) {
        try {
            adaptee.delete( new ObjectKey( keyPool, key ) );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public ObjectMetadata getAllMetadata( String keyPool, String key ) {
        try {
            return adaptObjectMetadata( adaptee.getObjectMetadata( new ObjectKey( keyPool, key ) ) );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public MetadataList getSystemMetadata( String keyPool, String key, MetadataTags tags ) {
        try {
            return adaptMetadata( adaptee.getSystemMetadata( new ObjectKey( keyPool, key ), adaptNames( tags ) ) );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public byte[] readObjectWithKey( String keyPool, String key, Extent extent, byte[] buffer, Checksum checksum ) {
        try {
            return readObject( new ObjectKey( keyPool, key ), extent, buffer, checksum );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public InputStream readObjectStreamWithKey( String keyPool, String key, Extent extent ) {
        try {
            return adaptee.readObjectStream( new ObjectKey( keyPool, key ), adaptExtent( extent ) ).getObject();
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public void updateObjectWithKeyFromStream( String keyPool, String key, Acl acl, MetadataList metadata,
                                               Extent extent, InputStream data, long length, String mimeType ) {
        try {
            UpdateObjectRequest request = new UpdateObjectRequest();
            request.identifier( new ObjectKey( keyPool, key ) ).acl( adaptAcl( acl ) )
                   .userMetadata( adaptMetadata( metadata ) ).range( adaptExtent( extent ) ).content( data )
                   .contentLength( length ).contentType( mimeType );
            adaptee.updateObject( request );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    @Override
    public void updateObjectWithKeyFromSegment( String keyPool, String key, Acl acl, MetadataList metadata,
                                                Extent extent, BufferSegment data, String mimeType,
                                                Checksum checksum ) {
        try {
            UpdateObjectRequest request = new UpdateObjectRequest();
            request.identifier( new ObjectKey( keyPool, key ) ).acl( adaptAcl( acl ) )
                   .userMetadata( adaptMetadata( metadata ) ).range( adaptExtent( extent ) )
                   .content( adaptBuffer( data ) ).contentType( mimeType )
                   .wsChecksum( adaptChecksum( checksum, data ) );
            adaptee.updateObject( request );
        } catch ( AtmosException e ) {
            throw adaptException( e );
        }
    }

    private byte[] readObject( ObjectIdentifier identifier, Extent extent, byte[] buffer, Checksum checksum ) {
        try {
            if ( buffer != null && extent != null && extent.getSize() > (long) buffer.length ) {
                throw new IllegalArgumentException( "The buffer is smaller than the requested extent" );
            }

            ReadObjectResponse<InputStream> response = adaptee.readObjectStream( identifier, adaptExtent( extent ) );

            long contentLength = response.getContentLength();
            if ( buffer != null && contentLength > -1 ) {
                if ( (long) buffer.length < contentLength ) {
                    throw new EsuException( "The response buffer was not long enough to hold the response: "
                                            + buffer.length + "<" + contentLength );
                }
                int read, c = 0;
                InputStream in = response.getObject();
                while ( c < contentLength ) {
                    read = in.read( buffer, c, (int) contentLength - c );
                    if ( read == -1 ) {
                        throw new EOFException(
                                "EOF reading response at position " + c + " size " + (contentLength - c) );
                    }
                    c += read;
                }
            } else {
                if ( contentLength > Integer.MAX_VALUE )
                    throw new EsuException( "Object is too large to hold in a byte array" );
                buffer = StreamUtil.readAsBytes( response.getObject() );
            }

            if ( checksum != null && response.getWsChecksum() != null ) {
                checksum.setExpectedValue( response.getWsChecksum().toString() );
                if ( contentLength > -1 ) checksum.update( buffer, 0, (int) contentLength );
                else checksum.update( buffer, 0, buffer.length );
            }

            return buffer;
        } catch ( IOException e ) {
            throw new EsuException( "Error connecting to server", e );
        }
    }

    private ObjectIdentifier adaptIdentifier( Identifier identifier ) {
        if ( identifier == null ) return null;
        if ( identifier instanceof ObjectId ) return new com.emc.atmos.api.ObjectId( identifier.toString() );
        if ( identifier instanceof ObjectPath ) return new com.emc.atmos.api.ObjectPath( identifier.toString() );
        throw new RuntimeException(
                "Unable to convert identifier " + identifier + " (" + identifier.getClass().getName() + ")" );
    }

    private Identifier adaptIdentifier( ObjectIdentifier identifier ) {
        if ( identifier == null ) return null;
        if ( identifier instanceof com.emc.atmos.api.ObjectId ) return new ObjectId( identifier.toString() );
        if ( identifier instanceof com.emc.atmos.api.ObjectPath ) return new ObjectPath( identifier.toString() );
        throw new RuntimeException(
                "Unable to convert identifier " + identifier + " (" + identifier.getClass().getName() + ")" );
    }

    private com.emc.atmos.api.Acl adaptAcl( Acl acl ) {
        if ( acl == null ) return null;
        com.emc.atmos.api.Acl newAcl = new com.emc.atmos.api.Acl();
        for ( Grant grant : acl ) {
            switch ( grant.getGrantee().getType() ) {
                case GROUP:
                    newAcl.addGroupGrant( grant.getGrantee().getName(),
                                          com.emc.atmos.api.bean.Permission.valueOf( grant.getPermission() ) );
                    break;
                case USER:
                    newAcl.addUserGrant( grant.getGrantee().getName(),
                                         com.emc.atmos.api.bean.Permission.valueOf( grant.getPermission() ) );
                    break;
            }
        }
        return newAcl;
    }

    private Acl adaptAcl( com.emc.atmos.api.Acl acl ) {
        if ( acl == null ) return null;
        Acl newAcl = new Acl();
        for ( String name : acl.getGroupAcl().keySet() ) {
            newAcl.addGrant( new Grant( new Grantee( name, Grantee.GRANT_TYPE.GROUP ),
                                        acl.getGroupAcl().get( name ).toString() ) );
        }
        for ( String name : acl.getUserAcl().keySet() ) {
            newAcl.addGrant( new Grant( new Grantee( name, Grantee.GRANT_TYPE.USER ),
                                        acl.getUserAcl().get( name ).toString() ) );
        }
        return newAcl;
    }

    private com.emc.atmos.api.bean.Metadata[] adaptMetadata( MetadataList metadataList ) {
        if ( metadataList == null ) return null;
        List<com.emc.atmos.api.bean.Metadata> newMetadata = new ArrayList<com.emc.atmos.api.bean.Metadata>();
        for ( Metadata metadata : metadataList ) {
            newMetadata.add( new com.emc.atmos.api.bean.Metadata( metadata.getName(),
                                                                  metadata.getValue(),
                                                                  metadata.isListable() ) );
        }
        return newMetadata.toArray( new com.emc.atmos.api.bean.Metadata[newMetadata.size()] );
    }

    private MetadataList adaptMetadata( Map<String, com.emc.atmos.api.bean.Metadata> metadataMap ) {
        if ( metadataMap == null ) return null;
        MetadataList metadataList = new MetadataList();
        for ( com.emc.atmos.api.bean.Metadata oneMetadata : metadataMap.values() ) {
            metadataList.addMetadata( new Metadata( oneMetadata.getName(),
                                                    oneMetadata.getValue(),
                                                    oneMetadata.isListable() ) );
        }
        return metadataList;
    }

    private String[] adaptNames( MetadataTags tags ) {
        if ( tags == null ) return null;
        List<String> names = new ArrayList<String>();
        for ( MetadataTag tag : tags ) {
            names.add( tag.getName() );
        }
        return names.toArray( new String[names.size()] );
    }

    private MetadataTags adaptTags( Set<String> names ) {
        if ( names == null ) return null;
        MetadataTags newTags = new MetadataTags();
        for ( String name : names ) {
            newTags.addTag( new MetadataTag( name, false ) );
        }
        return newTags;
    }

    private MetadataTags adaptTags( Map<String, Boolean> tags ) {
        if ( tags == null ) return null;
        MetadataTags newTags = new MetadataTags();
        for ( String name : tags.keySet() ) {
            newTags.addTag( new MetadataTag( name, tags.get( name ) ) );
        }
        return newTags;
    }

    private Range adaptExtent( Extent extent ) {
        if ( extent == null ) return null;
        return new Range( extent.getOffset(), extent.getOffset() + extent.getSize() - 1 );
    }

    private com.emc.atmos.api.BufferSegment adaptBuffer( BufferSegment bufferSegment ) {
        if ( bufferSegment == null ) return null;
        return new com.emc.atmos.api.BufferSegment( bufferSegment.getBuffer(),
                                                    bufferSegment.getOffset(),
                                                    bufferSegment.getSize() );
    }

    private ChecksumValue adaptChecksum( Checksum checksum, BufferSegment bufferSegment ) {
        if ( checksum == null ) return null;
        checksum.update( bufferSegment.getBuffer(), bufferSegment.getOffset(), bufferSegment.getSize() );
        return new ChecksumValueImpl( checksum.toString() );
    }

    private Version adaptVersion( ObjectVersion version ) {
        if ( version == null ) return null;
        return new Version( (ObjectId) adaptIdentifier( version.getVersionId() ),
                            version.getVersionNumber(),
                            version.getItime() );
    }

    private ObjectResult adaptObjectEntry( ObjectEntry entry ) {
        if ( entry == null ) return null;
        ObjectResult result = new ObjectResult();
        result.setId( (ObjectId) adaptIdentifier( entry.getObjectId() ) );
        Map<String, com.emc.atmos.api.bean.Metadata> allMetadata = new TreeMap<String, com.emc.atmos.api.bean.Metadata>();
        if ( entry.getSystemMetadata() != null ) allMetadata.putAll( entry.getSystemMetadataMap() );
        if ( entry.getUserMetadata() != null ) allMetadata.putAll( entry.getUserMetadataMap() );
        if ( !allMetadata.isEmpty() ) result.setMetadata( adaptMetadata( allMetadata ) );
        return result;
    }

    private DirectoryEntry adaptDirectoryEntry( com.emc.atmos.api.bean.DirectoryEntry entry,
                                                com.emc.atmos.api.ObjectPath parentPath ) {
        if ( entry == null ) return null;
        String path = parentPath.getPath() + entry.getFilename();
        if ( entry.isDirectory() ) path += "/";
        DirectoryEntry newEntry = new DirectoryEntry();
        newEntry.setId( (ObjectId) adaptIdentifier( entry.getObjectId() ) );
        newEntry.setPath( new ObjectPath( path ) );
        newEntry.setType( entry.getFileType().toString() );
        newEntry.setSystemMetadata( adaptMetadata( entry.getSystemMetadataMap() ) );
        newEntry.setUserMetadata( adaptMetadata( entry.getUserMetadataMap() ) );
        return newEntry;
    }

    private ObjectMetadata adaptObjectMetadata( com.emc.atmos.api.bean.ObjectMetadata objectMetadata ) {
        if ( objectMetadata == null ) return null;
        ObjectMetadata newMetadata = new ObjectMetadata();
        newMetadata.setAcl( adaptAcl( objectMetadata.getAcl() ) );
        newMetadata.setMimeType( objectMetadata.getContentType() );
        newMetadata.setMetadata( adaptMetadata( objectMetadata.getMetadata() ) );
        return newMetadata;
    }

    private ObjectInfo adaptObjectInfo( com.emc.atmos.api.bean.ObjectInfo objectInfo ) throws JAXBException {
        if ( objectInfo == null ) return null;

        ObjectInfo newObjectInfo = new ObjectInfo();
        newObjectInfo.setObjectId( (ObjectId) adaptIdentifier( objectInfo.getObjectId() ) );
        newObjectInfo.setSelection( objectInfo.getSelection() );

        if ( objectInfo.getExpiration() != null ) {
            ObjectExpiration objectExpiration = new ObjectExpiration();
            objectExpiration.setEnabled( objectInfo.getExpiration().isEnabled() );
            objectExpiration.setEndAt( objectInfo.getExpiration().getEndAt() );
            newObjectInfo.setExpiration( objectExpiration );
        }

        if ( objectInfo.getRetention() != null ) {
            ObjectRetention objectRetention = new ObjectRetention();
            objectRetention.setEnabled( objectInfo.getRetention().isEnabled() );
            objectRetention.setEndAt( objectInfo.getRetention().getEndAt() );
            newObjectInfo.setRetention( objectRetention );
        }

        if ( objectInfo.getReplicas() != null ) {
            List<ObjectReplica> replicas = new ArrayList<ObjectReplica>();
            for ( Replica replica : objectInfo.getReplicas() ) {
                ObjectReplica newReplica = new ObjectReplica();
                newReplica.setId( "" + replica.getId() );
                newReplica.setCurrent( replica.isCurrent() );
                newReplica.setLocation( replica.getLocation() );
                newReplica.setReplicaType( replica.getType() );
                newReplica.setStorageType( replica.getStorageType() );
                replicas.add( newReplica );
            }
            newObjectInfo.setReplicas( replicas );
        }

        StringWriter xmlString = new StringWriter();
        getMarshaller().marshal( objectInfo, xmlString );
        newObjectInfo.setRawXml( xmlString.toString() );

        return newObjectInfo;
    }

    private ServiceInformation adaptServiceInformation( com.emc.atmos.api.bean.ServiceInformation serviceInformation ) {
        if ( serviceInformation == null ) return null;
        ServiceInformation newServiceInformation = new ServiceInformation();
        newServiceInformation.setAtmosVersion( serviceInformation.getAtmosVersion() );
        newServiceInformation.setUnicodeMetadataSupported( serviceInformation.hasFeature( com.emc.atmos.api.bean.ServiceInformation.Feature.Utf8 ) );
        for ( com.emc.atmos.api.bean.ServiceInformation.Feature feature : serviceInformation.getFeatures() ) {
            newServiceInformation.addFeature( feature.getHeaderName() );
        }
        return newServiceInformation;
    }

    private EsuException adaptException( AtmosException e ) {
        return new EsuException( e.getMessage(), e.getCause(), e.getHttpCode(), e.getErrorCode() );
    }

    private Marshaller getMarshaller() throws JAXBException {
        Marshaller m = marshaller.get();
        if ( m == null ) {
            m = jaxbContext.createMarshaller();
            marshaller.set( m );
        }
        return m;
    }
}
