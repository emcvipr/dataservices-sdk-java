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
import com.emc.atmos.api.multipart.MultipartEntity;
import com.emc.atmos.api.request.*;
import com.emc.util.HttpUtil;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.ClientFilter;

import org.apache.log4j.Logger;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Reference implementation of AtmosApi.
 * <p/>
 * This implementation uses the JAX-RS reference implementation (Jersey) as it's REST client.  When sending or
 * receiving data, the following content handlers are supported by default.  You may add your own
 * MessageBodyReader/MessageBodyWriter implementations by using the optional constructor, however be sure to return a
 * positive value from MessageBodyWriter.getSize() as this sets the content-length of the request, which is required by
 * Atmos.  Be sure to use the appropriate content-type associated with each object type or the handlers will not
 * understand the request.
 * <p/>
 * <table>
 * <tr><th>Object Type (class)</th><th>Expected Content-Type(s)</th></tr>
 * <tr><td>byte[]</td><td>*any*</td></tr>
 * <tr><td>java.lang.String</td><td>*any*</td></tr>
 * <tr><td>java.io.File (send-only)</td><td>*any*</td></tr>
 * <tr><td>java.io.InputStream (send-only)</td><td>*any*</td></tr>
 * <tr><td>com.emc.atmos.api.BufferSegment (send-only)</td><td>*any*</td></tr>
 * <tr><td>any annotated JAXB root element bean</td><td>text/xml, application/xml</td></tr>
 * <tr><td>com.emc.atmos.api.multipart.MultipartEntity (receive-only)</td><td>multipart/*</td></tr>
 * </table>
 * <p/>
 * Also keep in mind that you can always send/receive byte[] and do your own conversion as that has always been
 * supported.
 * <p/>
 * Note: this implementation is <b>not</b> supported with Atmos versions below 1.4.2.
 * <p/>
 * To use, simply pass a new AtmosConfig object to the constructor like so:
 * <pre>
 *     URI atmosEndpoint = new URI( "http://api.atmosonline.com" ); // or use your private cloud endpoint/load balancer
 *     AtmosApi atmos = new AtmosApiClient( new AtmosConfig( "my_full_token_id", "my_secret_key", atmosEndpoint ) );
 * </pre>
 * <p/>
 * You can also specify multiple endpoints and have each request round-robin between them like so:
 * <pre>
 *     URI endpoint1 = new URI( "https://10.0.0.101" ); // 1st Atmos node
 *     URI endpoint2 = new URI( "https://10.0.0.102" ); // 2nd Atmos node
 *     URI endpoint3 = new URI( "https://10.0.0.103" ); // 3rd Atmos node
 *     URI endpoint4 = new URI( "https://10.0.0.104" ); // 4th Atmos node
 *     AtmosApi atmos = new AtmosApiClient( new AtmosConfig( "my_full_token_id", "my_secret_key",
 *                                          endpoint1, endpoint2, endpoint3, endpoint4 ) );
 * </pre>
 * <p/>
 * To create an object, simply pass the object in to one of the createObject methods. The object type must be one of
 * the supported types above.
 * <pre>
 *     String stringContent = "Hello World!";
 *     ObjectId oid1 = atmos.createObject( stringContent, "text/plain" );
 *
 *     File fileContent = new File( "spreadsheet.xls" );
 *     ObjectId oid2 = atmos.createObject( fileContent, "application/vnd.ms-excel" );
 *
 *     byte[] binaryContent;
 *     ... // load binary content to store as an object
 *     ObjectId oid3 = atmos.createObject( binaryContent, null ); // default content-type is application/octet-stream
 * </pre>
 * <p/>
 * To read an object, specify the type of object you want to receive from a readObject method. The same rules apply to
 * this type.
 * <pre>
 *     String stringContent = atmos.readObject( oid1, String.class );
 *
 *     byte[] fileContent = atmos.readObject( oid2, byte[].class );
 *     // do something with file content (stream to client? save in local filesystem?)
 *
 *     byte[] binaryContent = atmos.readObject( oid3, byte[].class );
 * </pre>
 */
public class AtmosApiClient extends AbstractAtmosApi {
    private static final Logger l4j = Logger.getLogger( AtmosApiClient.class );

    protected Client client;
    protected Client client100;

    public AtmosApiClient( AtmosConfig config ) {
        this( config, (List<Class<MessageBodyReader<?>>>) null, null );
    }

    public AtmosApiClient( AtmosConfig config,
                           List<Class<MessageBodyReader<?>>> readers,
                           List<Class<MessageBodyWriter<?>>> writers ) {
        this( config,
              JerseyApacheUtil.createApacheClient( config, false, readers, writers ),
              JerseyApacheUtil.createApacheClient( config, true, readers, writers ) );
    }

    protected AtmosApiClient( AtmosConfig config, Client client, Client client100 ) {
        super( config );
        this.client = client;

        // without writing our own client implementation, the only way to discriminate requests that enable
        // Expect: 100-continue behavior is to have two clients; one with the feature enabled and one without.
        this.client100 = client100;
    }

    /**
     * Adds a ClientFilter to the Jersey client used to make REST requests.  Useful to provide your own in-line content
     * or header manipulation.
     */
    public void addClientFilter( ClientFilter filter ) {
        client.addFilter( filter );
        client100.addFilter( filter );
    }

    @Override
    public ServiceInformation getServiceInformation() {
        ClientResponse response = client.resource( config.resolvePath( "service", null ) ).get( ClientResponse.class );
        ServiceInformation serviceInformation = response.getEntity( ServiceInformation.class );

        String featureString = response.getHeaders().getFirst( RestUtil.XHEADER_FEATURES );
        if ( featureString != null ) {
            for ( String feature : featureString.split( "," ) )
                serviceInformation.addFeatureFromHeaderName( feature.trim() );
        }

        // legacy
        String utf8String = response.getHeaders().getFirst( RestUtil.XHEADER_SUPPORT_UTF8 );
        if ( utf8String != null && Boolean.valueOf( utf8String ) )
            serviceInformation.addFeature( ServiceInformation.Feature.Utf8 );

        response.close();

        return serviceInformation;
    }

    @Override
    public long calculateServerClockSkew() {
        ClientResponse response = client.resource( config.resolvePath( "", null ) ).get( ClientResponse.class );

        if ( response.getResponseDate() == null )
            throw new AtmosException( "Response date is null", response.getStatus() );

        config.setServerClockSkew( System.currentTimeMillis() - response.getResponseDate().getTime() );

        response.close();

        return config.getServerClockSkew();
    }

    @Override
    public CreateObjectResponse createObject( CreateObjectRequest request ) {
        ClientResponse response = build( request ).post( ClientResponse.class, getContent( request ) );

        response.close();

        return fillResponse( new CreateObjectResponse(), response );
    }

    @Override
    public <T> ReadObjectResponse<T> readObject( ReadObjectRequest request, Class<T> objectType ) throws IOException {
        if ( request.getRanges() != null && request.getRanges().size() > 1
             && !MultipartEntity.class.isAssignableFrom( objectType ) )
            l4j.warn( "multiple ranges imply a multi-part response. you should ask for MultipartEntity instead of " +
                      objectType.getSimpleName() );

        ClientResponse response = build( request ).get( ClientResponse.class );
        ReadObjectResponse<T> ret = new ReadObjectResponse<T>( response.getEntity( objectType ) );

        response.close();

        return fillResponse( ret, response );
    }

    @Override
    public ReadObjectResponse<InputStream> readObjectStream( ObjectIdentifier identifier, Range range ) {
        ClientResponse response = build( new ReadObjectRequest().identifier( identifier ).ranges( range ) )
                .get( ClientResponse.class );
        return fillResponse( new ReadObjectResponse<InputStream>( response.getEntityInputStream() ), response );
    }

    @Override
    public BasicResponse updateObject( UpdateObjectRequest request ) {
        ClientResponse response = build( request ).put( ClientResponse.class, getContent( request ) );

        response.close();

        return fillResponse( new BasicResponse(), response );
    }

    @Override
    public void delete( ObjectIdentifier identifier ) {
        URI uri = config.resolvePath( identifier.getRelativeResourcePath(), null );
        WebResource.Builder builder = client.resource( uri ).getRequestBuilder();

        if ( identifier instanceof ObjectKey )
            builder.header( RestUtil.XHEADER_POOL, ((ObjectKey) identifier).getBucket() );

        builder.delete();
    }

    @Override
    public ObjectId createDirectory( ObjectPath path ) {
        if ( !path.isDirectory() ) throw new AtmosException( "Path must be a directory" );

        CreateObjectRequest request = new CreateObjectRequest().identifier( path );

        ClientResponse response = build( request ).post( ClientResponse.class );

        response.close();

        return RestUtil.parseObjectId( response.getLocation().getPath() );
    }

    @Override
    public ObjectId createDirectory( ObjectPath path, Acl acl, Metadata... metadata ) {
        if ( !path.isDirectory() ) throw new AtmosException( "Path must be a directory" );

        CreateObjectRequest request = new CreateObjectRequest().identifier( path ).acl( acl );
        request.userMetadata( metadata );

        ClientResponse response = build( request ).post( ClientResponse.class );

        response.close();

        return RestUtil.parseObjectId( response.getLocation().getPath() );
    }

    @Override
    public ListDirectoryResponse listDirectory( ListDirectoryRequest request ) {
        if ( !request.getPath().isDirectory() ) throw new AtmosException( "Path must be a directory" );

        ClientResponse response = build( request ).get( ClientResponse.class );

        request.setToken( response.getHeaders().getFirst( RestUtil.XHEADER_TOKEN ) );
        if ( request.getToken() != null )
            l4j.info( "Results truncated. Call listDirectory again for next page of results." );

        ListDirectoryResponse ret = response.getEntity( ListDirectoryResponse.class );

        response.close();

        return fillResponse( ret, response );
    }

    @Override
    public void move( ObjectPath oldPath, ObjectPath newPath, boolean overwrite ) {
        WebResource resource = client.resource( config.resolvePath( oldPath.getRelativeResourcePath(), "rename" ) );
        WebResource.Builder builder = resource.getRequestBuilder();
        builder.header( RestUtil.XHEADER_UTF8, "true" )
               .header( RestUtil.XHEADER_PATH, HttpUtil.encodeUtf8( newPath.getPath() ) );
        if ( overwrite ) builder.header( RestUtil.XHEADER_FORCE, "true" );

        // workaround for clients that set a default content-type for POSTs
        builder.type( RestUtil.TYPE_DEFAULT );
        builder.post();
    }

    @Override
    public Map<String, Boolean> getUserMetadataNames( ObjectIdentifier identifier ) {
        URI uri = config.resolvePath( identifier.getRelativeResourcePath(), "metadata/tags" );
        WebResource.Builder builder = client.resource( uri ).getRequestBuilder();

        if ( identifier instanceof ObjectKey )
            builder.header( RestUtil.XHEADER_POOL, ((ObjectKey) identifier).getBucket() );

        ClientResponse response = builder.header( RestUtil.XHEADER_UTF8, "true" ).get( ClientResponse.class );

        Map<String, Boolean> metaNames = new TreeMap<String, Boolean>();

        String nameString = response.getHeaders().getFirst( RestUtil.XHEADER_TAGS );
        if ( nameString != null ) {
            for ( String name : nameString.split( "," ) )
                metaNames.put( HttpUtil.decodeUtf8( name.trim() ), false );
        }

        nameString = response.getHeaders().getFirst( RestUtil.XHEADER_LISTABLE_TAGS );
        if ( nameString != null ) {
            for ( String name : nameString.split( "," ) )
                metaNames.put( HttpUtil.decodeUtf8( name.trim() ), true );
        }

        response.close();

        return metaNames;
    }

    @Override
    public Map<String, Metadata> getUserMetadata( ObjectIdentifier identifier, String... metadataNames ) {
        URI uri = config.resolvePath( identifier.getRelativeResourcePath(), "metadata/user" );
        WebResource.Builder builder = client.resource( uri ).getRequestBuilder();

        if ( identifier instanceof ObjectKey )
            builder.header( RestUtil.XHEADER_POOL, ((ObjectKey) identifier).getBucket() );

        if ( metadataNames != null ) {
            for ( String name : metadataNames ) {
                builder.header( RestUtil.XHEADER_TAGS, HttpUtil.encodeUtf8( name ) );
            }
        }

        ClientResponse response = builder.header( RestUtil.XHEADER_UTF8, "true" ).get( ClientResponse.class );

        Map<String, Metadata> metaMap = new TreeMap<String, Metadata>();
        metaMap.putAll( RestUtil.parseMetadataHeader( response.getHeaders().getFirst( RestUtil.XHEADER_META ),
                                                      false ) );
        metaMap.putAll( RestUtil.parseMetadataHeader( response.getHeaders().getFirst( RestUtil.XHEADER_LISTABLE_META ),
                                                      true ) );

        response.close();

        return metaMap;
    }

    @Override
    public Map<String, Metadata> getSystemMetadata( ObjectIdentifier identifier, String... metadataNames ) {
        URI uri = config.resolvePath( identifier.getRelativeResourcePath(), "metadata/system" );
        WebResource.Builder builder = client.resource( uri ).getRequestBuilder();

        if ( identifier instanceof ObjectKey )
            builder.header( RestUtil.XHEADER_POOL, ((ObjectKey) identifier).getBucket() );

        if ( metadataNames != null ) {
            for ( String name : metadataNames ) {
                builder.header( RestUtil.XHEADER_TAGS, HttpUtil.encodeUtf8( name ) );
            }
        }

        ClientResponse response = builder.header( RestUtil.XHEADER_UTF8, "true" ).get( ClientResponse.class );

        response.close();

        return RestUtil.parseMetadataHeader( response.getHeaders().getFirst( RestUtil.XHEADER_META ), false );
    }

    @Override
    public ObjectMetadata getObjectMetadata( ObjectIdentifier identifier ) {
        URI uri = config.resolvePath( identifier.getRelativeResourcePath(), null );
        WebResource.Builder builder = client.resource( uri ).getRequestBuilder();

        if ( identifier instanceof ObjectKey )
            builder.header( RestUtil.XHEADER_POOL, ((ObjectKey) identifier).getBucket() );

        ClientResponse response = builder.header( RestUtil.XHEADER_UTF8, "true" ).head();

        Acl acl = new Acl( RestUtil.parseAclHeader( response.getHeaders().getFirst( RestUtil.XHEADER_USER_ACL ) ),
                           RestUtil.parseAclHeader( response.getHeaders()
                                                            .getFirst( RestUtil.XHEADER_GROUP_ACL ) ) );

        Map<String, Metadata> metaMap = new TreeMap<String, Metadata>();
        metaMap.putAll( RestUtil.parseMetadataHeader( response.getHeaders().getFirst( RestUtil.XHEADER_META ),
                                                      false ) );
        metaMap.putAll( RestUtil.parseMetadataHeader( response.getHeaders()
                                                              .getFirst( RestUtil.XHEADER_LISTABLE_META ),
                                                      true ) );

        response.close();

        return new ObjectMetadata( metaMap, acl, response.getType().toString() );
    }

    @Override
    public void setUserMetadata( ObjectIdentifier identifier, Metadata... metadata ) {
        URI uri = config.resolvePath( identifier.getRelativeResourcePath(), "metadata/user" );
        WebResource.Builder builder = client.resource( uri ).getRequestBuilder();

        if ( identifier instanceof ObjectKey )
            builder.header( RestUtil.XHEADER_POOL, ((ObjectKey) identifier).getBucket() );

        for ( Metadata oneMetadata : metadata ) {
            if ( oneMetadata.isListable() )
                builder.header( RestUtil.XHEADER_LISTABLE_META, oneMetadata.toASCIIString() );
            else builder.header( RestUtil.XHEADER_META, oneMetadata.toASCIIString() );
        }

        // workaround for clients that set a default content-type for POSTs
        builder.type( RestUtil.TYPE_DEFAULT );
        builder.header( RestUtil.XHEADER_UTF8, "true" ).post();
    }

    @Override
    public void deleteUserMetadata( ObjectIdentifier identifier, String... names ) {
        URI uri = config.resolvePath( identifier.getRelativeResourcePath(), "metadata/user" );
        WebResource.Builder builder = client.resource( uri ).getRequestBuilder();

        if ( identifier instanceof ObjectKey )
            builder.header( RestUtil.XHEADER_POOL, ((ObjectKey) identifier).getBucket() );

        for ( String name : names ) {
            builder.header( RestUtil.XHEADER_TAGS, HttpUtil.encodeUtf8( name ) );
        }

        builder.header( RestUtil.XHEADER_UTF8, "true" ).delete();
    }

    @Override
    public Set<String> listMetadata( String metadataName ) {
        URI uri = config.resolvePath( "objects", "listabletags" );
        WebResource.Builder builder = client.resource( uri ).getRequestBuilder();

        if ( metadataName != null )
            builder.header( RestUtil.XHEADER_TAGS, HttpUtil.encodeUtf8( metadataName ) );

        ClientResponse response = builder.header( RestUtil.XHEADER_UTF8, "true" ).get( ClientResponse.class );
        String headerValue = response.getHeaders().getFirst( RestUtil.XHEADER_LISTABLE_TAGS );

        Set<String> names = new TreeSet<String>();
        if ( headerValue == null ) return names;
        for ( String name : headerValue.split( "," ) )
            names.add( HttpUtil.decodeUtf8( name.trim() ) );

        response.close();

        return names;
    }

    @Override
    public ListObjectsResponse listObjects( ListObjectsRequest request ) {
        if ( request.getMetadataName() == null )
            throw new AtmosException( "You must specify the name of a listable piece of metadata" );

        ClientResponse response;
        try {
            response = build( request ).get( ClientResponse.class );
        } catch ( AtmosException e ) {

            // if the name doesn't exist, return an empty result instead of throwing an exception (requested by users)
            if ( e.getErrorCode() != 1003 ) throw e;
            ListObjectsResponse lor = new ListObjectsResponse();
            lor.setEntries( new ArrayList<ObjectEntry>() );
            return lor;
        }

        request.setToken( response.getHeaders().getFirst( RestUtil.XHEADER_TOKEN ) );
        if ( request.getToken() != null )
            l4j.info( "Results truncated. Call listObjects again for next page of results." );

        ListObjectsResponse ret = response.getEntity( ListObjectsResponse.class );

        response.close();

        return fillResponse( ret, response );
    }

    @Override
    public Acl getAcl( ObjectIdentifier identifier ) {
        URI uri = config.resolvePath( identifier.getRelativeResourcePath(), "acl" );
        WebResource.Builder builder = client.resource( uri ).getRequestBuilder();

        if ( identifier instanceof ObjectKey )
            builder.header( RestUtil.XHEADER_POOL, ((ObjectKey) identifier).getBucket() );

        ClientResponse response = builder.get( ClientResponse.class );

        Acl acl = new Acl();
        acl.setUserAcl( RestUtil.parseAclHeader( response.getHeaders().getFirst( RestUtil.XHEADER_USER_ACL ) ) );
        acl.setGroupAcl( RestUtil.parseAclHeader( response.getHeaders().getFirst( RestUtil.XHEADER_GROUP_ACL ) ) );

        response.close();

        return acl;
    }

    @Override
    public void setAcl( ObjectIdentifier identifier, Acl acl ) {
        URI uri = config.resolvePath( identifier.getRelativeResourcePath(), "acl" );
        WebResource.Builder builder = client.resource( uri ).getRequestBuilder();

        if ( identifier instanceof ObjectKey )
            builder.header( RestUtil.XHEADER_POOL, ((ObjectKey) identifier).getBucket() );

        if ( acl != null ) {
            for ( Object value : acl.getUserAclHeader() ) builder.header( RestUtil.XHEADER_USER_ACL, value );
            for ( Object value : acl.getGroupAclHeader() ) builder.header( RestUtil.XHEADER_GROUP_ACL, value );
        }

        // workaround for clients that set a default content-type for POSTs
        builder.type( RestUtil.TYPE_DEFAULT );
        builder.post();
    }

    @Override
    public ObjectInfo getObjectInfo( ObjectIdentifier identifier ) {
        URI uri = config.resolvePath( identifier.getRelativeResourcePath(), "info" );
        WebResource.Builder builder = client.resource( uri ).getRequestBuilder();

        if ( identifier instanceof ObjectKey )
            builder.header( RestUtil.XHEADER_POOL, ((ObjectKey) identifier).getBucket() );

        return builder.get( ObjectInfo.class );
    }

    @Override
    public ObjectId createVersion( ObjectIdentifier identifier ) {
        URI uri = config.resolvePath( identifier.getRelativeResourcePath(), "versions" );
        WebResource.Builder builder = client.resource( uri ).getRequestBuilder();

        if ( identifier instanceof ObjectKey )
            builder.header( RestUtil.XHEADER_POOL, ((ObjectKey) identifier).getBucket() );

        // workaround for clients that set a default content-type for POSTs
        builder.type( RestUtil.TYPE_DEFAULT );
        ClientResponse response = builder.post( ClientResponse.class );

        response.close();

        return RestUtil.parseObjectId( response.getLocation().getPath() );
    }

    @Override
    public ListVersionsResponse listVersions( ListVersionsRequest request ) {
        ClientResponse response = build( request ).get( ClientResponse.class );

        request.setToken( response.getHeaders().getFirst( RestUtil.XHEADER_TOKEN ) );
        if ( request.getToken() != null )
            l4j.info( "Results truncated. Call listVersions again for next page of results." );

        ListVersionsResponse ret = response.getEntity( ListVersionsResponse.class );

        response.close();

        return fillResponse( ret, response );
    }

    @Override
    public void restoreVersion( ObjectId objectId, ObjectId versionId ) {
        URI uri = config.resolvePath( objectId.getRelativeResourcePath(), "versions" );
        WebResource.Builder builder = client.resource( uri ).getRequestBuilder();

        builder.header( RestUtil.XHEADER_VERSION_OID, versionId ).put();
    }

    @Override
    public void deleteVersion( ObjectId versionId ) {
        client.resource( config.resolvePath( versionId.getRelativeResourcePath(), "versions" ) ).delete();
    }

    @Override
    public CreateAccessTokenResponse createAccessToken( CreateAccessTokenRequest request )
            throws MalformedURLException {
        ClientResponse response = build( request ).post( ClientResponse.class, request.getPolicy() );
        URI tokenUri = config.resolvePath( response.getLocation().getPath(), response.getLocation().getQuery() );

        response.close();

        return fillResponse( new CreateAccessTokenResponse( tokenUri.toURL() ), response );
    }

    @Override
    public GetAccessTokenResponse getAccessToken( String accessTokenId ) {
        URI uri = config.resolvePath( "accesstokens/" + accessTokenId, "info" );
        ClientResponse response = client.resource( uri ).get( ClientResponse.class );

        GetAccessTokenResponse ret = new GetAccessTokenResponse( response.getEntity( AccessToken.class ) );

        response.close();

        return fillResponse( ret, response );
    }

    @Override
    public void deleteAccessToken( String accessTokenId ) {
        client.resource( config.resolvePath( "accesstokens/" + accessTokenId, null ) ).delete();
    }

    @Override
    public ListAccessTokensResponse listAccessTokens( ListAccessTokensRequest request ) {
        ClientResponse response = build( request ).get( ClientResponse.class );

        request.setToken( response.getHeaders().getFirst( RestUtil.XHEADER_TOKEN ) );
        if ( request.getToken() != null )
            l4j.info( "Results truncated. Call listAccessTokens again for next page of results." );

        ListAccessTokensResponse ret = response.getEntity( ListAccessTokensResponse.class );

        response.close();

        return fillResponse( ret, response );
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> GenericResponse<T> execute( PreSignedRequest request, Class<T> resultType, Object content )
            throws URISyntaxException {
        WebResource.Builder builder = client.resource( request.getUrl().toURI() ).getRequestBuilder();
        addHeaders( builder, request.getHeaders() ).type( request.getContentType() );
        ClientResponse response = builder.method( request.getMethod(), ClientResponse.class, content );

        GenericResponse<T> ret;
        if ( InputStream.class.equals( resultType ) ) {
            ret = (GenericResponse<T>) new GenericResponse<InputStream>( response.getEntityInputStream() );
        } else {
            ret = new GenericResponse<T>( response.getEntity( resultType ) );
            response.close();
        }

        return fillResponse( ret, response );
    }

    protected WebResource.Builder build( Request request ) {
        WebResource resource;
        if ( request.supports100Continue() && config.isEnableExpect100Continue() && client100 != null ) {
            // use client with Expect: 100-continue
            l4j.debug( "Expect: 100-continue is enabled for this request" );
            resource = client100.resource( config.resolvePath( request.getServiceRelativePath(), request.getQuery() ) );
        } else {
            resource = client.resource( config.resolvePath( request.getServiceRelativePath(), request.getQuery() ) );
        }

        WebResource.Builder builder = resource.getRequestBuilder();

        if ( request instanceof ContentRequest ) {
            ContentRequest contentRequest = (ContentRequest) request;
            if ( contentRequest.getContentType() == null ) builder.type( AbstractAtmosApi.DEFAULT_CONTENT_TYPE );
            else builder.type( contentRequest.getContentType() );
        } else if ( "POST".equals( request.getMethod() ) ) {
            // workaround for clients that set a default content-type for POSTs
            builder.type( RestUtil.TYPE_DEFAULT );
        }

        return addHeaders( builder, request.generateHeaders() );
    }

    protected WebResource.Builder addHeaders( WebResource.Builder builder, Map<String, List<Object>> headers ) {
        for ( String name : headers.keySet() ) {
            for ( Object value : headers.get( name ) ) {
                builder.header( name, value );
            }
        }
        return builder;
    }

    /**
     * Populates a response object with data from the ClientResponse.
     */
    protected <T extends BasicResponse> T fillResponse( T response, ClientResponse clientResponse ) {
        ClientResponse.Status status = clientResponse.getClientResponseStatus();
        MediaType type = clientResponse.getType();
        URI location = clientResponse.getLocation();
        response.setHttpStatus( clientResponse.getStatus() );
        response.setHttpMessage( status == null ? null : status.getReasonPhrase() );
        response.setHeaders( clientResponse.getHeaders() );
        response.setContentType( type == null ? null : type.toString() );
        response.setContentLength( clientResponse.getLength() );
        response.setLocation( location == null ? null : location.toString() );
        response.setLastModified( clientResponse.getLastModified() );
        response.setDate( clientResponse.getResponseDate() );
        return response;
    }

    protected Object getContent( ContentRequest request ) {
        Object content = request.getContent();
        if ( content == null ) return new byte[0]; // need this to provide Content-Length: 0

        else if ( content instanceof InputStream ) {
            if ( request.getContentLength() < 0 )
                throw new UnsupportedOperationException(
                        "Content request with input stream must provide content length" );

            if ( request.getContentLength() == 0 )
                l4j.info( "Content request with input stream and zero-length will not send any data" );

            return new MeasuredInputStream( (InputStream) content, request.getContentLength() );
        } else return content;
    }

    @Override
    public String createSubtenant(CreateSubtenantRequest request) {
        ClientResponse response = build( request ).put( ClientResponse.class );
        return response.getHeaders().get("subtenantID").get(0);
    }

}
