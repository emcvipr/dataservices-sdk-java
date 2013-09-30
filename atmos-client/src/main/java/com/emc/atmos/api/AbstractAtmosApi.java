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

import com.emc.atmos.api.bean.GetAccessTokenResponse;
import com.emc.atmos.api.request.*;
import com.emc.util.HttpUtil;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

public abstract class AbstractAtmosApi implements AtmosApi {
    public static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";

    protected AtmosConfig config;

    public AbstractAtmosApi( AtmosConfig config ) {
        this.config = config;
    }

    @Override
    public ObjectId createObject( Object content, String contentType ) {
        return createObject( new CreateObjectRequest().content( content ).contentType( contentType ) ).getObjectId();
    }

    @Override
    public ObjectId createObject( ObjectIdentifier identifier, Object content, String contentType ) {
        return createObject( new CreateObjectRequest().identifier( identifier )
                                                      .content( content )
                                                      .contentType( contentType ) ).getObjectId();
    }

    @Override
    public <T> T readObject( ObjectIdentifier identifier, Class<T> objectType ) throws IOException {
        return readObject( new ReadObjectRequest().identifier( identifier ), objectType ).getObject();
    }

    @Override
    public <T> T readObject( ObjectIdentifier identifier, Range range, Class<T> objectType ) throws IOException {
        return readObject( new ReadObjectRequest().identifier( identifier ).ranges( range ), objectType ).getObject();
    }

    @Override
    public void updateObject( ObjectIdentifier identifier, Object content ) {
        updateObject( new UpdateObjectRequest().identifier( identifier ).content( content ) );
    }

    @Override
    public void updateObject( ObjectIdentifier identifier, Object content, Range range ) {
        updateObject( new UpdateObjectRequest().identifier( identifier ).content( content ).range( range ) );
    }

    @Override
    public URL getShareableUrl( ObjectIdentifier identifier, Date expirationDate ) throws MalformedURLException {
        return getShareableUrl( identifier, expirationDate, null );
    }

    @Override
    public URL getShareableUrl( ObjectIdentifier identifier, Date expirationDate, String disposition )
            throws MalformedURLException {
        if ( identifier instanceof ObjectKey )
            throw new IllegalArgumentException( "You cannot create shareable URLs using a key; try using the object ID" );

        URI uri = config.resolvePath( identifier.getRelativeResourcePath(), null );
        String path = uri.getPath().toLowerCase();
        long expiresTime = expirationDate.getTime() / 1000;

        String hashString = "GET\n"
                            + path + '\n'
                            + config.getTokenId() + '\n'
                            + expiresTime;
        if ( disposition != null )
            hashString += '\n' + disposition;

        String hash = RestUtil.sign( hashString, config.getSecretKey() );

        String query = "uid=" + HttpUtil.encodeUtf8( config.getTokenId() ) + "&expires=" + expiresTime
                       + "&signature=" + HttpUtil.encodeUtf8( hash );
        if ( disposition != null )
            query += "&disposition=" + HttpUtil.encodeUtf8( disposition );

        // we must manually append the query string to ensure the equals sign in the signature gets encoded properly
        return new URL( uri + "?" + query );
    }

    @Override
    public GetAccessTokenResponse getAccessToken( URL url ) {
        return getAccessToken( RestUtil.lastPathElement( url.getPath() ) );
    }

    @Override
    public void deleteAccessToken( URL url ) {
        deleteAccessToken( RestUtil.lastPathElement( url.getPath() ) );
    }

    @Override
    public PreSignedRequest preSignRequest( Request request, Date expiration ) throws MalformedURLException {
        URI uri = config.resolvePath( request.getServiceRelativePath(), request.getQuery() );
        Map<String, List<Object>> headers = request.generateHeaders();

        String contentType = null;
        if ( request instanceof ContentRequest ) contentType = ((ContentRequest) request).getContentType();
        // workaround for clients that set a default content-type for POSTs
        if ( "POST".equals( request.getMethod() ) ) contentType = RestUtil.TYPE_DEFAULT;

        // add expiration header
        headers.put( RestUtil.XHEADER_EXPIRES, Arrays.asList( (Object) expiration.getTime() ) );

        RestUtil.signRequest( request.getMethod(),
                              uri.getPath(),
                              uri.getQuery(),
                              headers,
                              config.getTokenId(),
                              config.getSecretKey(),
                              config.getServerClockSkew() );

        return new PreSignedRequest( uri.toURL(), request.getMethod(), contentType, headers, expiration );
    }
}
