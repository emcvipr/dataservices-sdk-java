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
package com.emc.atmos.api.test;

import com.emc.atmos.AtmosException;
import com.emc.atmos.api.*;
import com.emc.atmos.api.bean.*;
import com.emc.atmos.api.jersey.AtmosApiClient;
import com.emc.atmos.api.multipart.MultipartEntity;
import com.emc.atmos.api.request.*;
import com.emc.atmos.util.AtmosClientFactory;
import com.emc.atmos.util.RandomInputStream;
import com.emc.util.StreamUtil;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.filter.ClientFilter;
import com.sun.jersey.core.header.FormDataContentDisposition;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.FormDataMultiPart;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.junit.*;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.ws.rs.core.MediaType;
import java.io.*;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AtmosApiClientTest {
    public static Logger l4j = Logger.getLogger( AtmosApiClientTest.class );

    /**
     * Use this as a prefix for namespace object paths and you won't have to clean up after yourself.
     * This also keeps all test objects under one folder, which is easy to delete should something go awry.
     */
    protected static final String TESTDIR = "/test_" + AtmosApiClientTest.class.getSimpleName() + "/";

    protected AtmosConfig config;
    protected AtmosApi api;

    protected List<ObjectIdentifier> cleanup = Collections.synchronizedList( new ArrayList<ObjectIdentifier>() );

    public AtmosApiClientTest() throws Exception {
        config = AtmosClientFactory.getAtmosConfig();
        config.setDisableSslValidation( false );
        config.setEnableExpect100Continue( false );
        config.setEnableRetry( false );
        api = new AtmosApiClient( config );
    }

    @After
    public void tearDown() {
        for ( final ObjectIdentifier cleanItem : cleanup ) {
            new Thread() {
                public void run() {
                    try {
                        api.delete( cleanItem );
                    } catch ( Throwable t ) {
                        System.out.println( "Failed to delete " + cleanItem + ": " + t.getMessage() );
                    }
                }
            }.start();
        }
        try { // if the test directory exists, recursively delete it
            this.api.getSystemMetadata( new ObjectPath( TESTDIR ) );
            deleteRecursively( new ObjectPath( TESTDIR ) );
        } catch ( AtmosException e ) {
            if ( e.getHttpCode() != 404 ) {
                l4j.warn( "Could not delete test dir: ", e );
            }
        }
        try {
            ListAccessTokensResponse response = this.api.listAccessTokens( new ListAccessTokensRequest() );
            if ( response.getTokens() != null ) {
                for ( AccessToken token : response.getTokens() ) {
                    this.api.deleteAccessToken( token.getId() );
                }
            }
        } catch ( Exception e ) {
            System.out.println( "Failed to delete access tokens: " + e.getMessage() );
        }
    }

    protected void deleteRecursively( ObjectPath path ) {
        if ( path.isDirectory() ) {
            ListDirectoryRequest request = new ListDirectoryRequest().path( path );
            do {
                for ( DirectoryEntry entry : this.api.listDirectory( request ).getEntries() ) {
                    deleteRecursively( new ObjectPath( path, entry ) );
                }
            } while ( request.getToken() != null );
        }
        this.api.delete( path );
    }

    //
    // TESTS START HERE
    //

    @Test
    public void testUtf8JavaEncoding() throws Exception {
        String oneByteCharacters = "Hello";
        String twoByteCharacters = "\u0410\u0411\u0412\u0413"; // Cyrillic letters
        String twoByteEscaped = "%D0%90%D0%91%D0%92%D0%93";
        String fourByteCharacters = "\ud841\udf0e\ud841\udf31\ud841\udf79\ud843\udc53"; // Chinese symbols
        String fourByteEscaped = "%F0%A0%9C%8E%F0%A0%9C%B1%F0%A0%9D%B9%F0%A0%B1%93";
        Assert.assertEquals( "2-byte characters failed",
                             URLEncoder.encode( twoByteCharacters, "UTF-8" ),
                             twoByteEscaped );
        Assert.assertEquals( "4-byte characters failed",
                             URLEncoder.encode( fourByteCharacters, "UTF-8" ),
                             fourByteEscaped );
        Assert.assertEquals( "2-byte/4-byte mix failed",
                             URLEncoder.encode( twoByteCharacters + fourByteCharacters, "UTF-8" ),
                             twoByteEscaped + fourByteEscaped );
        Assert.assertEquals( "1-byte/2-byte mix failed",
                             URLEncoder.encode( oneByteCharacters + twoByteCharacters, "UTF-8" ),
                             oneByteCharacters + twoByteEscaped );
        Assert.assertEquals( "1-4 byte mix failed",
                             URLEncoder.encode( oneByteCharacters + twoByteCharacters + fourByteCharacters, "UTF-8" ),
                             oneByteCharacters + twoByteEscaped + fourByteEscaped );
    }

    /**
     * Test creating one empty object.  No metadata, no content.
     */
    @Test
    public void testCreateEmptyObject() throws Exception {
        ObjectId id = this.api.createObject( null, null );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read back the content
        String content = new String( this.api.readObject( id, null, byte[].class ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "", content );

    }

    /**
     * Test creating one empty object on a path.  No metadata, no content.
     */
    @Test
    public void testCreateEmptyObjectOnPath() throws Exception {
        ObjectPath op = new ObjectPath( "/" + rand8char() );
        ObjectId id = this.api.createObject( op, null, null );
        cleanup.add( op );
        l4j.debug( "Path: " + op + " ID: " + id );
        Assert.assertNotNull( id );

        // Read back the content
        String content = new String( this.api.readObject( op, null, byte[].class ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "", content );
        content = new String( this.api.readObject( id, null, byte[].class ), "UTF-8" );
        Assert.assertEquals( "object content wrong when reading by id", "", content );
    }


    /**
     * Tests using some extended characters when creating on a path.  This particular test
     * uses one cryllic, one accented, and one japanese character.
     */
    @Test
    public void testUnicodePath() throws Exception {
        String dirName = rand8char();
        ObjectPath path = new ObjectPath( "/" + dirName + "/бöｼ.txt" );
        ObjectId id = this.api.createObject( path, null, null );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        ObjectPath parent = new ObjectPath( "/" + dirName + "/" );
        ListDirectoryResponse response = this.api.listDirectory( new ListDirectoryRequest().path( parent ) );
        boolean found = false;
        for ( DirectoryEntry ent : response.getEntries() ) {
            if ( new ObjectPath( parent, ent.getFilename() ).equals( path ) ) {
                found = true;
            }
        }
        Assert.assertTrue( "Did not find unicode file in dir", found );

        // Check read
        this.api.readObject( path, null, byte[].class );
    }

    /**
     * Tests using some extra characters that might break URIs
     */
    @Test
    public void testExtraPath() throws Exception {
        ObjectPath path = new ObjectPath( "/" + rand8char() + "/a+=-  _!#$%^&*(),.z.txt" );
        //ObjectPath path = new ObjectPath("/zimbramailbox/c8b4/511a-63c4-4ac9-8ff7+1c578de044be/stage/3r0sFrgUgL2ApCSkl3pobSX9D+k-1");
        byte[] data = "Hello World".getBytes( "UTF-8" );
        InputStream in = new ByteArrayInputStream( data );
        CreateObjectRequest request = new CreateObjectRequest().identifier( path ).content( in )
                                                               .contentLength( data.length );
        ObjectId id = this.api.createObject( request ).getObjectId();
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );
    }

    @Test
    public void testUtf8Path() throws Exception {
        String oneByteCharacters = "Hello! ,";
        String twoByteCharacters = "\u0410\u0411\u0412\u0413"; // Cyrillic letters
        String fourByteCharacters = "\ud841\udf0e\ud841\udf31\ud841\udf79\ud843\udc53"; // Chinese symbols
        String crazyName = oneByteCharacters + twoByteCharacters + fourByteCharacters;
        byte[] content = "Crazy name creation test.".getBytes( "UTF-8" );
        ObjectPath parent = new ObjectPath( TESTDIR );
        ObjectPath path = new ObjectPath( parent, crazyName );

        // create crazy-name object
        this.api.createObject( path, content, "text/plain" );

        cleanup.add( path );

        // verify name in directory list
        boolean found = false;
        ListDirectoryRequest request = new ListDirectoryRequest().path( parent );
        for ( DirectoryEntry entry : this.api.listDirectory( request ).getEntries() ) {
            if ( new ObjectPath( parent, entry.getFilename() ).equals( path ) ) {
                found = true;
                break;
            }
        }
        Assert.assertTrue( "crazyName not found in directory listing", found );

        // verify content
        Assert.assertTrue( "content does not match",
                           Arrays.equals( content, this.api.readObject( path, null, byte[].class ) ) );
    }

    @Test
    public void testUtf8Content() throws Exception {
        String oneByteCharacters = "Hello! ,";
        String twoByteCharacters = "\u0410\u0411\u0412\u0413"; // Cyrillic letters
        String fourByteCharacters = "\ud841\udf0e\ud841\udf31\ud841\udf79\ud843\udc53"; // Chinese symbols
        byte[] content = (oneByteCharacters + twoByteCharacters + fourByteCharacters).getBytes( "UTF-8" );

        // create object with multi-byte UTF-8 content
        ObjectId oid = api.createObject( content, "text/plain" );
        cleanup.add( oid );

        byte[] readContent = this.api.readObject( oid, null, byte[].class );

        // verify content
        Assert.assertTrue( "content does not match", Arrays.equals( content, readContent ) );
    }

    /**
     * Test creating an object with content but without metadata
     */
    @Test
    public void testCreateObjectWithContent() throws Exception {
        ObjectId id = this.api.createObject( "hello".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read back the content
        String content = new String( this.api.readObject( id, null, byte[].class ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "hello", content );
    }

    @Test
    public void testCreateObjectWithSegment() throws Exception {
        byte[] content = "hello".getBytes( "UTF-8" );
        ObjectId id = api.createObject( new BufferSegment( content, 0, content.length ), null );
        cleanup.add( id );

        // Read back the content
        String result = new String( this.api.readObject( id, null, byte[].class ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "hello", result );
    }

    @Test
    public void testCreateObjectWithContentStream() throws Exception {
        InputStream in = new ByteArrayInputStream( "hello".getBytes( "UTF-8" ) );
        CreateObjectRequest request = new CreateObjectRequest().content( in ).contentLength( 5 )
                                                               .contentType( "text/plain" );
        ObjectId id = this.api.createObject( request ).getObjectId();
        in.close();
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read back the content
        String content = new String( this.api.readObject( id, null, byte[].class ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "hello", content );
    }

    @Test
    public void testCreateObjectWithContentStreamOnPath() throws Exception {
        ObjectPath op = new ObjectPath( "/" + rand8char() + ".tmp" );
        InputStream in = new ByteArrayInputStream( "hello".getBytes( "UTF-8" ) );
        CreateObjectRequest request = new CreateObjectRequest();
        request.identifier( op ).content( in ).contentLength( 5 ).contentType( "text/plain" );
        ObjectId id = this.api.createObject( request ).getObjectId();
        in.close();
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read back the content
        String content = new String( this.api.readObject( id, null, byte[].class ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "hello", content );
    }

    /**
     * Test creating an object with metadata but no content.
     */
    @Test
    public void testCreateObjectWithMetadataOnPath() {
        ObjectPath op = new ObjectPath( "/" + rand8char() + ".tmp" );
        CreateObjectRequest request = new CreateObjectRequest().identifier( op );
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "listable2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "unlistable2", "bar2 bar2", false );
        request.userMetadata( listable, unlistable, listable2, unlistable2 );
        ObjectId id = this.api.createObject( request ).getObjectId();
        //this.esu.updateObject( op, null, mlist, null, null, null );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( op );

        // Read and validate the metadata
        Map<String, Metadata> meta = this.api.getUserMetadata( op );
        Assert.assertNotNull( "value of 'listable' missing", meta.get( "listable" ) );
        Assert.assertNotNull( "value of 'listable2' missing", meta.get( "listable2" ) );
        Assert.assertNotNull( "value of 'unlistable' missing", meta.get( "unlistable" ) );
        Assert.assertNotNull( "value of 'unlistable2' missing", meta.get( "unlistable2" ) );

        Assert.assertEquals( "value of 'listable' wrong", "foo", meta.get( "listable" ).getValue() );
        Assert.assertEquals( "value of 'listable2' wrong", "foo2 foo2", meta.get( "listable2" ).getValue() );
        Assert.assertEquals( "value of 'unlistable' wrong", "bar", meta.get( "unlistable" ).getValue() );
        Assert.assertEquals( "value of 'unlistable2' wrong", "bar2 bar2", meta.get( "unlistable2" ).getValue() );
        // Check listable flags
        Assert.assertEquals( "'listable' is not listable", true, meta.get( "listable" ).isListable() );
        Assert.assertEquals( "'listable2' is not listable", true, meta.get( "listable2" ).isListable() );
        Assert.assertEquals( "'unlistable' is listable", false, meta.get( "unlistable" ).isListable() );
        Assert.assertEquals( "'unlistable2' is listable", false, meta.get( "unlistable2" ).isListable() );
    }

    /**
     * Test creating an object with metadata but no content.
     */
    @Test
    public void testCreateObjectWithMetadata() {
        CreateObjectRequest request = new CreateObjectRequest();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "listable2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "unlistable2", "bar2 bar2", false );
        Metadata listable3 = new Metadata( "listable3", null, true );
        Metadata quotes = new Metadata( "ST_modalities", "\\US\\", false );
        //Metadata withCommas = new Metadata( "withcommas", "I, Robot", false );
        //Metadata withEquals = new Metadata( "withequals", "name=value", false );
        request.userMetadata( listable, unlistable, listable2, unlistable2, listable3, quotes );
        //request.addUserMetadata( withCommas );
        //request.addUserMetadata( withEquals );
        ObjectId id = this.api.createObject( request ).getObjectId();
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read and validate the metadata
        Map<String, Metadata> meta = this.api.getUserMetadata( id );
        Assert.assertEquals( "value of 'listable' wrong", "foo", meta.get( "listable" ).getValue() );
        Assert.assertEquals( "value of 'listable2' wrong", "foo2 foo2", meta.get( "listable2" ).getValue() );
        Assert.assertEquals( "value of 'unlistable' wrong", "bar", meta.get( "unlistable" ).getValue() );
        Assert.assertEquals( "value of 'unlistable2' wrong",
                             "bar2 bar2",
                             meta.get( "unlistable2" ).getValue() );
        Assert.assertNotNull( "listable3 missing", meta.get( "listable3" ) );
        Assert.assertTrue( "Value of listable3 should be empty",
                           meta.get( "listable3" ).getValue() == null
                           || meta.get( "listable3" ).getValue().length() == 0 );
        //Assert.assertEquals( "Value of withcommas wrong", "I, Robot", meta.get( "withcommas" ).getValue() );
        //Assert.assertEquals( "Value of withequals wrong", "name=value", meta.get( "withequals" ).getValue() );

        // Check listable flags
        Assert.assertEquals( "'listable' is not listable", true, meta.get( "listable" ).isListable() );
        Assert.assertEquals( "'listable2' is not listable", true, meta.get( "listable2" ).isListable() );
        Assert.assertEquals( "'listable3' is not listable", true, meta.get( "listable3" ).isListable() );
        Assert.assertEquals( "'unlistable' is listable", false, meta.get( "unlistable" ).isListable() );
        Assert.assertEquals( "'unlistable2' is listable", false, meta.get( "unlistable2" ).isListable() );

    }

    /**
     * Test creating an object with metadata but no content.
     */
    @Test
    public void testMetadataNormalizeSpace() {
        CreateObjectRequest request = new CreateObjectRequest();
        Metadata unlistable = new Metadata( "unlistable", "bar  bar   bar    bar", false );
        Metadata leadingSpacesOdd = new Metadata( "leadingodd", "   spaces", false );
        Metadata trailingSpacesOdd = new Metadata( "trailingodd", "spaces   ", false );
        Metadata leadingSpacesEven = new Metadata( "leadingeven", "    SPACES", false );
        Metadata trailingSpacesEven = new Metadata( "trailingeven", "spaces    ", false );
        request.userMetadata( unlistable, leadingSpacesOdd, trailingSpacesOdd, leadingSpacesEven, trailingSpacesEven );
        ObjectId id = this.api.createObject( request ).getObjectId();
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read and validate the metadata
        Map<String, Metadata> meta = this.api.getUserMetadata( id );
        Assert.assertEquals( "value of 'unlistable' wrong",
                             "bar  bar   bar    bar",
                             meta.get( "unlistable" ).getValue() );
        // Check listable flags
        Assert.assertEquals( "'unlistable' is listable", false, meta.get( "unlistable" ).isListable() );

    }

    /**
     * Test reading an object's content
     */
    @Test
    public void testReadObject() throws Exception {
        ObjectId id = this.api.createObject( "hello".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read back the content
        String content = new String( this.api.readObject( id, null, byte[].class ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "hello", content );

        // Read back only 2 bytes
        Range range = new Range( 1, 2 );
        content = new String( this.api.readObject( id, range, byte[].class ), "UTF-8" );
        Assert.assertEquals( "partial object content wrong", "el", content );
    }

    @Test
    public void testResponseProperties() throws Exception {
        // Subtract a second since the HTTP dates only have 1s precision.
        Date now = new Date();
        CreateObjectRequest request = new CreateObjectRequest().content( "hello".getBytes( "UTF-8" ) )
                                                               .contentType( "text/plain" );
        CreateObjectResponse response = this.api.createObject( request );
        Assert.assertNotNull( "null ID returned", response.getObjectId() );
        Assert.assertEquals( "location wrong", "/rest/objects/" + response.getObjectId(), response.getLocation() );
        cleanup.add( response.getObjectId() );

        // Read back the content
        ReadObjectResponse<String> readResponse = api.readObject( new ReadObjectRequest().identifier( response.getObjectId() ),
                                                                  String.class );
        Assert.assertEquals( "object content wrong", "hello", readResponse.getObject() );
        Assert.assertEquals( "HTTP status wrong", 200, readResponse.getHttpStatus() );
        Assert.assertEquals( "HTTP message wrong", "OK", readResponse.getHttpMessage() );
        Assert.assertFalse( "HTTP headers empty", readResponse.getHeaders().isEmpty() );
        Assert.assertTrue( "HTTP content-type wrong",
                           readResponse.getContentType().matches( "text/plain(; charset=UTF-8)?" ) );
        Assert.assertEquals( "HTTP content-length wrong", 5, readResponse.getContentLength() );
        Assert.assertTrue( "HTTP response date wrong", Math.abs( response.getDate().getTime() - now.getTime() ) < (1000 * 60 * 5) );
        // apparently last-modified isn't included in GET requests
        // Assert.assertTrue( "HTTP last modified date wrong", readResponse.getLastModified().after( now ) );
    }

    /**
     * Test reading an ACL back
     */
    @Test
    public void testReadAcl() {
        // Create an object with an ACL
        Acl acl = new Acl();
        acl.addUserGrant( stripUid( config.getTokenId() ), Permission.FULL_CONTROL );
        acl.addGroupGrant( Acl.GROUP_OTHER, Permission.READ );
        ObjectId id = this.api.createObject( new CreateObjectRequest().acl( acl ) ).getObjectId();
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read back the ACL and make sure it matches
        Acl newacl = this.api.getAcl( id );
        l4j.info( "Comparing " + newacl + " with " + acl );

        Assert.assertEquals( "ACLs don't match", acl, newacl );

    }

    /**
     * Inside an ACL, you use the UID only, not SubtenantID/UID
     */
    private String stripUid( String uid ) {
        int slash = uid.indexOf( '/' );
        if ( slash != -1 ) {
            return uid.substring( slash + 1 );
        } else {
            return uid;
        }
    }

    @Test
    public void testReadAclByPath() {
        ObjectPath op = new ObjectPath( "/" + rand8char() + ".tmp" );
        // Create an object with an ACL
        Acl acl = new Acl();
        acl.addUserGrant( stripUid( config.getTokenId() ), Permission.FULL_CONTROL );
        acl.addGroupGrant( Acl.GROUP_OTHER, Permission.READ );
        ObjectId id = this.api.createObject( new CreateObjectRequest().identifier( op ).acl( acl ) ).getObjectId();
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( op );

        // Read back the ACL and make sure it matches
        Acl newacl = this.api.getAcl( op );
        l4j.info( "Comparing " + newacl + " with " + acl );

        Assert.assertEquals( "ACLs don't match", acl, newacl );

    }

    /**
     * Test reading back user metadata
     */
    @Test
    public void testGetUserMetadata() {
        // Create an object with user metadata
        CreateObjectRequest request = new CreateObjectRequest();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "listable2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "unlistable2", "bar2 bar2", false );
        request.userMetadata( listable, unlistable, listable2, unlistable2 );
        ObjectId id = this.api.createObject( request ).getObjectId();
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read only part of the metadata
        Map<String, Metadata> meta = this.api.getUserMetadata( id, "listable", "unlistable" );
        Assert.assertEquals( "value of 'listable' wrong", "foo", meta.get( "listable" ).getValue() );
        Assert.assertNull( "value of 'listable2' should not have been returned", meta.get( "listable2" ) );
        Assert.assertEquals( "value of 'unlistable' wrong", "bar", meta.get( "unlistable" ).getValue() );
        Assert.assertNull( "value of 'unlistable2' should not have been returned", meta.get( "unlistable2" ) );

    }

    /**
     * Test deleting user metadata
     */
    @Test
    public void testDeleteUserMetadata() {
        // Create an object with metadata
        CreateObjectRequest request = new CreateObjectRequest();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "listable2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "unlistable2", "bar2 bar2", false );
        request.userMetadata( listable, unlistable, listable2, unlistable2 );
        ObjectId id = this.api.createObject( request ).getObjectId();
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Delete a couple of the metadata entries
        this.api.deleteUserMetadata( id, "listable2", "unlistable2" );

        // Read back the metadata for the object and ensure the deleted
        // entries don't exist
        Map<String, Metadata> meta = this.api.getUserMetadata( id );
        Assert.assertEquals( "value of 'listable' wrong", "foo", meta.get( "listable" ).getValue() );
        Assert.assertNull( "value of 'listable2' should not have been returned", meta.get( "listable2" ) );
        Assert.assertEquals( "value of 'unlistable' wrong", "bar", meta.get( "unlistable" ).getValue() );
        Assert.assertNull( "value of 'unlistable2' should not have been returned", meta.get( "unlistable2" ) );
    }

    /**
     * Test creating object versions
     */
    @Test
    public void testVersionObject() throws Exception {
        // Create an object
        String content = "Version Test";
        CreateObjectRequest request = new CreateObjectRequest().content( content );
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "listable2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "unlistable2", "bar2 bar2", false );
        request.userMetadata( listable, unlistable, listable2, unlistable2 );
        ObjectId id = this.api.createObject( request ).getObjectId();
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Version the object
        ObjectId vid = this.api.createVersion( id );
        cleanup.add( vid );
        Assert.assertNotNull( "null version ID returned", vid );

        Assert.assertFalse( "Version ID shoudn't be same as original ID", id.equals( vid ) );

        // Fetch the version and read its data
        Assert.assertEquals( "Version content wrong", content, this.api.readObject( vid, null, String.class ) );

        Map<String, Metadata> meta = this.api.getUserMetadata( vid );
        Assert.assertEquals( "value of 'listable' wrong", "foo", meta.get( "listable" ).getValue() );
        Assert.assertEquals( "value of 'listable2' wrong", "foo2 foo2", meta.get( "listable2" ).getValue() );
        Assert.assertEquals( "value of 'unlistable' wrong", "bar", meta.get( "unlistable" ).getValue() );
        Assert.assertEquals( "value of 'unlistable2' wrong",
                             "bar2 bar2",
                             meta.get( "unlistable2" ).getValue() );

    }

    /**
     * Test listing the versions of an object
     */
    @Test
    public void testListVersions() {
        // Create an object
        CreateObjectRequest request = new CreateObjectRequest();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "listable2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "unlistable2", "bar2 bar2", false );
        request.userMetadata( listable, unlistable, listable2, unlistable2 );
        ObjectId id = this.api.createObject( request ).getObjectId();
        cleanup.add( id );
        Assert.assertNotNull( "null ID returned", id );

        // Version the object
        ObjectId vid1 = this.api.createVersion( id );
        cleanup.add( vid1 );
        Assert.assertNotNull( "null version ID returned", vid1 );
        ObjectId vid2 = this.api.createVersion( id );
        cleanup.add( vid2 );
        Assert.assertNotNull( "null version ID returned", vid2 );

        // List the versions and ensure their IDs are correct
        ListVersionsResponse response = this.api.listVersions( new ListVersionsRequest().objectId( id ) );

        List<ObjectVersion> versions = response.getVersions();
        Assert.assertEquals( "Wrong number of versions returned", 2, versions.size() );
        Assert.assertTrue( "Version number less than zero", versions.get( 0 ).getVersionNumber() >= 0 );
        Assert.assertNotNull( "Version itime is null", versions.get( 0 ).getItime() );
        Assert.assertTrue( "Version number less than zero", versions.get( 1 ).getVersionNumber() >= 0 );
        Assert.assertNotNull( "Version itime is null", versions.get( 1 ).getItime() );


        List<ObjectId> versionIds = response.getVersionIds();
        Assert.assertEquals( "Wrong number of versions returned", 2, versionIds.size() );
        Assert.assertTrue( "version 1 not found in version list", versionIds.contains( vid1 ) );
        Assert.assertTrue( "version 2 not found in version list", versionIds.contains( vid2 ) );
    }

    /**
     * Test listing the versions of an object
     */
    @Test
    public void testListVersionsLong() {
        // Create an object
        CreateObjectRequest request = new CreateObjectRequest();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "listable2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "unlistable2", "bar2 bar2", false );
        request.userMetadata( listable, unlistable, listable2, unlistable2 );
        ObjectId id = this.api.createObject( request ).getObjectId();
        cleanup.add( id );
        Assert.assertNotNull( "null ID returned", id );

        // Version the object
        ObjectId vid1 = this.api.createVersion( id );
        ObjectVersion v1 = new ObjectVersion( 0, vid1, null );
        cleanup.add( vid1 );
        Assert.assertNotNull( "null version ID returned", vid1 );
        ObjectId vid2 = this.api.createVersion( id );
        cleanup.add( vid2 );
        ObjectVersion v2 = new ObjectVersion( 1, vid2, null );
        Assert.assertNotNull( "null version ID returned", vid2 );

        // List the versions and ensure their IDs are correct
        ListVersionsRequest vRequest = new ListVersionsRequest();
        vRequest.objectId( id ).setLimit( 1 );
        List<ObjectVersion> versions = new ArrayList<ObjectVersion>();
        do {
            ListVersionsResponse response = this.api.listVersions( vRequest );
            if ( response.getVersions() != null ) versions.addAll( response.getVersions() );
        } while ( vRequest.getToken() != null );
        Assert.assertEquals( "Wrong number of versions returned", 2, versions.size() );
        Assert.assertTrue( "version 1 not found in version list", versions.contains( v1 ) );
        Assert.assertTrue( "version 2 not found in version list", versions.contains( v2 ) );
        for ( ObjectVersion v : versions ) {
            Assert.assertNotNull( "oid null in version", v.getVersionId() );
            Assert.assertTrue( "Invalid version number in version", v.getVersionNumber() > -1 );
            Assert.assertNotNull( "itime null in version", v.getItime() );
        }
    }

    /**
     * Test listing the versions of an object
     */
    @Test
    public void testDeleteVersion() {
        // Create an object
        CreateObjectRequest request = new CreateObjectRequest();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "listable2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "unlistable2", "bar2 bar2", false );
        request.userMetadata( listable, unlistable, listable2, unlistable2 );
        ObjectId id = this.api.createObject( request ).getObjectId();
        cleanup.add( id );
        Assert.assertNotNull( "null ID returned", id );

        // Version the object
        ObjectId vid1 = this.api.createVersion( id );
        Assert.assertNotNull( "null version ID returned", vid1 );
        ObjectId vid2 = this.api.createVersion( id );
        cleanup.add( vid2 );
        Assert.assertNotNull( "null version ID returned", vid2 );

        // List the versions and ensure their IDs are correct
        List<ObjectId> versions = this.api.listVersions( new ListVersionsRequest().objectId( id ) ).getVersionIds();
        Assert.assertEquals( "Wrong number of versions returned", 2, versions.size() );
        Assert.assertTrue( "version 1 not found in version list", versions.contains( vid1 ) );
        Assert.assertTrue( "version 2 not found in version list", versions.contains( vid2 ) );

        // Delete a version
        this.api.deleteVersion( vid1 );
        versions = this.api.listVersions( new ListVersionsRequest().objectId( id ) ).getVersionIds();
        Assert.assertEquals( "Wrong number of versions returned", 1, versions.size() );
        Assert.assertFalse( "version 1 found in version list", versions.contains( vid1 ) );
        Assert.assertTrue( "version 2 not found in version list", versions.contains( vid2 ) );

    }

    @Test
    public void testRestoreVersion() throws IOException {
        ObjectId id = this.api.createObject( "Base Version Content".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Version the object
        ObjectId vId = this.api.createVersion( id );

        // Update the object content
        this.api.updateObject( id, "Child Version Content -- You should never see me".getBytes( "UTF-8" ) );

        // Restore the original version
        this.api.restoreVersion( id, vId );

        // Read back the content
        String content = new String( this.api.readObject( id, null, byte[].class ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "Base Version Content", content );
    }

    /**
     * Test listing the system metadata on an object
     */
    @Test
    public void testGetSystemMetadata() {
        // Create an object
        CreateObjectRequest request = new CreateObjectRequest();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "listable2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "unlistable2", "bar2 bar2", false );
        request.userMetadata( listable, unlistable, listable2, unlistable2 );
        ObjectId id = this.api.createObject( request ).getObjectId();
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read only part of the metadata
        Map<String, Metadata> meta = this.api.getSystemMetadata( id, "atime", "ctime" );
        Assert.assertNotNull( "value of 'atime' missing", meta.get( "atime" ) );
        Assert.assertNull( "value of 'mtime' should not have been returned", meta.get( "mtime" ) );
        Assert.assertNotNull( "value of 'ctime' missing", meta.get( "ctime" ) );
        Assert.assertNull( "value of 'gid' should not have been returned", meta.get( "gid" ) );
        Assert.assertNull( "value of 'listable' should not have been returned", meta.get( "listable" ) );
    }

    /**
     * Test listing objects by a tag that doesn't exist
     */
    @Test
    public void testListObjectsNoExist() {
        ListObjectsRequest request = new ListObjectsRequest().metadataName( "this_tag_should_not_exist" );
        List<ObjectEntry> objects = this.api.listObjects( request ).getEntries();
        Assert.assertNotNull( "object list should be not null", objects );
        Assert.assertEquals( "No objects should be returned", 0, objects.size() );
    }

    /**
     * Test listing objects by a tag
     */
    @Test
    public void testListObjects() {
        // Create an object
        CreateObjectRequest request = new CreateObjectRequest();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "list/able/2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "list/able/not", "bar2 bar2", false );
        request.userMetadata( listable, unlistable, listable2, unlistable2 );
        ObjectId id = this.api.createObject( request ).getObjectId();
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // List the objects.  Make sure the one we created is in the list
        ListObjectsRequest lRequest = new ListObjectsRequest().metadataName( "listable" );
        List<ObjectEntry> objects = this.api.listObjects( lRequest ).getEntries();
        ObjectEntry toFind = new ObjectEntry();
        toFind.setObjectId( id );
        Assert.assertTrue( "No objects returned", objects.size() > 0 );
        Assert.assertTrue( "object not found in list", objects.contains( toFind ) );

    }

    /**
     * Test listing objects by a tag
     */
    @Test
    public void testListObjectsWithMetadata() {
        // Create an object
        CreateObjectRequest request = new CreateObjectRequest();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "list/able/2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "list/able/not", "bar2 bar2", false );
        request.userMetadata( listable, unlistable, listable2, unlistable2 );
        ObjectId id = this.api.createObject( request ).getObjectId();
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // List the objects.  Make sure the one we created is in the list
        ListObjectsRequest lRequest = new ListObjectsRequest().metadataName( "listable" ).includeMetadata( true );
        List<ObjectEntry> objects = this.api.listObjects( lRequest ).getEntries();
        Assert.assertTrue( "No objects returned", objects.size() > 0 );

        // Find the item.
        boolean found = false;
        for ( ObjectEntry or : objects ) {
            if ( or.getObjectId().equals( id ) ) {
                found = true;
                // check metadata
                Assert.assertEquals( "Wrong value on metadata",
                                     or.getUserMetadataMap().get( "listable" ).getValue(), "foo" );
            }
        }
        Assert.assertTrue( "object not found in list", found );
    }

    /**
     * Test listing objects by a tag, with only some of the metadata
     */
    @Test
    public void testListObjectsWithSomeMetadata() {
        // Create an object
        CreateObjectRequest request = new CreateObjectRequest();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "list/able/2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "list/able/not", "bar2 bar2", false );
        request.userMetadata( listable, unlistable, listable2, unlistable2 );
        ObjectId id = this.api.createObject( request ).getObjectId();
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // List the objects.  Make sure the one we created is in the list
        ListObjectsRequest lRequest = new ListObjectsRequest();
        lRequest.metadataName( "listable" ).includeMetadata( true )
                .userMetadataNames( "listable" );
        List<ObjectEntry> objects = this.api.listObjects( lRequest ).getEntries();
        Assert.assertTrue( "No objects returned", objects.size() > 0 );

        // Find the item.
        boolean found = false;
        for ( ObjectEntry or : objects ) {
            if ( or.getObjectId().equals( id ) ) {
                found = true;
                // check metadata
                Assert.assertEquals( "Wrong value on metadata",
                                     or.getUserMetadataMap().get( "listable" ).getValue(), "foo" );

                // Other metadata should not be present
                Assert.assertNull( "unlistable should be missing",
                                   or.getUserMetadataMap().get( "unlistable" ) );
            }
        }
        Assert.assertTrue( "object not found in list", found );
    }

    /**
     * Test listing objects by a tag, paging the results
     */
    @Test
    public void testListObjectsPaged() {
        // Create two objects.
        CreateObjectRequest request = new CreateObjectRequest();
        Metadata listable = new Metadata( "listable", "foo", true );
        request.userMetadata( listable );
        ObjectId id1 = this.api.createObject( request ).getObjectId();
        ObjectId id2 = this.api.createObject( request ).getObjectId();
        Assert.assertNotNull( "null ID returned", id1 );
        Assert.assertNotNull( "null ID returned", id2 );
        cleanup.add( id1 );
        cleanup.add( id2 );

        // List the objects.  Make sure the one we created is in the list
        ListObjectsRequest lRequest = new ListObjectsRequest().metadataName( "listable" );
        lRequest.setIncludeMetadata( true );
        lRequest.setLimit( 1 );
        List<ObjectEntry> objects = this.api.listObjects( lRequest ).getEntries();
        Assert.assertTrue( "No objects returned", objects.size() > 0 );
        Assert.assertNotNull( "Token should be present", lRequest.getToken() );

        l4j.debug( "listObjectsPaged, Token: " + lRequest.getToken() );
        while ( lRequest.getToken() != null ) {
            // Subsequent pages
            objects.addAll( this.api.listObjects( lRequest ).getEntries() );
            l4j.debug( "listObjectsPaged, Token: " + lRequest.getToken() );
        }

        // Ensure our IDs exist
        ObjectEntry toFind1 = new ObjectEntry(), toFind2 = new ObjectEntry();
        toFind1.setObjectId( id1 );
        toFind2.setObjectId( id2 );
        Assert.assertTrue( "First object not found", objects.contains( toFind1 ) );
        Assert.assertTrue( "Second object not found", objects.contains( toFind2 ) );
    }


    /**
     * Test fetching listable tags
     */
    @Test
    public void testGetListableTags() {
        // Create an object
        ObjectId id = this.api.createObject( null, null );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        UpdateObjectRequest request = new UpdateObjectRequest().identifier( id );
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "list/able/2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "list/able/not", "bar2 bar2", false );
        request.userMetadata( listable, unlistable, listable2, unlistable2 );
        this.api.updateObject( request );

        // List tags.  Ensure our object's tags are in the list.
        Set<String> tags = this.api.listMetadata( null );
        Assert.assertTrue( "listable tag not returned", tags.contains( "listable" ) );
        Assert.assertTrue( "list/able/2 root tag not returned", tags.contains( "list" ) );
        Assert.assertFalse( "list/able/not tag returned", tags.contains( "list/able/not" ) );

        // List child tags
        tags = this.api.listMetadata( "list/able" );
        Assert.assertFalse( "non-child returned", tags.contains( "listable" ) );
        Assert.assertTrue( "list/able/2 tag not returned", tags.contains( "2" ) );
        Assert.assertFalse( "list/able/not tag returned", tags.contains( "not" ) );

    }

    /**
     * Test listing the user metadata tags on an object
     */
    @Test
    public void testListUserMetadataTags() {
        // Create an object
        CreateObjectRequest request = new CreateObjectRequest();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "list/able/2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "list/able/not", "bar2 bar2", false );
        request.userMetadata( listable, unlistable, listable2, unlistable2 );

        ObjectId id = this.api.createObject( request ).getObjectId();
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // List tags
        Map<String, Boolean> metaNames = this.api.getUserMetadataNames( id );
        Assert.assertTrue( "listable tag not returned", metaNames.containsKey( "listable" ) );
        Assert.assertTrue( "list/able/2 tag not returned", metaNames.containsKey( "list/able/2" ) );
        Assert.assertTrue( "unlistable tag not returned", metaNames.containsKey( "unlistable" ) );
        Assert.assertTrue( "list/able/not tag not returned", metaNames.containsKey( "list/able/not" ) );
        Assert.assertFalse( "unknown tag returned", metaNames.containsKey( "unknowntag" ) );

        // Check listable flag
        Assert.assertEquals( "'listable' is not listable", true, metaNames.get( "listable" ) );
        Assert.assertEquals( "'list/able/2' is not listable", true, metaNames.get( "list/able/2" ) );
        Assert.assertEquals( "'unlistable' is listable", false, metaNames.get( "unlistable" ) );
        Assert.assertEquals( "'list/able/not' is listable", false, metaNames.get( "list/able/not" ) );
    }

    /**
     * Tests updating an object's metadata
     */
    @Test
    public void testUpdateObjectMetadata() throws Exception {
        // Create an object
        CreateObjectRequest request = new CreateObjectRequest().content( "hello".getBytes( "UTF-8" ) );
        Metadata unlistable = new Metadata( "unlistable", "foo", false );
        request.userMetadata( unlistable );
        ObjectId id = this.api.createObject( request ).getObjectId();
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Update the metadata
        unlistable.setValue( "bar" );
        this.api.setUserMetadata( id,
                                  request.getUserMetadata().toArray( new Metadata[request.getUserMetadata().size()] ) );

        // Re-read the metadata
        Map<String, Metadata> meta = this.api.getUserMetadata( id );
        Assert.assertEquals( "value of 'unlistable' wrong", "bar", meta.get( "unlistable" ).getValue() );

        // Check that content was not modified
        String content = new String( this.api.readObject( id, null, byte[].class ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "hello", content );

    }

    @Test
    public void testUpdateObjectAcl() throws Exception {
        // Create an object with an ACL
        Acl acl = new Acl();
        acl.addUserGrant( stripUid( config.getTokenId() ), Permission.FULL_CONTROL );
        acl.addGroupGrant( Acl.GROUP_OTHER, Permission.READ );
        ObjectId id = this.api.createObject( new CreateObjectRequest().acl( acl ) ).getObjectId();
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read back the ACL and make sure it matches
        Acl newacl = this.api.getAcl( id );
        l4j.info( "Comparing " + newacl + " with " + acl );

        Assert.assertEquals( "ACLs don't match", acl, newacl );

        // Change the ACL and update the object.
        acl.removeGroupGrant( Acl.GROUP_OTHER );
        acl.addGroupGrant( Acl.GROUP_OTHER, Permission.NONE );
        this.api.setAcl( id, acl );

        // Read the ACL back and check it
        newacl = this.api.getAcl( id );
        l4j.info( "Comparing " + newacl + " with " + acl );
        Assert.assertEquals( "ACLs don't match", acl, newacl );
    }

    /**
     * Tests updating an object's contents
     */
    @Test
    public void testUpdateObjectContent() throws Exception {
        // Create an object
        ObjectId id = this.api.createObject( "hello".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Update part of the content
        Range range = new Range( 1, 1 );
        this.api.updateObject( id, "u".getBytes( "UTF-8" ), range );

        // Read back the content and check it
        String content = new String( this.api.readObject( id, null, byte[].class ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "hullo", content );
    }

    @Test
    public void testUpdateObjectContentStream() throws Exception {
        // Create an object
        ObjectId id = this.api.createObject( "hello".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Update part of the content
        InputStream in = new ByteArrayInputStream( "u".getBytes( "UTF-8" ) );
        UpdateObjectRequest request = new UpdateObjectRequest().identifier( id );
        request.range( new Range( 1, 1 ) ).content( in );
        request.setContentLength( 1 );
        this.api.updateObject( request );
        in.close();

        // Read back the content and check it
        String content = new String( this.api.readObject( id, null, byte[].class ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "hullo", content );
    }

    /**
     * Test replacing an object's entire contents
     */
    @Test
    public void testReplaceObjectContent() throws Exception {
        // Create an object
        ObjectId id = this.api.createObject( "hello".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Update all of the content
        this.api.updateObject( id, "bonjour".getBytes( "UTF-8" ) );

        // Read back the content and check it
        String content = new String( this.api.readObject( id, null, byte[].class ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "bonjour", content );
    }

    @Test
    public void testListDirectory() throws Exception {
        String dir = rand8char();
        String file = rand8char();
        String dir2 = rand8char();
        ObjectPath dirPath = new ObjectPath( "/" + dir + "/" );
        ObjectPath op = new ObjectPath( "/" + dir + "/" + file );
        ObjectPath dirPath2 = new ObjectPath( "/" + dir + "/" + dir2 + "/" );

        ObjectId dirId = this.api.createDirectory( dirPath );
        ObjectId id = this.api.createObject( op, null, null );
        this.api.createDirectory( dirPath2 );
        cleanup.add( op );
        cleanup.add( dirPath2 );
        cleanup.add( dirPath );
        l4j.debug( "Path: " + op + " ID: " + id );
        Assert.assertNotNull( id );
        Assert.assertNotNull( dirId );

        // Read back the content
        String content = new String( this.api.readObject( op, null, byte[].class ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "", content );
        content = new String( this.api.readObject( id, null, byte[].class ), "UTF-8" );
        Assert.assertEquals( "object content wrong when reading by id", "", content );

        // List the parent path
        List<DirectoryEntry> dirList = api.listDirectory( new ListDirectoryRequest().path( dirPath ) ).getEntries();
        l4j.debug( "Dir content: " + content );
        Assert.assertTrue( "File not found in directory", directoryContains( dirList, op.getFilename() ) );
        Assert.assertTrue( "subdirectory not found in directory",
                           directoryContains( dirList, dirPath2.getFilename() ) );
    }

    @Test
    public void testListDirectoryPaged() throws Exception {
        String dir = rand8char();
        String file = rand8char();
        String dir2 = rand8char();
        ObjectPath dirPath = new ObjectPath( "/" + dir + "/" );
        ObjectPath op = new ObjectPath( "/" + dir + "/" + file );
        ObjectPath dirPath2 = new ObjectPath( "/" + dir + "/" + dir2 + "/" );

        ObjectId dirId = this.api.createDirectory( dirPath );
        ObjectId id = this.api.createObject( op, null, null );
        this.api.createDirectory( dirPath2 );
        cleanup.add( op );
        cleanup.add( dirPath2 );
        cleanup.add( dirPath );
        l4j.debug( "Path: " + op + " ID: " + id );
        Assert.assertNotNull( id );
        Assert.assertNotNull( dirId );

        // List the parent path
        ListDirectoryRequest request = new ListDirectoryRequest().path( dirPath );
        request.setLimit( 1 );
        List<DirectoryEntry> dirList = api.listDirectory( request ).getEntries();

        Assert.assertNotNull( "Token should have been returned", request.getToken() );
        l4j.debug( "listDirectoryPaged, token: " + request.getToken() );
        while ( request.getToken() != null ) {
            dirList.addAll( api.listDirectory( request ).getEntries() );
        }

        Assert.assertTrue( "File not found in directory", directoryContains( dirList, op.getFilename() ) );
        Assert.assertTrue( "subdirectory not found in directory",
                           directoryContains( dirList, dirPath2.getFilename() ) );
    }

    @Test
    public void testListDirectoryWithMetadata() throws Exception {
        String dir = rand8char();
        String file = rand8char();
        String dir2 = rand8char();
        ObjectPath dirPath = new ObjectPath( "/" + dir + "/" );
        ObjectPath op = new ObjectPath( "/" + dir + "/" + file );
        ObjectPath dirPath2 = new ObjectPath( "/" + dir + "/" + dir2 + "/" );

        CreateObjectRequest request = new CreateObjectRequest().identifier( op );
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "list/able/2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "list/able/not", "bar2 bar2", false );
        request.userMetadata( listable, unlistable, listable2, unlistable2 );

        ObjectId dirId = this.api.createDirectory( dirPath );
        ObjectId id = this.api.createObject( request ).getObjectId();
        this.api.createDirectory( dirPath2 );
        cleanup.add( op );
        cleanup.add( dirPath2 );
        cleanup.add( dirPath );
        l4j.debug( "Path: " + op + " ID: " + id );
        Assert.assertNotNull( id );
        Assert.assertNotNull( dirId );

        // List the parent path
        ListDirectoryRequest lRequest = new ListDirectoryRequest().path( dirPath ).includeMetadata( true );
        List<DirectoryEntry> dirList = api.listDirectory( lRequest ).getEntries();
        Assert.assertTrue( "File not found in directory", directoryContains( dirList, op.getFilename() ) );
        Assert.assertTrue( "subdirectory not found in directory",
                           directoryContains( dirList, dirPath2.getFilename() ) );

        for ( DirectoryEntry de : dirList ) {
            if ( new ObjectPath( dirPath, de.getFilename() ).equals( op ) ) {
                // Check the metadata
                Assert.assertEquals( "Wrong value on metadata",
                                     de.getUserMetadataMap().get( "listable" ).getValue(), "foo" );

            }
        }
        Assert.assertTrue( "File not found in directory", directoryContains( dirList, op.getFilename() ) );
        Assert.assertTrue( "subdirectory not found in directory",
                           directoryContains( dirList, dirPath2.getFilename() ) );
    }

    @Test
    public void testListDirectoryWithSomeMetadata() throws Exception {
        String dir = rand8char();
        String file = rand8char();
        String dir2 = rand8char();
        ObjectPath dirPath = new ObjectPath( "/" + dir + "/" );
        ObjectPath op = new ObjectPath( "/" + dir + "/" + file );
        ObjectPath dirPath2 = new ObjectPath( "/" + dir + "/" + dir2 + "/" );

        CreateObjectRequest request = new CreateObjectRequest().identifier( op );
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "list/able/2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "list/able/not", "bar2 bar2", false );
        request.userMetadata( listable, unlistable, listable2, unlistable2 );

        ObjectId dirId = this.api.createDirectory( dirPath );
        ObjectId id = this.api.createObject( request ).getObjectId();
        this.api.createDirectory( dirPath2 );
        cleanup.add( op );
        cleanup.add( dirPath2 );
        cleanup.add( dirPath );
        l4j.debug( "Path: " + op + " ID: " + id );
        Assert.assertNotNull( id );
        Assert.assertNotNull( dirId );

        // List the parent path
        ListDirectoryRequest lRequest = new ListDirectoryRequest().path( dirPath ).includeMetadata( true );
        lRequest.userMetadataNames( "listable" );
        List<DirectoryEntry> dirList = api.listDirectory( lRequest ).getEntries();
        Assert.assertTrue( "File not found in directory", directoryContains( dirList, op.getFilename() ) );
        Assert.assertTrue( "subdirectory not found in directory",
                           directoryContains( dirList, dirPath2.getFilename() ) );

        for ( DirectoryEntry de : dirList ) {
            if ( new ObjectPath( dirPath, de.getFilename() ).equals( op ) ) {
                // Check the metadata
                Assert.assertEquals( "Wrong value on metadata",
                                     de.getUserMetadataMap().get( "listable" ).getValue(), "foo" );
                // Other metadata should not be present
                Assert.assertNull( "unlistable should be missing",
                                   de.getUserMetadataMap().get( "unlistable" ) );
            }
        }
        Assert.assertTrue( "File not found in directory", directoryContains( dirList, op.getFilename() ) );
        Assert.assertTrue( "subdirectory not found in directory",
                           directoryContains( dirList, dirPath2.getFilename() ) );
    }


    private boolean directoryContains( List<DirectoryEntry> dir, String filename ) {
        for ( DirectoryEntry de : dir ) {
            if ( de.getFilename().equals( filename ) ) {
                return true;
            }
        }

        return false;
    }

    /**
     * This method tests various legal and illegal pathnames
     *
     * @throws Exception
     */
    @Test
    public void testPathNaming() throws Exception {
        ObjectPath path = new ObjectPath( "/some/file" );
        Assert.assertFalse( "File should not be directory", path.isDirectory() );
        path = new ObjectPath( "/some/file.txt" );
        Assert.assertFalse( "File should not be directory", path.isDirectory() );
        ObjectPath path2 = new ObjectPath( "/some/file.txt" );
        Assert.assertEquals( "Equal paths should be equal", path, path2 );

        path = new ObjectPath( "/some/file/with/long.path/extra.stuff.here.zip" );
        Assert.assertFalse( "File should not be directory", path.isDirectory() );

        path = new ObjectPath( "/" );
        Assert.assertTrue( "Directory should be directory", path.isDirectory() );

        path = new ObjectPath( "/long/path/with/lots/of/elements/" );
        Assert.assertTrue( "Directory should be directory", path.isDirectory() );

    }

    /**
     * Tests dot directories (you should be able to create them even though they break the URL specification.)
     *
     * @throws Exception
     */
    @Test
    public void testDotDirectories() throws Exception {
        String parentPath = TESTDIR + "dottest/";
        String dotPath = parentPath + "./";
        String dotdotPath = parentPath + "../";
        String filename = rand8char();
        byte[] content = "Hello World!".getBytes( "UTF-8" );

        // isolate this test in the namespace
        ObjectId parentId = this.api.createDirectory( new ObjectPath( parentPath ) );

        // test single dot path (./)
        ObjectId fileId = this.api.createObject( new ObjectPath( parentPath + rand8char() ), content, "text/plain" );
        cleanup.add( fileId );
        ObjectId dirId = this.api.createDirectory( new ObjectPath( dotPath ) );
        Assert.assertNotNull( "null ID returned on dot path creation", dirId );
        fileId = this.api.createObject( new ObjectPath( dotPath + filename ), content, "text/plain" );

        // make sure we only see one file (the "." path is its own directory and not a synonym for the current directory)
        List<DirectoryEntry> entries = this.api
                .listDirectory( new ListDirectoryRequest().path( new ObjectPath( dotPath ) ) ).getEntries();
        Assert.assertEquals( "dot path listing was not 1", entries.size(), 1 );
        Assert.assertEquals( "dot path listing did not contain test file",
                             entries.get( 0 ).getFilename(),
                             filename );
        cleanup.add( fileId );
        cleanup.add( dirId );

        // test double dot path (../)
        dirId = this.api.createDirectory( new ObjectPath( dotdotPath ) );
        Assert.assertNotNull( "null ID returned on dotdot path creation", dirId );
        fileId = this.api.createObject( new ObjectPath( dotdotPath + filename ), content, "text/plain" );

        // make sure we only see one file (the ".." path is its own directory and not a synonym for the parent directory)
        entries = this.api
                .listDirectory( new ListDirectoryRequest().path( new ObjectPath( dotdotPath ) ) ).getEntries();
        Assert.assertEquals( "dotdot path listing was not 1", entries.size(), 1 );
        Assert.assertEquals( "dotdot path listing did not contain test file",
                             entries.get( 0 ).getFilename(),
                             filename );
        cleanup.add( fileId );
        cleanup.add( dirId );
        cleanup.add( parentId );
    }

    /**
     * Tests the 'get all metadata' call using a path
     *
     * @throws Exception
     */
    @Test
    public void testGetAllMetadataByPath() throws Exception {
        ObjectPath op = new ObjectPath( "/" + rand8char() + ".tmp" );
        String mimeType = "test/mimetype";

        // Create an object with an ACL
        Acl acl = new Acl();
        acl.addUserGrant( stripUid( config.getTokenId() ), Permission.FULL_CONTROL );
        acl.addGroupGrant( Acl.GROUP_OTHER, Permission.READ );
        CreateObjectRequest request = new CreateObjectRequest().identifier( op ).acl( acl );
        request.content( "test".getBytes( "UTF-8" ) ).contentType( mimeType );

        ObjectId id = this.api.createObject( request ).getObjectId();
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( op );

        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "listable2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "unlistable2", "bar2 bar2", false );

        this.api.updateObject( new UpdateObjectRequest().identifier( op )
                                                        .userMetadata( listable, unlistable, listable2, unlistable2 )
                                                        .contentType( mimeType ) );

        // Read it back with HEAD call
        ObjectMetadata om = this.api.getObjectMetadata( op );
        Assert.assertNotNull( "value of 'listable' missing", om.getMetadata().get( "listable" ) );
        Assert.assertNotNull( "value of 'unlistable' missing", om.getMetadata().get( "unlistable" ) );
        Assert.assertNotNull( "value of 'atime' missing", om.getMetadata().get( "atime" ) );
        Assert.assertNotNull( "value of 'ctime' missing", om.getMetadata().get( "ctime" ) );
        Assert.assertEquals( "value of 'listable' wrong", "foo", om.getMetadata().get( "listable" ).getValue() );
        Assert.assertEquals( "value of 'unlistable' wrong", "bar", om.getMetadata().get( "unlistable" ).getValue() );
        Assert.assertEquals( "Mimetype incorrect", mimeType, om.getContentType() );

        // Check the ACL
        // not checking this by path because an extra groupid is added
        // during the create calls by path.
        //Assert.assertEquals( "ACLs don't match", acl, om.getAcl() );
    }

    @Test
    public void testGetAllMetadataById() throws Exception {
        // Create an object with an ACL
        CreateObjectRequest request = new CreateObjectRequest();

        Acl acl = new Acl();
        acl.addUserGrant( stripUid( config.getTokenId() ), Permission.FULL_CONTROL );
        acl.addGroupGrant( Acl.GROUP_OTHER, Permission.READ );

        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "listable2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "unlistable2", "bar2 bar2", false );

        String mimeType = "test/mimetype";
        request.acl( acl ).userMetadata( listable, unlistable, listable2, unlistable2 );
        request.content( "test".getBytes( "UTF-8" ) ).contentType( mimeType );

        ObjectId id = this.api.createObject( request ).getObjectId();
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read it back with HEAD call
        ObjectMetadata om = this.api.getObjectMetadata( id );
        Assert.assertNotNull( "value of 'listable' missing", om.getMetadata().get( "listable" ) );
        Assert.assertNotNull( "value of 'unlistable' missing", om.getMetadata().get( "unlistable" ) );
        Assert.assertNotNull( "value of 'atime' missing", om.getMetadata().get( "atime" ) );
        Assert.assertNotNull( "value of 'ctime' missing", om.getMetadata().get( "ctime" ) );
        Assert.assertEquals( "value of 'listable' wrong", "foo", om.getMetadata().get( "listable" ).getValue() );
        Assert.assertEquals( "value of 'unlistable' wrong", "bar", om.getMetadata().get( "unlistable" ).getValue() );
        Assert.assertEquals( "Mimetype incorrect", mimeType, om.getContentType() );

        // Check the ACL
        Assert.assertEquals( "ACLs don't match", acl, om.getAcl() );
    }

    /**
     * Tests getting object replica information.
     */
    @Test
    public void testGetObjectReplicaInfo() throws Exception {
        ObjectId id = this.api.createObject( "hello".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        Map<String, Metadata> meta = this.api.getUserMetadata( id, "user.maui.lso" );
        Assert.assertNotNull( meta.get( "user.maui.lso" ) );
        l4j.debug( "Replica info: " + meta.get( "user.maui.lso" ) );
    }

    @Test
    public void testGetShareableUrl() throws Exception {
        // Create an object with content.
        String str = "Four score and twenty years ago";
        ObjectId id = this.api.createObject( str.getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        Calendar c = Calendar.getInstance();
        c.add( Calendar.HOUR, 4 );
        Date expiration = c.getTime();
        URL u = api.getShareableUrl( id, expiration );

        l4j.debug( "Sharable URL: " + u );

        InputStream stream = (InputStream) u.getContent();
        BufferedReader br = new BufferedReader( new InputStreamReader( stream ) );
        String content = br.readLine();
        l4j.debug( "Content: " + content );
        Assert.assertEquals( "URL does not contain proper content",
                             str, content );
    }

    @Test
    public void testGetShareableUrlWithPath() throws Exception {
        // Create an object with content.
        String str = "Four score and twenty years ago";
        ObjectPath op = new ObjectPath( "/" + rand8char() + ".txt" );
        ObjectId id = this.api.createObject( op, str.getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( op );

        Calendar c = Calendar.getInstance();
        c.add( Calendar.HOUR, 4 );
        Date expiration = c.getTime();
        URL u = api.getShareableUrl( op, expiration );

        l4j.debug( "Sharable URL: " + u );

        InputStream stream = (InputStream) u.getContent();
        BufferedReader br = new BufferedReader( new InputStreamReader( stream ) );
        String content = br.readLine();
        l4j.debug( "Content: " + content );
        Assert.assertEquals( "URL does not contain proper content",
                             str, content );
    }

    @Test
    public void testExpiredSharableUrl() throws Exception {
        // Create an object with content.
        String str = "Four score and twenty years ago";
        ObjectId id = this.api.createObject( str.getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        Calendar c = Calendar.getInstance();
        c.add( Calendar.HOUR, -4 );
        Date expiration = c.getTime();
        URL u = api.getShareableUrl( id, expiration );

        l4j.debug( "Sharable URL: " + u );

        try {
            InputStream stream = (InputStream) u.getContent();
            BufferedReader br = new BufferedReader( new InputStreamReader( stream ) );
            String content = br.readLine();
            l4j.debug( "Content: " + content );
            Assert.fail( "Request should have failed" );
        } catch ( Exception e ) {
            l4j.debug( "Error (expected): " + e );
        }
    }

    @Test
    public void testReadObjectStream() throws Exception {
        ObjectId id = this.api.createObject( "hello".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read back the content
        InputStream in = this.api.readObjectStream( id, null ).getObject();
        BufferedReader br = new BufferedReader( new InputStreamReader( in, "UTF-8" ) );
        String content = br.readLine();
        br.close();
        Assert.assertEquals( "object content wrong", "hello", content );

        // Read back only 2 bytes
        Range range = new Range( 1, 2 );
        in = this.api.readObjectStream( id, range ).getObject();
        br = new BufferedReader( new InputStreamReader( in, "UTF-8" ) );
        content = br.readLine();
        br.close();
        Assert.assertEquals( "partial object content wrong", "el", content );
    }

    @Test
    public void testCreateChecksum() throws Exception {
        byte[] data = "hello".getBytes( "UTF-8" );
        RunningChecksum ck = new RunningChecksum( ChecksumAlgorithm.SHA0 );
        ck.update( data, 0, data.length );

        CreateObjectRequest request = new CreateObjectRequest().content( data ).contentType( "text/plain" );
        request.wsChecksum( ck );

        CreateObjectResponse response = this.api.createObject( request );
        cleanup.add( response.getObjectId() );
        Assert.assertNotNull( "Null object ID returned", response.getObjectId() );
        Assert.assertEquals( "Checksum doesn't match", ck, response.getWsChecksum() );
    }

    /**
     * Note, to test read checksums, see comment in testReadChecksum
     *
     * @throws Exception
     */
    @Test
    public void testUploadDownloadChecksum() throws Exception {
        // Create a byte array to test
        int totalSize = 10 * 1024 * 1024; // 10MB
        int chunkSize = 4 * 1024 * 1024; // 4MB
        byte[] testData = new byte[totalSize]; // 10MB
        for ( int i = 0; i < testData.length; i++ ) {
            testData[i] = (byte) (i % 0x93);
        }

        RunningChecksum sha0 = new RunningChecksum( ChecksumAlgorithm.SHA0 );
        BufferSegment segment = new BufferSegment( testData, 0, chunkSize );

        // upload in chunks
        sha0.update( segment );
        l4j.debug( "Create checksum: " + sha0 );
        CreateObjectRequest request = new CreateObjectRequest();
        request.content( segment ).userMetadata( new Metadata( "policy", "erasure", false ) ).setWsChecksum( sha0 );
        ObjectId id = api.createObject( request ).getObjectId();
        cleanup.add( id );

        while ( segment.getOffset() + segment.getSize() < totalSize ) {
            segment.setOffset( segment.getOffset() + chunkSize );
            if ( segment.getOffset() + chunkSize > totalSize ) segment.setSize( totalSize - segment.getOffset() );
            Range range = new Range( segment.getOffset(), segment.getOffset() + segment.getSize() - 1 );
            sha0.update( segment.getBuffer(), segment.getOffset(), segment.getSize() );
            l4j.debug( "Update checksum: " + sha0 );
            api.updateObject( new UpdateObjectRequest().identifier( id ).range( range )
                                                       .content( segment ).wsChecksum( sha0 ) );
        }

        // download in chunks
        totalSize = Integer.parseInt( api.getSystemMetadata( id ).get( "size" ).getValue() );

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int first = 0, last = chunkSize - 1;
        Range range = new Range( first, last );
        ReadObjectResponse<byte[]> response;
        RunningChecksum readSha0 = new RunningChecksum( ChecksumAlgorithm.SHA0 );
        do {
            response = api.readObject( new ReadObjectRequest().identifier( id ).ranges( range ),
                                       byte[].class );
            readSha0.update( response.getObject(), 0, response.getObject().length );
            out.write( response.getObject() );
            first += chunkSize;
            last += chunkSize;
            if ( last >= totalSize ) last = totalSize - 1;
            range = new Range( first, last );
        } while ( first < totalSize );

        byte[] outData = out.toByteArray();

        // verify checksum
        Assert.assertEquals( "Write checksum doesn't match read checksum", sha0, readSha0 );
        Assert.assertEquals( "Read checksum doesn't match", readSha0, response.getWsChecksum() );

        // Check the files
        Assert.assertEquals( "File lengths differ", testData.length, outData.length );
        Assert.assertArrayEquals( "Data contents differ", testData, outData );
    }

    @Ignore("TODO: Figure out why this fails")
    @Test
    public void testUtf8WhiteSpaceValues() throws Exception {
        String utf8String = "Hello ,\u0080 \r \u000B \t \n \t";

        CreateObjectRequest request = new CreateObjectRequest();
        request.userMetadata( new Metadata( "utf8Key", utf8String, false ) );

        ObjectId id = this.api.createObject( request ).getObjectId();
        cleanup.add( id );

        // get the user metadata and make sure all UTF8 characters are accurate
        Map<String, Metadata> metaMap = this.api.getUserMetadata( id );
        Assert.assertEquals( "UTF8 value does not match", utf8String, metaMap.get( "utf8Key" ).getValue() );

        // test set metadata with UTF8
        this.api.setUserMetadata( id, new Metadata( "newKey", utf8String + "2", false ) );

        // verify set metadata call (also testing getAllMetadata)
        ObjectMetadata objMeta = this.api.getObjectMetadata( id );
        metaMap = objMeta.getMetadata();
        //Assert.assertEquals( "UTF8 key does not match", meta.getName(), whiteSpaceString + "2" );
        //Assert.assertEquals( "UTF8 key value does not match", meta.getValue(), "newValue" );
        Assert.assertEquals( "UTF8 value does not match",
                             utf8String + "2",
                             metaMap.get( "newKey" ).getValue() );
    }

    @Test
    public void testUnicodeMetadata() throws Exception {
        CreateObjectRequest request = new CreateObjectRequest();

        Metadata nbspValue = new Metadata( "nbspvalue", "Nobreak\u00A0Value", false );
        Metadata nbspName = new Metadata( "Nobreak\u00A0Name", "regular text here", false );
        Metadata cryllic = new Metadata( "cryllic", "спасибо", false );
        l4j.debug( "NBSP Value: " + nbspValue );
        l4j.debug( "NBSP Name: " + nbspName );

        request.userMetadata( nbspValue, nbspName, cryllic );

        ObjectId id = this.api.createObject( request ).getObjectId();
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read and validate the metadata
        Map<String, Metadata> meta = this.api.getUserMetadata( id );
        l4j.debug( "Read Back:" );
        l4j.debug( "NBSP Value: " + meta.get( "nbspvalue" ) );
        l4j.debug( "NBSP Name: " + meta.get( "Nobreak\u00A0Name" ) );
        Assert.assertEquals( "value of 'nobreakvalue' wrong",
                             "Nobreak\u00A0Value",
                             meta.get( "nbspvalue" ).getValue() );
        Assert.assertEquals( "Value of cryllic wrong", "спасибо", meta.get( "cryllic" ).getValue() );
    }

    @Test
    public void testUtf8Metadata() throws Exception {
        String oneByteCharacters = "Hello! ";
        String twoByteCharacters = "\u0410\u0411\u0412\u0413"; // Cyrillic letters
        String fourByteCharacters = "\ud841\udf0e\ud841\udf31\ud841\udf79\ud843\udc53"; // Chinese symbols
        String utf8String = oneByteCharacters + twoByteCharacters + fourByteCharacters;

        CreateObjectRequest request = new CreateObjectRequest();
        request.userMetadata( new Metadata( "utf8Key", utf8String, false ),
                              new Metadata( utf8String, "utf8Value", false ) );

        ObjectId id = this.api.createObject( request ).getObjectId();
        cleanup.add( id );

        // list all tags and make sure the UTF8 tag is in the list
        Map<String, Boolean> tags = this.api.getUserMetadataNames( id );
        Assert.assertTrue( "UTF8 key not found in tag list", tags.containsKey( utf8String ) );

        // get the user metadata and make sure all UTF8 characters are accurate
        Map<String, Metadata> metaMap = this.api.getUserMetadata( id );
        Metadata meta = metaMap.get( utf8String );
        Assert.assertEquals( "UTF8 key does not match", meta.getName(), utf8String );
        Assert.assertEquals( "UTF8 key value does not match", meta.getValue(), "utf8Value" );
        Assert.assertEquals( "UTF8 value does not match", metaMap.get( "utf8Key" ).getValue(), utf8String );

        // test set metadata with UTF8
        this.api.setUserMetadata( id, new Metadata( "newKey", utf8String + "2", false ),
                                  new Metadata( utf8String + "2", "newValue", false ) );

        // verify set metadata call (also testing getAllMetadata)
        ObjectMetadata objMeta = this.api.getObjectMetadata( id );
        metaMap = objMeta.getMetadata();
        meta = metaMap.get( utf8String + "2" );
        Assert.assertEquals( "UTF8 key does not match", meta.getName(), utf8String + "2" );
        Assert.assertEquals( "UTF8 key value does not match", meta.getValue(), "newValue" );
        Assert.assertEquals( "UTF8 value does not match",
                             metaMap.get( "newKey" ).getValue(),
                             utf8String + "2" );
    }

    @Test
    public void testUtf8MetadataFilter() throws Exception {
        String oneByteCharacters = "Hello! ";
        String twoByteCharacters = "\u0410\u0411\u0412\u0413"; // Cyrillic letters
        String fourByteCharacters = "\ud841\udf0e\ud841\udf31\ud841\udf79\ud843\udc53"; // Chinese symbols
        String utf8String = oneByteCharacters + twoByteCharacters + fourByteCharacters;

        CreateObjectRequest request = new CreateObjectRequest();
        request.userMetadata( new Metadata( "utf8Key", utf8String, false ) )
               .userMetadata( new Metadata( utf8String, "utf8Value", false ) );

        ObjectId id = this.api.createObject( request ).getObjectId();
        cleanup.add( id );

        // apply a filter that includes the UTF8 tag
        Map<String, Metadata> metaMap = this.api.getUserMetadata( id, utf8String );
        Assert.assertEquals( "UTF8 filter was not honored", metaMap.size(), 1 );
        Assert.assertNotNull( "UTF8 key was not found in filtered results", metaMap.get( utf8String ) );
    }

    @Test
    public void testUtf8DeleteMetadata() throws Exception {
        String oneByteCharacters = "Hello! ";
        String twoByteCharacters = "\u0410\u0411\u0412\u0413"; // Cyrillic letters
        String fourByteCharacters = "\ud841\udf0e\ud841\udf31\ud841\udf79\ud843\udc53"; // Chinese symbols
        String utf8String = oneByteCharacters + twoByteCharacters + fourByteCharacters;

        CreateObjectRequest request = new CreateObjectRequest();
        request.userMetadata( new Metadata( "utf8Key", utf8String, false ) )
               .userMetadata( new Metadata( utf8String, "utf8Value", false ) );

        ObjectId id = this.api.createObject( request ).getObjectId();
        cleanup.add( id );

        // delete the UTF8 tag
        this.api.deleteUserMetadata( id, utf8String );

        // verify delete was successful
        Map<String, Boolean> nameMap = this.api.getUserMetadataNames( id );
        Assert.assertFalse( "UTF8 key was not deleted", nameMap.containsKey( utf8String ) );
    }

    @Test
    public void testUtf8ListableMetadata() throws Exception {
        String oneByteCharacters = "Hello! ";
        String twoByteCharacters = "\u0410\u0411\u0412\u0413"; // Cyrillic letters
        String fourByteCharacters = "\ud841\udf0e\ud841\udf31\ud841\udf79\ud843\udc53"; // Chinese symbols
        String utf8String = oneByteCharacters + twoByteCharacters + fourByteCharacters;

        CreateObjectRequest request = new CreateObjectRequest();
        request.userMetadata( new Metadata( utf8String, "utf8Value", true ) );

        ObjectId id = this.api.createObject( request ).getObjectId();
        cleanup.add( id );

        Map<String, Metadata> metaMap = this.api.getUserMetadata( id );
        Metadata meta = metaMap.get( utf8String );
        Assert.assertEquals( "UTF8 key does not match", meta.getName(), utf8String );
        Assert.assertEquals( "UTF8 key value does not match", meta.getValue(), "utf8Value" );
        Assert.assertTrue( "UTF8 metadata is not listable", meta.isListable() );

        // verify we can list the tag and see our object
        boolean found = false;
        for ( ObjectEntry result : this.api
                .listObjects( new ListObjectsRequest().metadataName( utf8String ) ).getEntries() ) {
            if ( result.getObjectId().equals( id ) ) {
                found = true;
                break;
            }
        }
        Assert.assertTrue( "UTF8 tag listing did not contain the correct object ID", found );

        // verify we can list child tags of the UTF8 tag
        Set<String> tags = this.api.listMetadata( utf8String );
        Assert.assertNotNull( "UTF8 child tag listing was null", tags );
    }

    @Test
    public void testUtf8ListableTagWithComma() {
        String stringWithComma = "Hello, you!";

        CreateObjectRequest request = new CreateObjectRequest();
        request.userMetadata( new Metadata( stringWithComma, "value", true ) );

        ObjectId id = this.api.createObject( request ).getObjectId();
        cleanup.add( id );

        Map<String, Metadata> metaMap = this.api.getUserMetadata( id );
        Metadata meta = metaMap.get( stringWithComma );
        Assert.assertEquals( "key does not match", meta.getName(), stringWithComma );
        Assert.assertTrue( "metadata is not listable", meta.isListable() );

        boolean found = false;
        for ( ObjectEntry result : this.api
                .listObjects( new ListObjectsRequest().metadataName( stringWithComma ) ).getEntries() ) {
            if ( result.getObjectId().equals( id ) ) {
                found = true;
                break;
            }
        }
        Assert.assertTrue( "listing did not contain the correct object ID", found );
    }

    @Test
    public void testRename() throws Exception {
        ObjectPath op1 = new ObjectPath( "/" + rand8char() + ".tmp" );
        ObjectPath op2 = new ObjectPath( "/" + rand8char() + ".tmp" );

        ObjectId id = this.api.createObject( op1, "Four score and seven years ago".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Rename
        this.api.move( op1, op2, false );

        // Read back the content
        String content = new String( this.api.readObject( op2, null, byte[].class ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "Four score and seven years ago", content );

    }

    @Test
    public void testRenameOverwrite() throws Exception {
        ObjectPath op1 = new ObjectPath( "/" + rand8char() + ".tmp" );
        ObjectPath op2 = new ObjectPath( "/" + rand8char() + ".tmp" );

        ObjectId id = this.api.createObject( op1, "Four score and seven years ago".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        ObjectId id2 = this.api.createObject( op2, "You should not see this".getBytes( "UTF-8" ), "text/plain" );
        cleanup.add( id2 );

        // Rename
        this.api.move( op1, op2, true );

        // Wait for overwrite to complete
        Thread.sleep( 5000 );

        // Read back the content
        String content = new String( this.api.readObject( op2, null, byte[].class ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "Four score and seven years ago", content );

    }

    /**
     * Tests renaming a path to UTF-8 multi-byte characters.  This is a separate test from create as the characters are
     * passed in the headers instead of the URL itself.
     *
     * @throws Exception
     */
    @Test
    public void testUtf8Rename() throws Exception {
        String oneByteCharacters = "Hello! ,";
        String twoByteCharacters = "\u0410\u0411\u0412\u0413"; // Cyrillic letters
        String fourByteCharacters = "\ud841\udf0e\ud841\udf31\ud841\udf79\ud843\udc53"; // Chinese symbols
        String normalName = TESTDIR + rand8char() + ".tmp";
        String crazyName = oneByteCharacters + twoByteCharacters + fourByteCharacters;
        String crazyPath = TESTDIR + crazyName;
        byte[] content = "This is a really crazy name.".getBytes( "UTF-8" );

        // normal name
        this.api.createObject( new ObjectPath( normalName ), content, "text/plain" );

        // crazy multi-byte character name
        this.api.move( new ObjectPath( normalName ), new ObjectPath( crazyPath ), true );

        // Wait for overwrite to complete
        Thread.sleep( 5000 );

        // verify name in directory list
        List<DirectoryEntry> entries = this.api
                .listDirectory( new ListDirectoryRequest().path( new ObjectPath( TESTDIR ) ) )
                .getEntries();

        Assert.assertTrue( "crazyName not found in directory listing", directoryContains( entries, crazyName ) );

        // Read back the content
        Assert.assertTrue( "object content wrong",
                           Arrays.equals( content,
                                          this.api.readObject( new ObjectPath( crazyPath ), null, byte[].class ) ) );
    }

    /**
     * Tests readback with checksum verification.  In order to test this, create a policy
     * with erasure coding and then set a policy selector with "policy=erasure" to invoke
     * the erasure coding policy.
     *
     * @throws Exception
     */
    @Test
    public void testReadChecksum() throws Exception {
        byte[] data = "hello".getBytes( "UTF-8" );
        Metadata policy = new Metadata( "policy", "erasure", false );
        RunningChecksum wsChecksum = new RunningChecksum( ChecksumAlgorithm.SHA0 );
        wsChecksum.update( data, 0, data.length );

        CreateObjectRequest request = new CreateObjectRequest().content( data ).contentType( "text/plain" );
        request.userMetadata( policy ).wsChecksum( wsChecksum );

        CreateObjectResponse response = this.api.createObject( request );
        Assert.assertNotNull( "null ID returned", response.getObjectId() );
        cleanup.add( response.getObjectId() );
        Assert.assertNotNull( "null ID returned", response.getObjectId() );
        Assert.assertEquals( "create checksums don't match", wsChecksum, response.getWsChecksum() );

        // Read back the content
        ReadObjectRequest readRequest = new ReadObjectRequest().identifier( response.getObjectId() );
        ReadObjectResponse<byte[]> readResponse = this.api.readObject( readRequest, byte[].class );
        Assert.assertArrayEquals( "object content wrong", data, readResponse.getObject() );
        Assert.assertEquals( "read checksums don't match", wsChecksum, readResponse.getWsChecksum() );
    }


    /**
     * Tests getting the service information
     */
    @Test
    public void testGetServiceInformation() throws Exception {
        ServiceInformation si = this.api.getServiceInformation();

        Assert.assertNotNull( "Atmos version is null", si.getAtmosVersion() );
    }

    /**
     * Test getting object info.  Note to fully run this testcase, you should
     * create a policy named 'retaindelete' that keys off of the metadata
     * policy=retaindelete that includes a retention and deletion criteria.
     */
    @Test
    public void testGetObjectInfo() throws Exception {
        CreateObjectRequest request = new CreateObjectRequest();
        request.content( "hello".getBytes( "UTF-8" ) ).contentType( "text/plain" );
        ObjectId id = this.api.createObject( request ).getObjectId();
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        api.setUserMetadata( id, new Metadata( "policy", "retain", false ) );

        // Read back the content
        String content = new String( this.api.readObject( id, null, byte[].class ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "hello", content );

        // Check policyname
        Map<String,Metadata> sysmeta = this.api.getSystemMetadata(id, "policyname");
        Assume.assumeTrue("policyname != retaindelete", "retaindelete".equals(sysmeta.get("policyname")));

        // Get the object info
        ObjectInfo oi = this.api.getObjectInfo( id );
        Assert.assertNotNull( "ObjectInfo null", oi );
        Assert.assertNotNull( "ObjectInfo expiration null", oi.getExpiration().getEndAt() );
        Assert.assertNotNull( "ObjectInfo objectid null", oi.getObjectId() );
        Assert.assertTrue( "ObjectInfo numReplicas is 0", oi.getNumReplicas() > 0 );
        Assert.assertNotNull( "ObjectInfo replicas null", oi.getReplicas() );
        Assert.assertNotNull( "ObjectInfo retention null", oi.getRetention().getEndAt() );
        Assert.assertNotNull( "ObjectInfo selection null", oi.getSelection() );
        Assert.assertTrue( "ObjectInfo should have at least one replica", oi.getReplicas().size() > 0 );

        api.setUserMetadata( id, new Metadata( "user.maui.retentionEnable", "false", false ) );
    }

    @Test
    public void testHmac() throws Exception {
        // Compute the signature hash
        String input = "Hello World";
        byte[] secret = Base64.decodeBase64( "D7qsp4j16PBHWSiUbc/bt3lbPBY=".getBytes( "UTF-8" ) );
        Mac mac = Mac.getInstance( "HmacSHA1" );
        SecretKeySpec key = new SecretKeySpec( secret, "HmacSHA1" );
        mac.init( key );
        l4j.debug( "Hashing: \n" + input );

        byte[] hashData = mac.doFinal( input.getBytes( "ISO-8859-1" ) );

        // Encode the hash in Base64.
        String hashOut = new String( Base64.encodeBase64( hashData ), "UTF-8" );

        l4j.debug( "Hash: " + hashOut );
    }

    @Test
    public void testDirectoryMetadata() throws Exception {
        ObjectPath dir = new ObjectPath( "/" + rand8char() + "/" );
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "listable2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "unlistable2", "bar2 bar2", false );
        Metadata listable3 = new Metadata( "listable3", null, true );
        Metadata withCommas = new Metadata( "withcommas", "I, Robot", false );
        Metadata withEquals = new Metadata( "withequals", "name=value", false );
        ObjectId id = this.api.createDirectory( dir, null, listable, unlistable, listable2, unlistable2, listable3,
                                                withCommas, withEquals );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read and validate the metadata
        Map<String, Metadata> metaMap = this.api.getObjectMetadata( dir ).getMetadata();
        Assert.assertEquals( "value of 'listable' wrong", "foo", metaMap.get( "listable" ).getValue() );
        Assert.assertEquals( "value of 'listable2' wrong", "foo2 foo2", metaMap.get( "listable2" ).getValue() );
        Assert.assertEquals( "value of 'unlistable' wrong", "bar", metaMap.get( "unlistable" ).getValue() );
        Assert.assertEquals( "value of 'unlistable2' wrong", "bar2 bar2", metaMap.get( "unlistable2" ).getValue() );
        Assert.assertNotNull( "listable3 missing", metaMap.get( "listable3" ) );
        Assert.assertTrue( "Value of listable3 should be empty",
                           metaMap.get( "listable3" ).getValue() == null
                           || metaMap.get( "listable3" ).getValue().length() == 0 );
        Assert.assertEquals( "Value of withcommas wrong", "I, Robot", metaMap.get( "withcommas" ).getValue() );
        Assert.assertEquals( "Value of withequals wrong", "name=value", metaMap.get( "withequals" ).getValue() );

        // Check listable flags
        Assert.assertEquals( "'listable' is not listable", true, metaMap.get( "listable" ).isListable() );
        Assert.assertEquals( "'listable2' is not listable", true, metaMap.get( "listable2" ).isListable() );
        Assert.assertEquals( "'listable3' is not listable", true, metaMap.get( "listable3" ).isListable() );
        Assert.assertEquals( "'unlistable' is listable", false, metaMap.get( "unlistable" ).isListable() );
        Assert.assertEquals( "'unlistable2' is listable", false, metaMap.get( "unlistable2" ).isListable() );
    }

    /**
     * Tests fetching data with multiple ranges.
     */
    @Test
    public void testMultipleRanges() throws Exception {
        String input = "Four score and seven years ago";
        ObjectId id = api.createObject( input.getBytes( "UTF-8" ), "text/plain" );
        cleanup.add( id );
        Assert.assertNotNull( "Object null", id );

        Range[] ranges = new Range[5];
        ranges[0] = new Range( 27, 28 ); //ag
        ranges[1] = new Range( 9, 9 ); // e
        ranges[2] = new Range( 5, 5 ); // s
        ranges[3] = new Range( 4, 4 ); // ' '
        ranges[4] = new Range( 27, 29 ); // ago

        ReadObjectResponse<MultipartEntity> response = api.readObject( new ReadObjectRequest().identifier( id )
                                                                                              .ranges( ranges ),
                                                                       MultipartEntity.class );
        String out = new String( response.getObject().aggregateBytes(), "UTF-8" );
        Assert.assertEquals( "Content incorrect", "ages ago", out );
    }

    //---------- Features supported by the Atmos 2.0 REST API. ----------\\

    @Test
    public void testGetShareableUrlAndDisposition() throws Exception {
        // Create an object with content.
        String str = "Four score and twenty years ago";
        ObjectId id = this.api.createObject( str.getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        String disposition = "attachment; filename=\"foo bar.txt\"";

        Calendar c = Calendar.getInstance();
        c.add( Calendar.HOUR, 4 );
        Date expiration = c.getTime();
        URL u = this.api.getShareableUrl( id, expiration, disposition );

        l4j.debug( "Sharable URL: " + u );

        InputStream stream = (InputStream) u.getContent();
        BufferedReader br = new BufferedReader( new InputStreamReader( stream ) );
        String content = br.readLine();
        l4j.debug( "Content: " + content );
        Assert.assertEquals( "URL does not contain proper content",
                             str, content );
    }

    @Test
    public void testGetShareableUrlWithPathAndDisposition() throws Exception {
        // Create an object with content.
        String str = "Four score and twenty years ago";
        ObjectPath op = new ObjectPath( "/" + rand8char() + ".txt" );
        ObjectId id = this.api.createObject( op, str.getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        //cleanup.add( op );

        String disposition = "attachment; filename=\"foo bar.txt\"";

        Calendar c = Calendar.getInstance();
        c.add( Calendar.HOUR, 4 );
        Date expiration = c.getTime();
        URL u = this.api.getShareableUrl( op, expiration, disposition );

        l4j.debug( "Sharable URL: " + u );

        InputStream stream = (InputStream) u.getContent();
        BufferedReader br = new BufferedReader( new InputStreamReader( stream ) );
        String content = br.readLine();
        l4j.debug( "Content: " + content );
        Assert.assertEquals( "URL does not contain proper content",
                             str, content );
    }

    @Test
    public void testGetShareableUrlWithPathAndUTF8Disposition() throws Exception {
        // Create an object with content.
        String str = "Four score and twenty years ago";
        ObjectPath op = new ObjectPath( "/" + rand8char() + ".txt" );
        ObjectId id = this.api.createObject( op, str.getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        //cleanup.add( op );

        // One cryllic, one accented, and one japanese character
        // RFC5987
        String disposition = "attachment; filename=\"no UTF support.txt\"; filename*=UTF-8''" + URLEncoder.encode(
                "бöｼ.txt",
                "UTF-8" );

        Calendar c = Calendar.getInstance();
        c.add( Calendar.HOUR, 4 );
        Date expiration = c.getTime();
        URL u = this.api.getShareableUrl( op, expiration, disposition );

        l4j.debug( "Sharable URL: " + u );

        InputStream stream = (InputStream) u.getContent();
        BufferedReader br = new BufferedReader( new InputStreamReader( stream ) );
        String content = br.readLine();
        l4j.debug( "Content: " + content );
        Assert.assertEquals( "URL does not contain proper content",
                             str, content );
    }

    @Test
    public void testGetServiceInformationFeatures() throws Exception {
        ServiceInformation info = this.api.getServiceInformation();
        l4j.info( "Supported features: " + info.getFeatures() );

        Assert.assertTrue( "Expected at least one feature", info.getFeatures().size() > 0 );

    }

    @Test
    public void testBug23750() throws Exception {
        byte[] data = new byte[1000];
        Arrays.fill( data, (byte) 0 );
        Metadata meta = new Metadata( "test", null, true );

        RunningChecksum sha1 = new RunningChecksum( ChecksumAlgorithm.SHA1 );
        sha1.update( data, 0, data.length );
        CreateObjectResponse response = this.api.createObject(
                new CreateObjectRequest().content( data ).wsChecksum( sha1 ).userMetadata( meta ) );

        try {
            Range range = new Range( 1000, 1999 );
            RunningChecksum sha0 = new RunningChecksum( ChecksumAlgorithm.SHA0 );
            sha0.update( data, 0, 1000 );
            this.api.updateObject( new UpdateObjectRequest().identifier( response.getObjectId() ).content( data )
                                                            .range( range ).wsChecksum( sha0 ).userMetadata( meta ) );

            Assert.fail( "Should have triggered an exception" );
        } catch ( AtmosException e ) {
            // expected
        }
    }

    @Test
    public void testCrudKeys() throws Exception {
        ObjectKey key = new ObjectKey( "Test_key-pool#@!$%^..", "KEY_TEST" );
        String content = "Hello World!";

        CreateObjectRequest request = new CreateObjectRequest().identifier( key );
        request.content( content.getBytes( "UTF-8" ) ).contentType( "text/plain" );

        ObjectId oid = this.api.createObject( request ).getObjectId();
        Assert.assertNotNull( "Null object ID returned", oid );
        cleanup.add( oid );

        String readContent = new String( this.api.readObject( key, null, byte[].class ), "UTF-8" );
        Assert.assertEquals( "content mismatch", content, readContent );

        content = "Hello Waldo!";
        this.api.updateObject( new UpdateObjectRequest().identifier( key ).content( content.getBytes( "UTF-8" ) ) );

        readContent = new String( this.api.readObject( key, null, byte[].class ), "UTF-8" );
        Assert.assertEquals( "content mismatch", content, readContent );

        this.api.delete( key );

        try {
            this.api.readObject( key, null, byte[].class );
            Assert.fail( "Object still exists" );
        } catch ( AtmosException e ) {
            if ( e.getHttpCode() != 404 ) throw e;
        }
    }

    @Test
    public void testIssue9() throws Exception {
        int threadCount = 10;

        final int objectSize = 10 * 1000 * 1000; // not a power of 2
        final AtmosApi atmosApi = api;
        final List<ObjectIdentifier> cleanupList = new ArrayList<ObjectIdentifier>();
        ThreadPoolExecutor executor = new ThreadPoolExecutor( threadCount, threadCount, 0, TimeUnit.SECONDS,
                                                              new LinkedBlockingQueue<Runnable>() );
        try {
            for ( int i = 0; i < threadCount; i++ ) {
                executor.execute( new Thread() {
                    public void run() {
                        CreateObjectRequest request = new CreateObjectRequest();
                        request.content( new RandomInputStream( objectSize ) ).contentLength( objectSize )
                               .userMetadata( new Metadata( "test-data", null, true ) );
                        ObjectId oid = atmosApi.createObject( request ).getObjectId();
                        cleanupList.add( oid );
                    }
                } );
            }
            while ( true ) {
                Thread.sleep( 1000 );
                if ( executor.getActiveCount() < 1 ) break;
            }
        } finally {
            executor.shutdown();
            cleanup.addAll( cleanupList );
            if ( cleanupList.size() < threadCount ) Assert.fail( "At least one thread failed" );
        }
    }

    /**
     * Test handling signature failures.  Should throw an exception with
     * error code 1032.
     */
    @Test
    public void testSignatureFailure() throws Exception {
        byte[] goodSecret = config.getSecretKey();
        try {

            // Fiddle with the secret key
            byte[] badSecret = Arrays.copyOf( goodSecret, goodSecret.length );
            Arrays.fill( badSecret, 5, 10, (byte) 128 ); // indexes 5-9 will be 10000000 (binary)
            config.setSecretKey( badSecret );
            testCreateEmptyObject();
            Assert.fail( "Expected exception to be thrown" );
        } catch ( AtmosException e ) {
            Assert.assertEquals( "Expected error code 1032 for signature failure", 1032, e.getErrorCode() );
        } finally {
            config.setSecretKey( goodSecret );
        }
    }

    /**
     * Test general HTTP errors by generating a 404.
     */
    @Test
    public void testFourOhFour() throws Exception {
        try {
            // Fiddle with the context
            config.setContext( "/restttttttttt" );
            testCreateEmptyObject();
            Assert.fail( "Expected exception to be thrown" );
        } catch ( AtmosException e ) {
            Assert.assertEquals( "Expected error code 404 for bad context root", 404, e.getHttpCode() );
        } finally {
            config.setContext( AtmosConfig.DEFAULT_CONTEXT );
        }
    }

    @Test
    public void testServerOffset() throws Exception {
        long offset = api.calculateServerClockSkew();
        l4j.info( "Server offset: " + offset + " milliseconds" );
        testCreateEmptyObject(); // make sure requests still work after setting clock skew
    }

    /**
     * NOTE: This method does not actually test that the custom headers are sent over the wire. Run tcpmon or wireshark
     * to verify
     */
    @Test
    public void testCustomHeaders() throws Exception {
        final Map<String, String> customHeaders = new HashMap<String, String>();
        customHeaders.put( "myCustomHeader", "Hello World!" );

        ((AtmosApiClient) api).addClientFilter( new ClientFilter() {
            @Override
            public ClientResponse handle( ClientRequest clientRequest ) throws ClientHandlerException {
                for ( String name : customHeaders.keySet() )
                    clientRequest.getHeaders().add( name, customHeaders.get( name ) );
                return getNext().handle( clientRequest );
            }
        } );

        api.getServiceInformation();
    }

    @Test
    public void testServerGeneratedChecksum() throws Exception {
        byte[] data = "hello".getBytes( "UTF-8" );

        // generate our own checksum
        RunningChecksum md5 = new RunningChecksum( ChecksumAlgorithm.MD5 );
        md5.update( data, 0, data.length );

        CreateObjectRequest request = new CreateObjectRequest().content( data ).contentType( "text/plain" );
        request.setServerGeneratedChecksumAlgorithm( ChecksumAlgorithm.MD5 );
        CreateObjectResponse response = this.api.createObject( request );
        Assert.assertNotNull( "null ID returned", response.getObjectId() );
        cleanup.add( response.getObjectId() );

        // verify checksum
        Assert.assertEquals( md5.toString( false ), response.getServerGeneratedChecksum().toString( false ) );

        // Read back the content
        ReadObjectRequest readRequest = new ReadObjectRequest().identifier( response.getObjectId() );
        ReadObjectResponse<byte[]> readResponse = api.readObject( readRequest, byte[].class );
        String content = new String( readResponse.getObject(), "UTF-8" );
        Assert.assertEquals( "object content wrong", "hello", content );

        // verify checksum
        Assert.assertEquals( md5.toString( false ), readResponse.getServerGeneratedChecksum().toString( false ) );
    }

    @Ignore("Blocked by Bug 30073")
    @Test
    public void testReadAccessToken() throws Exception {
        ObjectPath path = new ObjectPath( TESTDIR + "read_token \n,<x> test" );
        ObjectId id = api.createObject( path, "hello", "text/plain" );

        Calendar expiration = Calendar.getInstance();
        expiration.add( Calendar.MINUTE, 5 ); // 5 minutes from now

        AccessTokenPolicy.Source source = new AccessTokenPolicy.Source();
        source.setAllowList( Arrays.asList( "10.0.0.0/8" ) );
        source.setDenyList( Arrays.asList( "1.1.1.1" ) );

        AccessTokenPolicy.ContentLengthRange range = new AccessTokenPolicy.ContentLengthRange();
        range.setFrom( 0 );
        range.setTo( 1024 ); // 1KB

        AccessTokenPolicy policy = new AccessTokenPolicy();
        policy.setExpiration( expiration.getTime() );
        policy.setSource( source );
        policy.setMaxDownloads( 2 );
        policy.setMaxUploads( 0 );
        policy.setContentLengthRange( range );

        CreateAccessTokenRequest request = new CreateAccessTokenRequest().identifier( id ).policy( policy );
        CreateAccessTokenResponse response = api.createAccessToken( request );

        String content = StreamUtil.readAsString( response.getTokenUrl().openStream() );
        Assert.assertEquals( "content from *id* access token doesn't match", content, "hello" );

        api.deleteAccessToken( response.getTokenUrl() );

        response = api.createAccessToken( new CreateAccessTokenRequest().identifier( path ).policy( policy ) );

        content = StreamUtil.readAsString( response.getTokenUrl().openStream() );
        Assert.assertEquals( "content from *path* access token doesn't match", content, "hello" );

        GetAccessTokenResponse getResponse = api.getAccessToken( response.getTokenUrl() );
        AccessToken token = getResponse.getToken();

        api.deleteAccessToken( token.getId() );

        Assert.assertEquals( "token ID doesn't match",
                             RestUtil.lastPathElement( response.getTokenUrl().getPath() ),
                             token.getId() );
        policy.setMaxDownloads( policy.getMaxDownloads() - 1 ); // we already used one
        Assert.assertEquals( "policy differs", policy, token );
    }

    @Ignore("Blocked by Bug 30073")
    @Test
    public void testWriteAccessToken() throws Exception {
        api.createDirectory( new ObjectPath( TESTDIR ) );
        ObjectPath path = new ObjectPath( TESTDIR + "write_token_test" );

        Calendar expiration = Calendar.getInstance();
        expiration.add( Calendar.MINUTE, 10 ); // 10 minutes from now

        AccessTokenPolicy.Source source = new AccessTokenPolicy.Source();
        source.setAllowList( Arrays.asList( "10.0.0.0/8" ) );
        source.setDenyList( Arrays.asList( "1.1.1.1" ) );

        AccessTokenPolicy.ContentLengthRange range = new AccessTokenPolicy.ContentLengthRange();
        range.setFrom( 0 );
        range.setTo( 1024 ); // 1KB

        List<AccessTokenPolicy.FormField> formFields = new ArrayList<AccessTokenPolicy.FormField>();
        AccessTokenPolicy.FormField formField = new AccessTokenPolicy.FormField();
        formField.setName( "x-emc-meta" );
        formField.setOptional( true );
        formFields.add( formField );
        formField = new AccessTokenPolicy.FormField();
        formField.setName( "x-emc-listable-meta" );
        formField.setOptional( true );
        formFields.add( formField );

        AccessTokenPolicy policy = new AccessTokenPolicy();
        policy.setExpiration( expiration.getTime() );
        policy.setSource( source );
        policy.setMaxDownloads( 2 );
        policy.setMaxUploads( 1 );
        policy.setContentLengthRange( range );
        policy.setFormFieldList( formFields );

        CreateAccessTokenRequest request = new CreateAccessTokenRequest().identifier( path ).policy( policy );
        URL tokenUrl = api.createAccessToken( request ).getTokenUrl();

        Client client = Client.create();

        // prepare upload form
        String content = "Anonymous Upload Test";

        // note we have to specify content-disposition parameters in a specific order due to bug 27005
        FormDataContentDisposition contentDisposition = new ReorderedFormDataContentDisposition(
                "form-data; name=\"data\"; filename=\"test.txt\"" );
        BodyPart bodyPart = new BodyPart( content, MediaType.TEXT_PLAIN_TYPE ).contentDisposition( contentDisposition );

        FormDataMultiPart form = new FormDataMultiPart();
        form.field( "x-emc-meta", "color=gray,size=3,foo=bar" )
            .field( "x-emc-listable-meta", "listable=" )
            .bodyPart( bodyPart );

        // upload
        ClientResponse clientResponse = client.resource( tokenUrl.toURI() )
                                              .type( MediaType.MULTIPART_FORM_DATA_TYPE )
                                              .post( ClientResponse.class, form );
        Assert.assertEquals( "http status from upload is wrong", 201, clientResponse.getStatus() );
        ObjectId oid = new ObjectId( RestUtil.lastPathElement( clientResponse.getLocation().getPath() ) );
        cleanup.add( oid );

        clientResponse = client.resource( tokenUrl.toURI() ).get( ClientResponse.class );
        Assert.assertEquals( content, clientResponse.getEntity( String.class ) );

        // verify upload/download counts changed
        AccessToken token = api.getAccessToken( tokenUrl ).getToken();
        Assert.assertEquals( "upload count is wrong", 0, token.getMaxUploads() );
        Assert.assertEquals( "download count is wrong", 1, token.getMaxDownloads() );

        // read object via standard api (namespace) - just make sure it's there
        api.readObject( new ReadObjectRequest().identifier( path ), String.class );

        // " " (objectspace)
        ReadObjectResponse<String> response = api.readObject( new ReadObjectRequest().identifier( oid ), String.class );
        Assert.assertEquals( "content is wrong", content, response.getObject() );
        Assert.assertNotNull( "metadata is null", response.getMetadata() );
        Assert.assertEquals( "content-type is wrong", "text/plain", response.getMetadata().getContentType() );

        Map<String, Metadata> meta = response.getMetadata().getMetadata();
        Assert.assertTrue( "color missing from metadata", meta.containsKey( "color" ) );
        Assert.assertTrue( "size missing from metadata", meta.containsKey( "size" ) );
        Assert.assertTrue( "foo missing from metadata", meta.containsKey( "foo" ) );

        api.deleteAccessToken( tokenUrl );
    }

    @Ignore("Blocked by Bug 30073")
    @Test
    public void testListAccessTokens() throws Exception {
        ObjectPath path = new ObjectPath( TESTDIR + "read_token_test" );
        ObjectId id = api.createObject( path, "hello", "text/plain" );

        Calendar expiration = Calendar.getInstance();
        expiration.add( Calendar.MINUTE, 5 ); // 5 minutes from now

        AccessTokenPolicy.Source source = new AccessTokenPolicy.Source();
        source.setAllowList( Arrays.asList( "10.0.0.0/8" ) );
        source.setDenyList( Arrays.asList( "1.1.1.1" ) );

        AccessTokenPolicy.ContentLengthRange range = new AccessTokenPolicy.ContentLengthRange();
        range.setFrom( 0 );
        range.setTo( 1024 ); // 1KB

        AccessTokenPolicy policy = new AccessTokenPolicy();
        policy.setExpiration( expiration.getTime() );
        policy.setSource( source );
        policy.setMaxDownloads( 2 );
        policy.setMaxUploads( 0 );
        policy.setContentLengthRange( range );

        CreateAccessTokenRequest request = new CreateAccessTokenRequest().identifier( id ).policy( policy );
        URL tokenUrl1 = api.createAccessToken( request ).getTokenUrl();

        request = new CreateAccessTokenRequest().identifier( path ).policy( policy );
        URL tokenUrl2 = api.createAccessToken( request ).getTokenUrl();

        ListAccessTokensResponse response = api.listAccessTokens( new ListAccessTokensRequest() );
        Assert.assertNotNull( "access token list is null", response.getTokens() );
        Assert.assertEquals( "access token count wrong", 2, response.getTokens().size() );

        AccessToken token = response.getTokens().get( 0 );
        Assert.assertEquals( "token ID doesn't match", RestUtil.lastPathElement( tokenUrl1.getPath() ), token.getId() );
        Assert.assertEquals( "policy differs", policy, token );

        token = response.getTokens().get( 1 );
        Assert.assertEquals( "token ID doesn't match", RestUtil.lastPathElement( tokenUrl2.getPath() ), token.getId() );
        Assert.assertEquals( "policy differs", policy, token );
    }

    @Test
    public void testDisableSslValidation() throws Exception {
        config.setDisableSslValidation( true );
        api = new AtmosApiClient( config );
        List<URI> sslUris = new ArrayList<URI>();
        for ( URI uri : config.getEndpoints() ) {
            sslUris.add( new URI( "https", uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(),
                                  uri.getQuery(), uri.getFragment() ) );
        }
        config.setEndpoints( sslUris.toArray( new URI[sslUris.size()] ) );

        cleanup.add( api.createObject( "Hello SSL!", null ) );
    }

    @Test
    public void testRetryFilter() throws Exception {
        final int retries = 3, delay = 500;
        final String flagMessage = "XXXXX";

        config.setEnableRetry( true );
        config.setMaxRetries( retries );
        config.setRetryDelayMillis( delay );
        api = new AtmosApiClient( config );

        CreateObjectRequest request = new CreateObjectRequest().contentLength( 1 ).contentType( "text/plain" );
        try {
            api.createObject( request.content( new RetryInputStream( config, flagMessage ) ) );
            Assert.fail( "Retried more than maxRetries times" );
        } catch ( ClientHandlerException e ) {
            Assert.assertEquals( "Wrong exception thrown", flagMessage, e.getCause().getMessage() );
        }

        config.setMaxRetries( retries + 1 );

        ObjectId oid = api.createObject( request.content( new RetryInputStream( config, flagMessage ) ) ).getObjectId();
        cleanup.add( oid );
        byte[] content = api.readObject( oid, null, byte[].class );
        Assert.assertEquals( "Content wrong size", 1, content.length );
        Assert.assertEquals( "Wrong content", (byte) 65, content[0] );

        try {
            api.createObject( request.content( new RetryInputStream( null, null ) {
                @Override
                public int read() throws IOException {
                    switch ( callCount++ ) {
                        case 0:
                            throw new AtmosException( "should not retry", 400 );
                        case 1:
                            return 65;
                    }
                    return -1;
                }
            } ) );
            Assert.fail( "HTTP 400 was retried and should not be" );
        } catch ( ClientHandlerException e ) {
            Assert.assertEquals( "Wrong http code", 400, ((AtmosException) e.getCause()).getHttpCode() );
        }

        try {
            api.createObject( request.content( new RetryInputStream( null, null ) {
                @Override
                public int read() throws IOException {
                    switch ( callCount++ ) {
                        case 0:
                            throw new RuntimeException( flagMessage );
                        case 1:
                            return 65;
                    }
                    return -1;
                }
            } ) );
            Assert.fail( "RuntimeException was retried and should not be" );
        } catch ( ClientHandlerException e ) {
            Assert.assertEquals( "Wrong exception message", flagMessage, e.getCause().getMessage() );
        }
    }

    @Test
    public void testExpect100Continue() throws Exception {
        config.setEnableExpect100Continue( true );

        InputStream is = new RandomInputStream( 5 );
        CreateObjectRequest request = new CreateObjectRequest().content( is ).contentLength( 5 );

        // test success first since some load-balancers screw up the next request after an E: 100-C failure
        cleanup.add( api.createObject( request ).getObjectId() );

        // now test failure
        String tokenId = config.getTokenId();
        config.setTokenId( "bogustokenid" );
        is = new RandomInputStream( 5 );
        try {
            api.createObject( request );
        } catch ( AtmosException e ) {
            Assert.assertEquals( "wrong error code", 1033, e.getErrorCode() );
            Assert.assertEquals( "input stream was read", 5, is.available() );
        } finally {
            config.setTokenId( tokenId );
        }
    }

    @Test
    public void testMultiThreadedBufferedWriter() throws Exception {
        int threadCount = 20;
        ThreadPoolExecutor executor = new ThreadPoolExecutor( threadCount, threadCount, 5000, TimeUnit.MILLISECONDS,
                                                              new LinkedBlockingQueue<Runnable>() );

        // test with String
        List<Throwable> errorList = Collections.synchronizedList( new ArrayList<Throwable>() );
        for ( int i = 0; i < threadCount; i++ ) {
            executor.execute( new ObjectTestThread<String>( "Test thread " + i,
                                                            "text/plain",
                                                            String.class,
                                                            errorList ) );
        }
        do {
            Thread.sleep( 500 );
        } while ( executor.getActiveCount() > 0 );
        if ( !errorList.isEmpty() ) {
            for ( Throwable t : errorList ) t.printStackTrace();
            Assert.fail( "At least one thread failed" );
        }

        // test with JAXB bean
        try {
            for ( int i = 0; i < threadCount; i++ ) {
                executor.execute( new ObjectTestThread<AccessTokenPolicy>( createTestTokenPolicy( "Test thread " + i,
                                                                                                  "x.x.x." + i ),
                                                                           "text/xml",
                                                                           AccessTokenPolicy.class,
                                                                           errorList ) );
            }
            do {
                Thread.sleep( 500 );
            } while ( executor.getActiveCount() > 0 );
        } finally {
            executor.shutdown();
        }
        if ( !errorList.isEmpty() ) {
            for ( Throwable t : errorList ) t.printStackTrace();
            Assert.fail( "At least one thread failed" );
        }
    }

    protected String rand8char() {
        Random r = new Random();
        StringBuilder sb = new StringBuilder( 8 );
        for ( int i = 0; i < 8; i++ ) {
            sb.append( (char) ('a' + r.nextInt( 26 )) );
        }
        return sb.toString();
    }

    private AccessTokenPolicy createTestTokenPolicy( String allow, String deny ) {
        AccessTokenPolicy.Source source = new AccessTokenPolicy.Source();
        source.setAllowList( Arrays.asList( allow ) );
        source.setDenyList( Arrays.asList( deny ) );
        AccessTokenPolicy policy = new AccessTokenPolicy();
        policy.setExpiration( new Date( 1355897000000L ) );
        policy.setMaxDownloads( 5 );
        policy.setMaxUploads( 10 );
        policy.setSource( source );
        return policy;
    }

    private class RetryInputStream extends InputStream {
        protected int callCount = 0;
        private long now;
        private long lastTime;
        private AtmosConfig config;
        private String flagMessage;

        public RetryInputStream( AtmosConfig config, String flagMessage ) {
            this.config = config;
            this.flagMessage = flagMessage;
        }

        @Override
        public int read() throws IOException {
            switch ( callCount++ ) {
                case 0:
                    lastTime = System.currentTimeMillis();
                    throw new AtmosException( "foo", 500 );
                case 1:
                    now = System.currentTimeMillis();
                    Assert.assertTrue( "Retry delay for 500 error was not honored",
                                       now - lastTime >= config.getRetryDelayMillis() );
                    lastTime = now;
                    throw new AtmosException( "bar", 500, 1040 );
                case 2:
                    now = System.currentTimeMillis();
                    Assert.assertTrue( "Retry delay for 1040 error was not honored",
                                       now - lastTime >= config.getRetryDelayMillis() + 300 );
                    lastTime = now;
                    throw new IOException( "baz" );
                case 3:
                    now = System.currentTimeMillis();
                    Assert.assertTrue( "Retry delay for IOException was not honored",
                                       now - lastTime >= config.getRetryDelayMillis() );
                    lastTime = now;
                    throw new AtmosException( flagMessage, 500 );
                case 4:
                    return 65;
            }
            return -1;
        }

        @Override
        public synchronized void reset() throws IOException {
        }

        @Override
        public boolean markSupported() {
            return true;
        }
    }

    private class ObjectTestThread<T> implements Runnable {
        private T content;
        private String contentType;
        private Class<T> objectType;
        private List<Throwable> errorList;

        public ObjectTestThread( T content,
                                 String contentType,
                                 Class<T> objectType,
                                 List<Throwable> errorList ) {
            this.content = content;
            this.contentType = contentType;
            this.objectType = objectType;
            this.errorList = errorList;
        }

        @Override
        public void run() {
            try {
                ObjectId oid = api.createObject( content, contentType );
                cleanup.add( oid );
                T readContent = api.readObject( oid, null, objectType );
                Assert.assertEquals( "Content for object " + oid + " not equal", content, readContent );
            } catch ( Throwable t ) {
                errorList.add( t );
            }
        }
    }
}