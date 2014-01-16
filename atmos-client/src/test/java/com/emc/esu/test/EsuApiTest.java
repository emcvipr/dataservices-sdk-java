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
package com.emc.esu.test;

import com.emc.atmos.util.RandomInputStream;
import com.emc.esu.api.*;
import com.emc.esu.api.Checksum.Algorithm;
import com.emc.esu.api.rest.DownloadHelper;
import com.emc.esu.api.rest.UploadHelper;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.io.*;
import java.net.URL;
import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Implements testcases that are independent of the protocol (REST vs. SOAP).
 * Note that this class does not implement TestCase; it is called by the
 * REST and SOAP testcases.
 */
@SuppressWarnings("deprecation")
public abstract class EsuApiTest {
    public static Logger l4j = Logger.getLogger( EsuApiTest.class );

    /**
     * Use this as a prefix for namespace object paths and you won't have to clean up after yourself.
     * This also keeps all test objects under one folder, which is easy to delete should something go awry.
     */
    protected static final String TEST_DIR_PREFIX = "/test_" + EsuApiTest.class.getSimpleName();

    protected EsuApi esu;
    protected String uid;

    protected List<Identifier> cleanup = Collections.synchronizedList( new ArrayList<Identifier>() );
    protected List<ObjectPath> cleanupDirs = Collections.synchronizedList( new ArrayList<ObjectPath>() );

    /**
     * Tear down after a test is run.  Cleans up objects that were created
     * during the test.  Set cleanUp=false to disable this behavior.
     */
    @After
    public void tearDown() {
        for (Identifier cleanItem : cleanup) {
            try {
                this.esu.deleteObject(cleanItem);
            } catch (Exception e) {
                System.out.println("Failed to delete " + cleanItem + ": " + e.getMessage());
            }
        }
        try { // if test directories exists, recursively delete them
            for ( ObjectPath testDir : cleanupDirs ) {
                deleteRecursively( testDir );
            }
        } catch ( EsuException e ) {
            if ( e.getHttpCode() != 404 ) {
                l4j.warn( "Could not delete test dir: ", e );
            }
        }
    }

    protected void deleteRecursively( ObjectPath path ) {
        if ( path.isDirectory() ) {
            for ( DirectoryEntry entry : this.esu.listDirectory( path, null ) ) {
                deleteRecursively( entry.getPath() );
            }
        }
        this.esu.deleteObject( path );
    }

    protected ObjectPath createTestDir( String name ) {
        if (!name.endsWith("/")) name = name + "/";
        ObjectPath path = new ObjectPath( TEST_DIR_PREFIX + "_" + name );
        this.esu.createObjectOnPath( path, null, null, null, null );
        cleanupDirs.add( path );
        return path;
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
        Assert.assertEquals( "2-byte characters failed", URLEncoder.encode( twoByteCharacters, "UTF-8" ), twoByteEscaped );
        Assert.assertEquals( "4-byte characters failed", URLEncoder.encode( fourByteCharacters, "UTF-8" ), fourByteEscaped );
        Assert.assertEquals( "2-byte/4-byte mix failed", URLEncoder.encode( twoByteCharacters + fourByteCharacters, "UTF-8" ), twoByteEscaped + fourByteEscaped );
        Assert.assertEquals( "1-byte/2-byte mix failed", URLEncoder.encode( oneByteCharacters + twoByteCharacters, "UTF-8" ), oneByteCharacters + twoByteEscaped );
        Assert.assertEquals( "1-4 byte mix failed", URLEncoder.encode( oneByteCharacters + twoByteCharacters + fourByteCharacters, "UTF-8" ), oneByteCharacters + twoByteEscaped + fourByteEscaped );
    }

    /**
     * Test creating one empty object.  No metadata, no content.
     */
    @Test
    public void testCreateEmptyObject() throws Exception {
        ObjectId id = this.esu.createObject( null, null, null, null );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read back the content
        String content = new String( this.esu.readObject( id, null, null ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "", content );

    }

    /**
     * Test creating one empty object on a path.  No metadata, no content.
     */
    @Test
    public void testCreateEmptyObjectOnPath() throws Exception {
        ObjectPath op = new ObjectPath( "/" + rand8char() );
        ObjectId id = this.esu.createObjectOnPath( op, null, null, null, null );
        cleanup.add( op );
        l4j.debug( "Path: " + op + " ID: " + id );
        Assert.assertNotNull( id );

        // Read back the content
        String content = new String( this.esu.readObject( op, null, null ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "", content );
        content = new String( this.esu.readObject( id, null, null ), "UTF-8" );
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
        ObjectId id = this.esu.createObjectOnPath( path, null, null, null, null );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        ObjectPath parent = new ObjectPath( "/" + dirName + "/" );
        List<DirectoryEntry> ents = this.esu.listDirectory( parent, null );
        boolean found = false;
        for ( DirectoryEntry ent : ents ) {
            if ( ent.getPath().equals( path ) ) {
                found = true;
            }
        }
        Assert.assertTrue( "Did not find unicode file in dir", found );

        // Check read
        this.esu.readObject( path, null, null );

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
        ObjectId id = this.esu.createObjectFromStreamOnPath( path, null, null, in, data.length, null );
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
        ObjectPath parentDir = createTestDir("Utf8Path");
        ObjectPath path = new ObjectPath( parentDir + crazyName );

        // create crazy-name object
        this.esu.createObjectOnPath( path, null, null, content, "text/plain" );

        cleanup.add(path);

        // verify name in directory list
        boolean found = false;
        for ( DirectoryEntry entry : this.esu.listDirectory( parentDir, null ) ) {
            if ( entry.getPath().toString().equals( path.toString() ) ) {
                found = true;
                break;
            }
        }
        Assert.assertTrue( "crazyName not found in directory listing", found );

        // verify content
        Assert.assertTrue( "content does not match", Arrays.equals( content, this.esu.readObject( path, null, null ) ) );
    }

    @Test
    public void testUtf8Content() throws Exception {
        String oneByteCharacters = "Hello! ,";
        String twoByteCharacters = "\u0410\u0411\u0412\u0413"; // Cyrillic letters
        String fourByteCharacters = "\ud841\udf0e\ud841\udf31\ud841\udf79\ud843\udc53"; // Chinese symbols
        byte[] content = (oneByteCharacters + twoByteCharacters + fourByteCharacters).getBytes( "UTF-8" );
        ObjectPath parentDir = createTestDir("Utf8Content");
        ObjectPath path = new ObjectPath( parentDir.getName() + "/" + "utf8Content.txt" );

        // create object with multi-byte UTF-8 content
        this.esu.createObjectOnPath( path, null, null, content, "text/plain" );

        // verify content
        Assert.assertTrue( "content does not match", Arrays.equals( content, this.esu.readObject( path, null, null ) ) );
    }

    protected String rand8char() {
        Random r = new Random();
        StringBuffer sb = new StringBuffer( 8 );
        for ( int i = 0; i < 8; i++ ) {
            sb.append( (char) ('a' + r.nextInt( 26 )) );
        }
        return sb.toString();
    }

    /**
     * Test creating an object with content but without metadata
     */
    @Test
    public void testCreateObjectWithContent() throws Exception {
        ObjectId id = this.esu.createObject( null, null, "hello".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read back the content
        String content = new String( this.esu.readObject( id, null, null ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "hello", content );
    }

    @Test
    public void testCreateObjectWithContentStream() throws Exception {
        InputStream in = new ByteArrayInputStream( "hello".getBytes( "UTF-8" ) );
        ObjectId id = this.esu.createObjectFromStream( null, null, in, 5, "text/plain" );
        in.close();
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read back the content
        String content = new String( this.esu.readObject( id, null, null ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "hello", content );
    }

    @Test
    public void testCreateObjectWithContentStreamOnPath() throws Exception {
        ObjectPath op = new ObjectPath( "/" + rand8char() + ".tmp" );
        InputStream in = new ByteArrayInputStream( "hello".getBytes( "UTF-8" ) );
        ObjectId id = this.esu.createObjectFromStreamOnPath( op, null, null, in, 5, "text/plain" );
        in.close();
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read back the content
        String content = new String( this.esu.readObject( id, null, null ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "hello", content );
    }

    /**
     * Test creating an object with metadata but no content.
     */
    @Test
    public void testCreateObjectWithMetadataOnPath() {
        ObjectPath op = new ObjectPath( "/" + rand8char() + ".tmp" );
        MetadataList mlist = new MetadataList();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "listable2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "unlistable2", "bar2 bar2", false );
        mlist.addMetadata( listable );
        mlist.addMetadata( unlistable );
        mlist.addMetadata( listable2 );
        mlist.addMetadata( unlistable2 );
        ObjectId id = this.esu.createObjectOnPath( op, null, mlist, null, null );
        //this.esu.updateObject( op, null, mlist, null, null, null );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( op );

        // Read and validate the metadata
        MetadataList meta = this.esu.getUserMetadata( op, null );
        Assert.assertNotNull( "value of 'listable' missing", meta.getMetadata( "listable" ) );
        Assert.assertNotNull( "value of 'listable2' missing", meta.getMetadata( "listable2" ) );
        Assert.assertNotNull( "value of 'unlistable' missing", meta.getMetadata( "unlistable" ) );
        Assert.assertNotNull( "value of 'unlistable2' missing", meta.getMetadata( "unlistable2" ) );

        Assert.assertEquals( "value of 'listable' wrong", "foo", meta.getMetadata( "listable" ).getValue() );
        Assert.assertEquals( "value of 'listable2' wrong", "foo2 foo2", meta.getMetadata( "listable2" ).getValue() );
        Assert.assertEquals( "value of 'unlistable' wrong", "bar", meta.getMetadata( "unlistable" ).getValue() );
        Assert.assertEquals( "value of 'unlistable2' wrong", "bar2 bar2", meta.getMetadata( "unlistable2" ).getValue() );
        // Check listable flags
        Assert.assertEquals( "'listable' is not listable", true, meta.getMetadata( "listable" ).isListable() );
        Assert.assertEquals( "'listable2' is not listable", true, meta.getMetadata( "listable2" ).isListable() );
        Assert.assertEquals( "'unlistable' is listable", false, meta.getMetadata( "unlistable" ).isListable() );
        Assert.assertEquals( "'unlistable2' is listable", false, meta.getMetadata( "unlistable2" ).isListable() );
    }

    /**
     * Test creating an object with metadata but no content.
     */
    @Test
    public void testCreateObjectWithMetadata() {
        MetadataList mlist = new MetadataList();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "listable2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "unlistable2", "bar2 bar2", false );
        Metadata listable3 = new Metadata( "listable3", null, true );
        Metadata quotes = new Metadata("ST_modalities", "\\US\\", false);
        //Metadata withCommas = new Metadata( "withcommas", "I, Robot", false );
        //Metadata withEquals = new Metadata( "withequals", "name=value", false );
        mlist.addMetadata( listable );
        mlist.addMetadata( unlistable );
        mlist.addMetadata( listable2 );
        mlist.addMetadata( unlistable2 );
        mlist.addMetadata( listable3 );
        mlist.addMetadata(quotes);
        //mlist.addMetadata( withCommas );
        //mlist.addMetadata( withEquals );
        ObjectId id = this.esu.createObject( null, mlist, null, null );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read and validate the metadata
        MetadataList meta = this.esu.getUserMetadata( id, null );
        Assert.assertEquals( "value of 'listable' wrong", "foo", meta.getMetadata( "listable" ).getValue() );
        Assert.assertEquals( "value of 'listable2' wrong", "foo2 foo2", meta.getMetadata( "listable2" ).getValue() );
        Assert.assertEquals( "value of 'unlistable' wrong", "bar", meta.getMetadata( "unlistable" ).getValue() );
        Assert.assertEquals( "value of 'unlistable2' wrong", "bar2 bar2", meta.getMetadata( "unlistable2" ).getValue() );
        Assert.assertNotNull( "listable3 missing", meta.getMetadata( "listable3" ) );
        Assert.assertTrue( "Value of listable3 should be empty", meta.getMetadata( "listable3" ).getValue() == null || meta.getMetadata( "listable3" ).getValue().length() == 0 );
        //Assert.assertEquals( "Value of withcommas wrong", "I, Robot", meta.getMetadata( "withcommas" ).getValue() );
        //Assert.assertEquals( "Value of withequals wrong", "name=value", meta.getMetadata( "withequals" ).getValue() );

        // Check listable flags
        Assert.assertEquals( "'listable' is not listable", true, meta.getMetadata( "listable" ).isListable() );
        Assert.assertEquals( "'listable2' is not listable", true, meta.getMetadata( "listable2" ).isListable() );
        Assert.assertEquals( "'listable3' is not listable", true, meta.getMetadata( "listable3" ).isListable() );
        Assert.assertEquals( "'unlistable' is listable", false, meta.getMetadata( "unlistable" ).isListable() );
        Assert.assertEquals( "'unlistable2' is listable", false, meta.getMetadata( "unlistable2" ).isListable() );

    }

    /**
     * Test creating an object with metadata but no content.
     */
    @Test
    public void testMetadataNormalizeSpace() {
        MetadataList mlist = new MetadataList();
        Metadata unlistable = new Metadata( "unlistable", "bar  bar   bar    bar", false );
        Metadata leadingSpacesOdd = new Metadata( "leadingodd", "   spaces", false);
        Metadata trailingSpacesOdd = new Metadata( "trailingodd", "spaces   ", false);
        Metadata leadingSpacesEven = new Metadata( "leadingeven", "    SPACES", false);
        Metadata trailingSpacesEven = new Metadata( "trailingeven", "spaces    ", false);
        mlist.addMetadata( unlistable );
        mlist.addMetadata( leadingSpacesOdd );
        mlist.addMetadata( trailingSpacesOdd );
        mlist.addMetadata( leadingSpacesEven);
        mlist.addMetadata( trailingSpacesEven);
        ObjectId id = this.esu.createObject( null, mlist, null, null );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read and validate the metadata
        MetadataList meta = this.esu.getUserMetadata( id, null );
        Assert.assertEquals( "value of 'unlistable' wrong", "bar  bar   bar    bar", meta.getMetadata( "unlistable" ).getValue() );
        // Check listable flags
        Assert.assertEquals( "'unlistable' is listable", false, meta.getMetadata( "unlistable" ).isListable() );

    }

    /**
     * Test reading an object's content
     */
    @Test
    public void testReadObject() throws Exception {
        ObjectId id = this.esu.createObject( null, null, "hello".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read back the content
        String content = new String( this.esu.readObject( id, null, null ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "hello", content );

        // Read back only 2 bytes
        Extent extent = new Extent( 1, 2 );
        content = new String( this.esu.readObject( id, extent, null ), "UTF-8" );
        Assert.assertEquals( "partial object content wrong", "el", content );
    }

    /**
     * Test reading an ACL back
     */
    @Test
    public void testReadAcl() {
        // Create an object with an ACL
        Acl acl = new Acl();
        acl.addGrant( new Grant( new Grantee( stripUid( uid ), Grantee.GRANT_TYPE.USER ), Permission.FULL_CONTROL ) );
        acl.addGrant( new Grant( Grantee.OTHER, Permission.READ ) );
        ObjectId id = this.esu.createObject( acl, null, null, null );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read back the ACL and make sure it matches
        Acl newacl = this.esu.getAcl( id );
        l4j.info( "Comparing " + newacl + " with " + acl );

        Assert.assertEquals( "ACLs don't match", acl, newacl );

    }

    /**
     * Inside an ACL, you use the UID only, not SubtenantID/UID
     *
     * @param uid
     * @return
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
        acl.addGrant( new Grant( new Grantee( stripUid( uid ), Grantee.GRANT_TYPE.USER ), Permission.FULL_CONTROL ) );
        acl.addGrant( new Grant( Grantee.OTHER, Permission.READ ) );
        ObjectId id = this.esu.createObjectOnPath( op, acl, null, null, null );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( op );

        // Read back the ACL and make sure it matches
        Acl newacl = this.esu.getAcl( op );
        l4j.info( "Comparing " + newacl + " with " + acl );

        Assert.assertEquals( "ACLs don't match", acl, newacl );

    }

    /**
     * Test reading back user metadata
     */
    @Test
    public void testGetUserMetadata() {
        // Create an object with user metadata
        MetadataList mlist = new MetadataList();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "listable2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "unlistable2", "bar2 bar2", false );
        mlist.addMetadata( listable );
        mlist.addMetadata( unlistable );
        mlist.addMetadata( listable2 );
        mlist.addMetadata( unlistable2 );
        ObjectId id = this.esu.createObject( null, mlist, null, null );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read only part of the metadata
        MetadataTags mtags = new MetadataTags();
        mtags.addTag( new MetadataTag( "listable", true ) );
        mtags.addTag( new MetadataTag( "unlistable", false ) );
        MetadataList meta = this.esu.getUserMetadata( id, mtags );
        Assert.assertEquals( "value of 'listable' wrong", "foo", meta.getMetadata( "listable" ).getValue() );
        Assert.assertNull( "value of 'listable2' should not have been returned", meta.getMetadata( "listable2" ) );
        Assert.assertEquals( "value of 'unlistable' wrong", "bar", meta.getMetadata( "unlistable" ).getValue() );
        Assert.assertNull( "value of 'unlistable2' should not have been returned", meta.getMetadata( "unlistable2" ) );

    }

    /**
     * Test deleting user metadata
     */
    @Test
    public void testDeleteUserMetadata() {
        // Create an object with metadata
        MetadataList mlist = new MetadataList();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "listable2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "unlistable2", "bar2 bar2", false );
        mlist.addMetadata( listable );
        mlist.addMetadata( unlistable );
        mlist.addMetadata( listable2 );
        mlist.addMetadata( unlistable2 );
        ObjectId id = this.esu.createObject( null, mlist, null, null );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Delete a couple of the metadata entries
        MetadataTags mtags = new MetadataTags();
        mtags.addTag( new MetadataTag( "listable2", true ) );
        mtags.addTag( new MetadataTag( "unlistable2", false ) );
        this.esu.deleteUserMetadata( id, mtags );

        // Read back the metadata for the object and ensure the deleted
        // entries don't exist
        MetadataList meta = this.esu.getUserMetadata( id, null );
        Assert.assertEquals( "value of 'listable' wrong", "foo", meta.getMetadata( "listable" ).getValue() );
        Assert.assertNull( "value of 'listable2' should not have been returned", meta.getMetadata( "listable2" ) );
        Assert.assertEquals( "value of 'unlistable' wrong", "bar", meta.getMetadata( "unlistable" ).getValue() );
        Assert.assertNull( "value of 'unlistable2' should not have been returned", meta.getMetadata( "unlistable2" ) );
    }

    /**
     * Test creating object versions
     */
    @Test
    public void testVersionObject() {
        // Create an object
        MetadataList mlist = new MetadataList();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "listable2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "unlistable2", "bar2 bar2", false );
        mlist.addMetadata( listable );
        mlist.addMetadata( unlistable );
        mlist.addMetadata( listable2 );
        mlist.addMetadata( unlistable2 );
        ObjectId id = this.esu.createObject( null, mlist, null, null );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Version the object
        ObjectId vid = this.esu.versionObject( id );
        cleanup.add( vid );
        Assert.assertNotNull( "null version ID returned", vid );

        Assert.assertFalse( "Version ID shoudn't be same as original ID", id.equals( vid ) );

        // Fetch the version and read its data
        MetadataList meta = this.esu.getUserMetadata( vid, null );
        Assert.assertEquals( "value of 'listable' wrong", "foo", meta.getMetadata( "listable" ).getValue() );
        Assert.assertEquals( "value of 'listable2' wrong", "foo2 foo2", meta.getMetadata( "listable2" ).getValue() );
        Assert.assertEquals( "value of 'unlistable' wrong", "bar", meta.getMetadata( "unlistable" ).getValue() );
        Assert.assertEquals( "value of 'unlistable2' wrong", "bar2 bar2", meta.getMetadata( "unlistable2" ).getValue() );

    }

    /**
     * Test listing the versions of an object
     */
    @Test
    public void testListVersions() {
        // Create an object
        MetadataList mlist = new MetadataList();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "listable2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "unlistable2", "bar2 bar2", false );
        mlist.addMetadata( listable );
        mlist.addMetadata( unlistable );
        mlist.addMetadata( listable2 );
        mlist.addMetadata( unlistable2 );
        ObjectId id = this.esu.createObject( null, mlist, null, null );
        Assert.assertNotNull( "null ID returned", id );

        // Version the object
        ObjectId vid1 = this.esu.versionObject( id );
        Assert.assertNotNull( "null version ID returned", vid1 );
        ObjectId vid2 = this.esu.versionObject( id );
        Assert.assertNotNull( "null version ID returned", vid2 );
        cleanup.add( id );

        // List the versions and ensure their IDs are correct
        List<Identifier> versions = this.esu.listVersions( id );
        Assert.assertEquals( "Wrong number of versions returned", 2, versions.size() );
        Assert.assertTrue( "version 1 not found in version list", versions.contains( vid1 ) );
        Assert.assertTrue( "version 2 not found in version list", versions.contains( vid2 ) );
    }

    /**
     * Test listing the versions of an object
     */
    @Test
    public void testListVersionsLong() {
        // Create an object
        MetadataList mlist = new MetadataList();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "listable2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "unlistable2", "bar2 bar2", false );
        mlist.addMetadata( listable );
        mlist.addMetadata( unlistable );
        mlist.addMetadata( listable2 );
        mlist.addMetadata( unlistable2 );
        ObjectId id = this.esu.createObject( null, mlist, null, null );
        Assert.assertNotNull( "null ID returned", id );

        // Version the object
        ObjectId vid1 = this.esu.versionObject( id );
        Version v1 = new Version(vid1, 0, null);
        Assert.assertNotNull( "null version ID returned", vid1 );
        ObjectId vid2 = this.esu.versionObject( id );
        Version v2 = new Version(vid2, 1, null);
        Assert.assertNotNull( "null version ID returned", vid2 );
        cleanup.add( id );

        // List the versions and ensure their IDs are correct
        ListOptions options = new ListOptions();
        options.setLimit(1);
        List<Version> versions = new ArrayList<Version>();
        do {
        	versions.addAll(this.esu.listVersions( id, options ));
        } while(options.getToken() != null);
        Assert.assertEquals( "Wrong number of versions returned", 2, versions.size() );
        Assert.assertTrue( "version 1 not found in version list", versions.contains( v1 ) );
        Assert.assertTrue( "version 2 not found in version list", versions.contains( v2 ) );
        for(Version v : versions) {
        	Assert.assertNotNull("oid null in version", v.getId());
        	Assert.assertTrue("Invalid version number in version", v.getVersionNumber()>-1);
        	Assert.assertNotNull("itime null in version", v.getItime());
        }
    }

    /**
     * Test listing the versions of an object
     */
    @Test
    public void testDeleteVersion() {
        // Create an object
        MetadataList mlist = new MetadataList();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "listable2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "unlistable2", "bar2 bar2", false );
        mlist.addMetadata( listable );
        mlist.addMetadata( unlistable );
        mlist.addMetadata( listable2 );
        mlist.addMetadata( unlistable2 );
        ObjectId id = this.esu.createObject(null, mlist, null, null);
        Assert.assertNotNull( "null ID returned", id );

        // Version the object
        ObjectId vid1 = this.esu.versionObject( id );
        Assert.assertNotNull( "null version ID returned", vid1 );
        ObjectId vid2 = this.esu.versionObject( id );
        Assert.assertNotNull( "null version ID returned", vid2 );
        cleanup.add( id );

        // List the versions and ensure their IDs are correct
        List<Identifier> versions = this.esu.listVersions( id );
        Assert.assertEquals( "Wrong number of versions returned", 2, versions.size() );
        Assert.assertTrue( "version 1 not found in version list", versions.contains( vid1 ) );
        Assert.assertTrue( "version 2 not found in version list", versions.contains( vid2 ) );

        // Delete a version
        this.esu.deleteVersion( vid1 );
        versions = this.esu.listVersions( id );
        Assert.assertEquals( "Wrong number of versions returned", 1, versions.size() );
        Assert.assertFalse( "version 1 found in version list", versions.contains( vid1 ) );
        Assert.assertTrue( "version 2 not found in version list", versions.contains( vid2 ) );

    }

    @Test
    public void testRestoreVersion() throws UnsupportedEncodingException {
        ObjectId id = this.esu.createObject( null, null, "Base Version Content".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Version the object
        ObjectId vId = this.esu.versionObject( id );

        // Update the object content
        this.esu.updateObject( id, null, null, null, "Child Version Content -- You should never see me".getBytes( "UTF-8" ), "text/plain" );

        // Restore the original version
        this.esu.restoreVersion( id, vId );

        // Read back the content
        String content = new String( this.esu.readObject( id, null, null ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "Base Version Content", content );

    }

    /**
     * Test listing the system metadata on an object
     */
    @Test
    public void testGetSystemMetadata() {
        // Create an object
        MetadataList mlist = new MetadataList();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "listable2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "unlistable2", "bar2 bar2", false );
        mlist.addMetadata( listable );
        mlist.addMetadata( unlistable );
        mlist.addMetadata( listable2 );
        mlist.addMetadata( unlistable2 );
        ObjectId id = this.esu.createObject( null, mlist, null, null );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read only part of the metadata
        MetadataTags mtags = new MetadataTags();
        mtags.addTag( new MetadataTag( "atime", false ) );
        mtags.addTag( new MetadataTag( "ctime", false ) );
        MetadataList meta = this.esu.getSystemMetadata( id, mtags );
        Assert.assertNotNull( "value of 'atime' missing", meta.getMetadata( "atime" ) );
        Assert.assertNull( "value of 'mtime' should not have been returned", meta.getMetadata( "mtime" ) );
        Assert.assertNotNull( "value of 'ctime' missing", meta.getMetadata( "ctime" ) );
        Assert.assertNull( "value of 'gid' should not have been returned", meta.getMetadata( "gid" ) );
        Assert.assertNull( "value of 'listable' should not have been returned", meta.getMetadata( "listable" ) );
    }

    /**
     * Test listing objects by a tag that doesn't exist
     */
    @Test
    public void testListObjectsNoExist() {
        ListOptions options = new ListOptions();
        List<ObjectResult> objects = this.esu.listObjects( "this_tag_should_not_exist", options );
        Assert.assertNotNull( "object list should be not null", objects );
        Assert.assertEquals( "No objects should be returned", 0, objects.size() );
    }

    /**
     * Test listing objects by a tag
     */
    @Test
    public void testListObjects() {
        // Create an object
        MetadataList mlist = new MetadataList();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "list/able/2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "list/able/not", "bar2 bar2", false );
        mlist.addMetadata( listable );
        mlist.addMetadata( unlistable );
        mlist.addMetadata( listable2 );
        mlist.addMetadata( unlistable2 );
        ObjectId id = this.esu.createObject( null, mlist, null, null );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // List the objects.  Make sure the one we created is in the list
        List<ObjectResult> objects = this.esu.listObjects( "listable", null );
        Assert.assertTrue( "No objects returned", objects.size() > 0 );
        Assert.assertTrue( "object not found in list", objects.contains( id ) );

    }

    /**
     * Test listing objects by a tag
     */
    @Test
    public void testListObjectsWithMetadata() {
        // Create an object
        MetadataList mlist = new MetadataList();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "list/able/2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "list/able/not", "bar2 bar2", false );
        mlist.addMetadata( listable );
        mlist.addMetadata( unlistable );
        mlist.addMetadata( listable2 );
        mlist.addMetadata( unlistable2 );
        ObjectId id = this.esu.createObject( null, mlist, null, null );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // List the objects.  Make sure the one we created is in the list
        ListOptions options = new ListOptions();
        options.setIncludeMetadata( true );
        List<ObjectResult> objects = this.esu.listObjects( "listable", options );
        Assert.assertTrue( "No objects returned", objects.size() > 0 );

        // Find the item.
        boolean found = false;
        for ( Iterator<ObjectResult> i = objects.iterator(); i.hasNext(); ) {
            ObjectResult or = i.next();
            if ( or.getId().equals( id ) ) {
                found = true;
                // check metadata
                Assert.assertEquals( "Wrong value on metadata",
                        or.getMetadata().getMetadata( "listable" ).getValue(), "foo" );
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
        MetadataList mlist = new MetadataList();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "list/able/2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "list/able/not", "bar2 bar2", false );
        mlist.addMetadata( listable );
        mlist.addMetadata( unlistable );
        mlist.addMetadata( listable2 );
        mlist.addMetadata( unlistable2 );
        ObjectId id = this.esu.createObject( null, mlist, null, null );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // List the objects.  Make sure the one we created is in the list
        ListOptions options = new ListOptions();
        options.setIncludeMetadata( true );
        options.setUserMetadata( Arrays.asList( new String[]{"listable"} ) );
        List<ObjectResult> objects = this.esu.listObjects( "listable", options );
        Assert.assertTrue( "No objects returned", objects.size() > 0 );

        // Find the item.
        boolean found = false;
        for ( Iterator<ObjectResult> i = objects.iterator(); i.hasNext(); ) {
            ObjectResult or = i.next();
            if ( or.getId().equals( id ) ) {
                found = true;
                // check metadata
                Assert.assertEquals( "Wrong value on metadata",
                        or.getMetadata().getMetadata( "listable" ).getValue(), "foo" );

                // Other metadata should not be present
                Assert.assertNull( "unlistable should be missing",
                        or.getMetadata().getMetadata( "unlistable" ) );
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
        MetadataList mlist = new MetadataList();
        Metadata listable = new Metadata( "listable", "foo", true );
        mlist.addMetadata( listable );
        ObjectId id1 = this.esu.createObject( null, mlist, null, null );
        ObjectId id2 = this.esu.createObject( null, mlist, null, null );
        Assert.assertNotNull( "null ID returned", id1 );
        Assert.assertNotNull( "null ID returned", id2 );
        cleanup.add( id1 );
        cleanup.add( id2 );

        // List the objects.  Make sure the one we created is in the list
        ListOptions options = new ListOptions();
        options.setIncludeMetadata( true );
        options.setLimit( 1 );
        List<ObjectResult> objects = this.esu.listObjects( "listable", options );
        Assert.assertTrue( "No objects returned", objects.size() > 0 );
        Assert.assertNotNull( "Token should be present", options.getToken() );

        l4j.debug( "listObjectsPaged, Token: " + options.getToken() );
        while ( options.getToken() != null ) {
            // Subsequent pages
            objects.addAll( this.esu.listObjects( "listable", options ) );
            l4j.debug( "listObjectsPaged, Token: " + options.getToken() );
        }

        // Ensure our IDs exist
        Assert.assertTrue( "First object not found", objects.contains( id1 ) );
        Assert.assertTrue( "Second object not found", objects.contains( id2 ) );
    }


    /**
     * Test fetching listable tags
     */
    @Test
    public void testGetListableTags() {
        // Create an object
        ObjectId id = this.esu.createObject( null, null, null, null );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        MetadataList mlist = new MetadataList();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "list/able/2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "list/able/not", "bar2 bar2", false );
        mlist.addMetadata( listable );
        mlist.addMetadata( unlistable );
        mlist.addMetadata( listable2 );
        mlist.addMetadata( unlistable2 );
        this.esu.updateObject( id, null, mlist, null, null, null );

        // List tags.  Ensure our object's tags are in the list.
        MetadataTags tags = this.esu.getListableTags( (String) null );
        Assert.assertTrue( "listable tag not returned", tags.contains( "listable" ) );
        Assert.assertTrue( "list/able/2 root tag not returned", tags.contains( "list" ) );
        Assert.assertFalse( "list/able/not tag returned", tags.contains( "list/able/not" ) );

        // List child tags
        tags = this.esu.getListableTags( "list/able" );
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
        MetadataList mlist = new MetadataList();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "list/able/2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "list/able/not", "bar2 bar2", false );
        mlist.addMetadata( listable );
        mlist.addMetadata( unlistable );
        mlist.addMetadata( listable2 );
        mlist.addMetadata( unlistable2 );
        ObjectId id = this.esu.createObject( null, mlist, null, null );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // List tags
        MetadataTags tags = this.esu.listUserMetadataTags( id );
        Assert.assertTrue( "listable tag not returned", tags.contains( "listable" ) );
        Assert.assertTrue( "list/able/2 tag not returned", tags.contains( "list/able/2" ) );
        Assert.assertTrue( "unlistable tag not returned", tags.contains( "unlistable" ) );
        Assert.assertTrue( "list/able/not tag not returned", tags.contains( "list/able/not" ) );
        Assert.assertFalse( "unknown tag returned", tags.contains( "unknowntag" ) );

        // Check listable flag
        Assert.assertEquals( "'listable' is not listable", true, tags.getTag( "listable" ).isListable() );
        Assert.assertEquals( "'list/able/2' is not listable", true, tags.getTag( "list/able/2" ).isListable() );
        Assert.assertEquals( "'unlistable' is listable", false, tags.getTag( "unlistable" ).isListable() );
        Assert.assertEquals( "'list/able/not' is listable", false, tags.getTag( "list/able/not" ).isListable() );
    }

//    /**
//     * Test executing a query.
//     */
//    @Test
//    public void testQueryObjects() {
//        // Create an object
//        MetadataList mlist = new MetadataList();
//        Metadata listable = new Metadata( "listable", "foo", true );
//        Metadata unlistable = new Metadata( "unlistable", "bar", false );
//        Metadata listable2 = new Metadata( "list/able/2", "foo2 foo2", true );
//        Metadata unlistable2 = new Metadata( "list/able/not", "bar2 bar2", false );
//        mlist.addMetadata( listable );
//        mlist.addMetadata( unlistable );
//        mlist.addMetadata( listable2 );
//        mlist.addMetadata( unlistable2 );
//        ObjectId id = this.esu.createObject( null, mlist, null, null );
//        Assert.assertNotNull( "null ID returned", id );
//        cleanup.add( id );
//
//        // Query for all objects for the current UID
//        String query = "for $h in collection() where $h/maui:MauiObject[uid=\"" +
//            uid + "\"] return $h";
//        l4j.info( "Query: " + query );
//        List<Identifier> objects = this.esu.queryObjects( query );
//
//        // Ensure the search results contains the object we just created
//        Assert.assertTrue( "object not found in list", objects.contains( id ) );
//
//    }

    /**
     * Tests updating an object's metadata
     */
    @Test
    public void testUpdateObjectMetadata() throws Exception {
        // Create an object
        MetadataList mlist = new MetadataList();
        Metadata unlistable = new Metadata( "unlistable", "foo", false );
        mlist.addMetadata( unlistable );
        ObjectId id = this.esu.createObject( null, mlist, "hello".getBytes( "UTF-8" ), null );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Update the metadata
        unlistable.setValue( "bar" );
        this.esu.setUserMetadata( id, mlist );

        // Re-read the metadata
        MetadataList meta = this.esu.getUserMetadata( id, null );
        Assert.assertEquals( "value of 'unlistable' wrong", "bar", meta.getMetadata( "unlistable" ).getValue() );

        // Check that content was not modified
        String content = new String( this.esu.readObject( id, null, null ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "hello", content );

    }

    @Test
    public void testUpdateObjectAcl() throws Exception {
        // Create an object with an ACL
        Acl acl = new Acl();
        acl.addGrant( new Grant( new Grantee( stripUid( uid ), Grantee.GRANT_TYPE.USER ), Permission.FULL_CONTROL ) );
        Grant other = new Grant( Grantee.OTHER, Permission.READ );
        acl.addGrant( other );
        ObjectId id = this.esu.createObject( acl, null, null, null );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read back the ACL and make sure it matches
        Acl newacl = this.esu.getAcl( id );
        l4j.info( "Comparing " + newacl + " with " + acl );

        Assert.assertEquals( "ACLs don't match", acl, newacl );

        // Change the ACL and update the object.
        acl.removeGrant( other );
        Grant o2 = new Grant( Grantee.OTHER, Permission.NONE );
        acl.addGrant( o2 );
        this.esu.setAcl( id, acl );

        // Read the ACL back and check it
        newacl = this.esu.getAcl( id );
        l4j.info( "Comparing " + newacl + " with " + acl );
        Assert.assertEquals( "ACLs don't match", acl, newacl );
    }

    /**
     * Tests updating an object's contents
     */
    @Test
    public void testUpdateObjectContent() throws Exception {
        // Create an object
        ObjectId id = this.esu.createObject( null, null, "hello".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Update part of the content
        Extent extent = new Extent( 1, 1 );
        this.esu.updateObject( id, null, null, extent, "u".getBytes( "UTF-8" ), null );

        // Read back the content and check it
        String content = new String( this.esu.readObject( id, null, null ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "hullo", content );
    }

    @Test
    public void testUpdateObjectContentStream() throws Exception {
        // Create an object
        ObjectId id = this.esu.createObject( null, null, "hello".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Update part of the content
        Extent extent = new Extent( 1, 1 );
        InputStream in = new ByteArrayInputStream( "u".getBytes( "UTF-8" ) );
        this.esu.updateObjectFromStream( id, null, null, extent, in, 1, null );
        in.close();

        // Read back the content and check it
        String content = new String( this.esu.readObject( id, null, null ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "hullo", content );
    }

    /**
     * Test replacing an object's entire contents
     */
    @Test
    public void testReplaceObjectContent() throws Exception {
        // Create an object
        ObjectId id = this.esu.createObject( null, null, "hello".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Update all of the content
        this.esu.updateObject( id, null, null, null, "bonjour".getBytes( "UTF-8" ), null );

        // Read back the content and check it
        String content = new String( this.esu.readObject( id, null, null ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "bonjour", content );
    }

    /**
     * Test the UploadHelper's create method
     */
    @Test
    public void testCreateHelper() throws Exception {
        // use a blocksize of 1 to test multiple transfers.
        UploadHelper uploadHelper = new UploadHelper( this.esu, new byte[1] );
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write( "hello".getBytes( "UTF-8" ) );

        // Create an object from our file stream
        ObjectId id = uploadHelper.createObject(
                new ByteArrayInputStream( baos.toByteArray() ),
                null, null, true );
        cleanup.add( id );

        // Read contents back and check them
        String content = new String( this.esu.readObject( id, null, null ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "hello", content );
    }

    /**
     * Test the UploadHelper's update method
     */
    @Test
    public void testUpdateHelper() throws Exception {
        // use a blocksize of 1 to test multiple transfers.
        UploadHelper uploadHelper = new UploadHelper( this.esu, new byte[1] );
        uploadHelper.setMimeType( "text/plain" );

        // Create an object with content.
        ObjectId id = this.esu.createObject( null, null, "Four score and twenty years ago".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // update the object contents
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write( "hello".getBytes( "UTF-8" ) );

        uploadHelper.updateObject( id,
                new ByteArrayInputStream( baos.toByteArray() ), null, null, true );

        // Read contents back and check them
        String content = new String( this.esu.readObject( id, null, null ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "hello", content );
    }

    /**
     * Tests the download helper.  Tests both single and multiple requests.
     */
    @Test
    public void testDownloadHelper() throws Exception {
        // Create an object with content.
        ObjectId id = this.esu.createObject( null, null, "Four score and twenty years ago".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Download the content
        DownloadHelper downloadHelper = new DownloadHelper( this.esu, null );
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        downloadHelper.readObject( id, baos, false );

        // Check the download
        String data = new String( baos.toByteArray(), "UTF-8" );
        Assert.assertEquals( "object content wrong", "Four score and twenty years ago", data );

        // Download again 1 byte in a request
        downloadHelper = new DownloadHelper( this.esu, new byte[1] );
        baos = new ByteArrayOutputStream();
        downloadHelper.readObject( id, baos, false );

        // Check the download
        data = new String( baos.toByteArray(), "UTF-8" );
        Assert.assertEquals( "object content wrong", "Four score and twenty years ago", data );
    }

//    @Test
//    public void testUploadDownload() throws Exception {
//        // Create a byte array to test
//        int size=10*1024*1024;
//        byte[] testData = new byte[size];
//        for( int i=0; i<size; i++ ) {
//            testData[i] = (byte)(i%0x93);
//        }
//        UploadHelper uh = new UploadHelper( this.esu, null );
//        
//        ObjectId id = uh.createObject( new ByteArrayInputStream( testData ), null, null, true );
//        cleanup.add( id );
//        
//        ByteArrayOutputStream baos = new ByteArrayOutputStream( size );
//        
//        DownloadHelper dl = new DownloadHelper( this.esu, new byte[4*1024*1024] );
//        dl.readObject( id, baos, true );
//        
//        Assert.assertFalse( "Download should have been OK", dl.isFailed() );
//        Assert.assertNull( "Error should have been null", dl.getError() );
//        
//        byte[] outData = baos.toByteArray();
//        
//        // Check the files
//        Assert.assertEquals( "File lengths differ", testData.length, outData.length );
//
//        Assert.assertArrayEquals( "Data contents differ", testData, outData );
//        
//    }

    @Test
    public void testListDirectory() throws Exception {
        String dir = rand8char();
        String file = rand8char();
        String dir2 = rand8char();
        ObjectPath dirPath = new ObjectPath( "/" + dir + "/" );
        ObjectPath op = new ObjectPath( "/" + dir + "/" + file );
        ObjectPath dirPath2 = new ObjectPath( "/" + dir + "/" + dir2 + "/" );

        ObjectId dirId = this.esu.createObjectOnPath( dirPath, null, null, null, null );
        ObjectId id = this.esu.createObjectOnPath( op, null, null, null, null );
        this.esu.createObjectOnPath( dirPath2, null, null, null, null );
        cleanup.add( op );
        cleanup.add( dirPath2 );
        cleanup.add( dirPath );
        l4j.debug( "Path: " + op + " ID: " + id );
        Assert.assertNotNull( id );
        Assert.assertNotNull( dirId );

        // Read back the content
        String content = new String( this.esu.readObject( op, null, null ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "", content );
        content = new String( this.esu.readObject( id, null, null ), "UTF-8" );
        Assert.assertEquals( "object content wrong when reading by id", "", content );

        // List the parent path
        List<DirectoryEntry> dirList = esu.listDirectory( dirPath, null );
        l4j.debug( "Dir content: " + content );
        Assert.assertTrue( "File not found in directory", directoryContains( dirList, op ) );
        Assert.assertTrue( "subdirectory not found in directory", directoryContains( dirList, dirPath2 ) );
    }

    @Test
    public void testListDirectoryPaged() throws Exception {
        String dir = rand8char();
        String file = rand8char();
        String dir2 = rand8char();
        ObjectPath dirPath = new ObjectPath( "/" + dir + "/" );
        ObjectPath op = new ObjectPath( "/" + dir + "/" + file );
        ObjectPath dirPath2 = new ObjectPath( "/" + dir + "/" + dir2 + "/" );

        ObjectId dirId = this.esu.createObjectOnPath( dirPath, null, null, null, null );
        ObjectId id = this.esu.createObjectOnPath( op, null, null, null, null );
        this.esu.createObjectOnPath( dirPath2, null, null, null, null );
        cleanup.add( op );
        cleanup.add( dirPath2 );
        cleanup.add( dirPath );
        l4j.debug( "Path: " + op + " ID: " + id );
        Assert.assertNotNull( id );
        Assert.assertNotNull( dirId );

        // List the parent path
        ListOptions options = new ListOptions();
        options.setLimit( 1 );
        List<DirectoryEntry> dirList = esu.listDirectory( dirPath, options );

        Assert.assertNotNull( "Token should have been returned", options.getToken() );
        l4j.debug( "listDirectoryPaged, token: " + options.getToken() );
        while ( options.getToken() != null ) {
            dirList.addAll( esu.listDirectory( dirPath, options ) );
        }

        Assert.assertTrue( "File not found in directory", directoryContains( dirList, op ) );
        Assert.assertTrue( "subdirectory not found in directory", directoryContains( dirList, dirPath2 ) );
    }

    @Test
    public void testListDirectoryWithMetadata() throws Exception {
        String dir = rand8char();
        String file = rand8char();
        String dir2 = rand8char();
        ObjectPath dirPath = new ObjectPath( "/" + dir + "/" );
        ObjectPath op = new ObjectPath( "/" + dir + "/" + file );
        ObjectPath dirPath2 = new ObjectPath( "/" + dir + "/" + dir2 + "/" );

        MetadataList mlist = new MetadataList();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "list/able/2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "list/able/not", "bar2 bar2", false );
        mlist.addMetadata( listable );
        mlist.addMetadata( unlistable );
        mlist.addMetadata( listable2 );
        mlist.addMetadata( unlistable2 );

        ObjectId dirId = this.esu.createObjectOnPath( dirPath, null, null, null, null );
        ObjectId id = this.esu.createObjectOnPath( op, null, mlist, null, null );
        this.esu.createObjectOnPath( dirPath2, null, null, null, null );
        cleanup.add( op );
        cleanup.add( dirPath2 );
        cleanup.add( dirPath );
        l4j.debug( "Path: " + op + " ID: " + id );
        Assert.assertNotNull( id );
        Assert.assertNotNull( dirId );

        // List the parent path
        ListOptions options = new ListOptions();
        options.setIncludeMetadata( true );
        List<DirectoryEntry> dirList = esu.listDirectory( dirPath, options );
        Assert.assertTrue( "File not found in directory", directoryContains( dirList, op ) );
        Assert.assertTrue( "subdirectory not found in directory", directoryContains( dirList, dirPath2 ) );

        for ( Iterator<DirectoryEntry> i = dirList.iterator(); i.hasNext(); ) {
            DirectoryEntry de = i.next();
            if ( de.getPath().equals( op ) ) {
                // Check the metadata
                Assert.assertEquals( "Wrong value on metadata",
                        de.getUserMetadata().getMetadata( "listable" ).getValue(), "foo" );

            }
        }
        Assert.assertTrue( "File not found in directory", directoryContains( dirList, op ) );
        Assert.assertTrue( "subdirectory not found in directory", directoryContains( dirList, dirPath2 ) );
    }

    @Test
    public void testListDirectoryWithSomeMetadata() throws Exception {
        String dir = rand8char();
        String file = rand8char();
        String dir2 = rand8char();
        ObjectPath dirPath = new ObjectPath( "/" + dir + "/" );
        ObjectPath op = new ObjectPath( "/" + dir + "/" + file );
        ObjectPath dirPath2 = new ObjectPath( "/" + dir + "/" + dir2 + "/" );

        MetadataList mlist = new MetadataList();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "list/able/2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "list/able/not", "bar2 bar2", false );
        mlist.addMetadata( listable );
        mlist.addMetadata( unlistable );
        mlist.addMetadata( listable2 );
        mlist.addMetadata( unlistable2 );

        ObjectId dirId = this.esu.createObjectOnPath( dirPath, null, null, null, null );
        ObjectId id = this.esu.createObjectOnPath( op, null, mlist, null, null );
        this.esu.createObjectOnPath( dirPath2, null, null, null, null );
        cleanup.add( op );
        cleanup.add( dirPath2 );
        cleanup.add( dirPath );
        l4j.debug( "Path: " + op + " ID: " + id );
        Assert.assertNotNull( id );
        Assert.assertNotNull( dirId );

        // List the parent path
        ListOptions options = new ListOptions();
        options.setIncludeMetadata( true );
        options.setUserMetadata( Arrays.asList( new String[]{"listable"} ) );
        List<DirectoryEntry> dirList = esu.listDirectory( dirPath, options );
        Assert.assertTrue( "File not found in directory", directoryContains( dirList, op ) );
        Assert.assertTrue( "subdirectory not found in directory", directoryContains( dirList, dirPath2 ) );

        for ( Iterator<DirectoryEntry> i = dirList.iterator(); i.hasNext(); ) {
            DirectoryEntry de = i.next();
            if ( de.getPath().equals( op ) ) {
                // Check the metadata
                Assert.assertEquals( "Wrong value on metadata",
                        de.getUserMetadata().getMetadata( "listable" ).getValue(), "foo" );
                // Other metadata should not be present
                Assert.assertNull( "unlistable should be missing",
                        de.getUserMetadata().getMetadata( "unlistable" ) );
            }
        }
        Assert.assertTrue( "File not found in directory", directoryContains( dirList, op ) );
        Assert.assertTrue( "subdirectory not found in directory", directoryContains( dirList, dirPath2 ) );
    }


    private boolean directoryContains( List<DirectoryEntry> dir, ObjectPath path ) {
        for ( Iterator<DirectoryEntry> i = dir.iterator(); i.hasNext(); ) {
            DirectoryEntry de = i.next();
            if ( de.getPath().equals( path ) ) {
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
        String parentPath = "/dottest/";
        String dotPath = parentPath + "./";
        String dotdotPath = parentPath + "../";
        String filename = "test.txt";
        byte[] content = "Hello World!".getBytes( "UTF-8" );

        // isolate this test in the namespace
        ObjectId parentId = null;
        try {
            parentId = this.esu.createObjectOnPath( new ObjectPath( parentPath ), null, null, null, null );
        } catch(EsuException e) {
            if(e.getAtmosCode() == 1016) {
                deleteRecursively(new ObjectPath( parentPath ));
                parentId = this.esu.createObjectOnPath( new ObjectPath( parentPath ), null, null, null, null );
            } else {
                throw e;
            }
        }

        // test single dot path (./)
        ObjectId fileId = this.esu.createObjectOnPath( new ObjectPath( parentPath + "hidden.txt" ), null, null, content, "text/plain" );
        cleanup.add( fileId );
        ObjectId dirId = this.esu.createObjectOnPath( new ObjectPath( dotPath ), null, null, null, null );
        Assert.assertNotNull( "null ID returned on dot path creation", dirId );
        fileId = this.esu.createObjectOnPath( new ObjectPath( dotPath + filename ), null, null, content, "text/plain" );

        // make sure we only see one file (the "." path is its own directory and not a synonym for the current directory)
        List<DirectoryEntry> entries = this.esu.listDirectory( new ObjectPath( dotPath ), null );
        Assert.assertEquals( "dot path listing was not 1", entries.size(), 1 );
        Assert.assertTrue( "dot path listing did not contain a dot in the path", entries.get( 0 ).getPath().toString().contains( dotPath ) );
        Assert.assertEquals( "dot path listing did not contain test file", entries.get( 0 ).getPath().getName(), filename );
        cleanup.add( fileId );
        cleanup.add( dirId );

        // test double dot path (../)
        dirId = this.esu.createObjectOnPath( new ObjectPath( dotdotPath ), null, null, null, null );
        Assert.assertNotNull( "null ID returned on dotdot path creation", dirId );
        fileId = this.esu.createObjectOnPath( new ObjectPath( dotdotPath + filename ), null, null, content, "text/plain" );

        // make sure we only see one file (the ".." path is its own directory and not a synonym for the parent directory)
        entries = this.esu.listDirectory( new ObjectPath( dotdotPath ), null );
        Assert.assertEquals( "dotdot path listing was not 1", entries.size(), 1 );
        Assert.assertTrue( "dotdot path listing did not contain a dotdot in the path", entries.get( 0 ).getPath().toString().contains( dotdotPath ) );
        Assert.assertEquals( "dotdot path listing did not contain test file", entries.get( 0 ).getPath().getName(), filename );
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
        // Create an object with an ACL
        Acl acl = new Acl();
        acl.addGrant( new Grant( new Grantee( uid, Grantee.GRANT_TYPE.USER ), Permission.FULL_CONTROL ) );
        acl.addGrant( new Grant( Grantee.OTHER, Permission.READ ) );
        MetadataList mlist = new MetadataList();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "listable2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "unlistable2", "bar2 bar2", false );
        mlist.addMetadata( listable );
        mlist.addMetadata( unlistable );
        mlist.addMetadata( listable2 );
        mlist.addMetadata( unlistable2 );

        String mimeType = "test/mimetype";
        String content = "test";

        ObjectId id = this.esu.createObjectOnPath( op, acl, null, content.getBytes( "UTF-8" ), mimeType );
        this.esu.updateObject( op, null, mlist, null, null, mimeType );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( op );

        // Read it back with HEAD call
        ObjectMetadata om = this.esu.getAllMetadata( op );
        Assert.assertNotNull( "value of 'listable' missing", om.getMetadata().getMetadata( "listable" ) );
        Assert.assertNotNull( "value of 'unlistable' missing", om.getMetadata().getMetadata( "unlistable" ) );
        Assert.assertNotNull( "value of 'atime' missing", om.getMetadata().getMetadata( "atime" ) );
        Assert.assertNotNull( "value of 'ctime' missing", om.getMetadata().getMetadata( "ctime" ) );
        Assert.assertEquals( "value of 'listable' wrong", "foo", om.getMetadata().getMetadata( "listable" ).getValue() );
        Assert.assertEquals( "value of 'unlistable' wrong", "bar", om.getMetadata().getMetadata( "unlistable" ).getValue() );
        Assert.assertEquals( "Mimetype incorrect", mimeType, om.getMimeType() );

        // Check the ACL
        // not checking this by path because an extra groupid is added 
        // during the create calls by path.
        //Assert.assertEquals( "ACLs don't match", acl, om.getAcl() );

    }

    @Test
    public void testGetAllMetadataById() throws Exception {
        // Create an object with an ACL
        Acl acl = new Acl();
        acl.addGrant( new Grant( new Grantee( uid, Grantee.GRANT_TYPE.USER ), Permission.FULL_CONTROL ) );
        acl.addGrant( new Grant( Grantee.OTHER, Permission.READ ) );
        MetadataList mlist = new MetadataList();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "listable2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "unlistable2", "bar2 bar2", false );
        mlist.addMetadata( listable );
        mlist.addMetadata( unlistable );
        mlist.addMetadata( listable2 );
        mlist.addMetadata( unlistable2 );

        String mimeType = "test/mimetype";
        String content = "test";

        ObjectId id = this.esu.createObject( acl, mlist, content.getBytes( "UTF-8" ), mimeType );

        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read it back with HEAD call
        ObjectMetadata om = this.esu.getAllMetadata( id );
        Assert.assertNotNull( "value of 'listable' missing", om.getMetadata().getMetadata( "listable" ) );
        Assert.assertNotNull( "value of 'unlistable' missing", om.getMetadata().getMetadata( "unlistable" ) );
        Assert.assertNotNull( "value of 'atime' missing", om.getMetadata().getMetadata( "atime" ) );
        Assert.assertNotNull( "value of 'ctime' missing", om.getMetadata().getMetadata( "ctime" ) );
        Assert.assertEquals( "value of 'listable' wrong", "foo", om.getMetadata().getMetadata( "listable" ).getValue() );
        Assert.assertEquals( "value of 'unlistable' wrong", "bar", om.getMetadata().getMetadata( "unlistable" ).getValue() );
        Assert.assertEquals( "Mimetype incorrect", mimeType, om.getMimeType() );

        // Check the ACL
        //Assert.assertEquals( "ACLs don't match", acl, om.getAcl() );

    }

    /**
     * Tests getting object replica information.
     */
    @Test
    public void testGetObjectReplicaInfo() throws Exception {
        ObjectId id = this.esu.createObject( null, null, "hello".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        MetadataTags mt = new MetadataTags();
        mt.addTag( new MetadataTag( "user.maui.lso", false ) );
        MetadataList meta = this.esu.getUserMetadata( id, mt );
        Assert.assertNotNull( meta.getMetadata( "user.maui.lso" ) );
        l4j.debug( "Replica info: " + meta.getMetadata( "user.maui.lso" ) );
    }

    @Test
    public void testCreateHelperWithPath() throws Exception {
        String dir = rand8char();
        String file = rand8char();

        ObjectPath op = new ObjectPath( "/" + dir + "/" + file );

        // use a blocksize of 1 to test multiple transfers.
        UploadHelper uploadHelper = new UploadHelper( this.esu, new byte[1] );
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write( "hello".getBytes( "UTF-8" ) );

        // Create an object from our file stream
        ObjectId id = uploadHelper.createObjectOnPath( op,
                new ByteArrayInputStream( baos.toByteArray() ),
                null, null, true );
        cleanup.add( op );

        // Read contents back and check them
        String content = new String( this.esu.readObject( id, null, null ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "hello", content );

    }

    @Test
    public void testUpdateHelperWithPath() throws Exception {
        String dir = rand8char();
        String file = rand8char();

        ObjectPath op = new ObjectPath( "/" + dir + "/" + file );

        // use a blocksize of 1 to test multiple transfers.
        UploadHelper uploadHelper = new UploadHelper( this.esu, new byte[1] );
        uploadHelper.setMimeType( "text/plain" );

        // Create an object with content.
        ObjectId id = this.esu.createObjectOnPath( op, null, null, "Four score and twenty years ago".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( op );

        // update the object contents
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write( "hello".getBytes( "UTF-8" ) );

        uploadHelper.updateObject( op,
                new ByteArrayInputStream( baos.toByteArray() ), null, null, true );

        // Read contents back and check them
        String content = new String( this.esu.readObject( id, null, null ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "hello", content );
    }

    @Test
    public void testGetShareableUrl() throws Exception {
        // Create an object with content.
        String str = "Four score and twenty years ago";
        ObjectId id = this.esu.createObject( null, null, str.getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        Calendar c = Calendar.getInstance();
        c.add( Calendar.HOUR, 4 );
        Date expiration = c.getTime();
        URL u = esu.getShareableUrl( id, expiration );

        l4j.debug( "Sharable URL: " + u );

        InputStream stream = (InputStream) u.getContent();
        BufferedReader br = new BufferedReader( new InputStreamReader( stream ) );
        String content = br.readLine();
        l4j.debug( "Content: " + content );
        Assert.assertEquals( "URL does not contain proper content",
                str, content.toString() );
    }

    @Test
    public void testGetShareableUrlWithPath() throws Exception {
        // Create an object with content.
        String str = "Four score and twenty years ago";
        ObjectPath op = new ObjectPath( "/" + rand8char() + ".txt" );
        ObjectId id = this.esu.createObjectOnPath( op, null, null, str.getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( op );

        Calendar c = Calendar.getInstance();
        c.add( Calendar.HOUR, 4 );
        Date expiration = c.getTime();
        URL u = esu.getShareableUrl( op, expiration );

        l4j.debug( "Sharable URL: " + u );

        InputStream stream = (InputStream) u.getContent();
        BufferedReader br = new BufferedReader( new InputStreamReader( stream ) );
        String content = br.readLine();
        l4j.debug( "Content: " + content );
        Assert.assertEquals( "URL does not contain proper content",
                str, content.toString() );
    }

    @Test
    public void testExpiredSharableUrl() throws Exception {
        // Create an object with content.
        String str = "Four score and twenty years ago";
        ObjectId id = this.esu.createObject( null, null,
                str.getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        Calendar c = Calendar.getInstance();
        c.add( Calendar.HOUR, -4 );
        Date expiration = c.getTime();
        URL u = esu.getShareableUrl( id, expiration );

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
        ObjectId id = this.esu.createObject( null, null, "hello".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read back the content
        InputStream in = this.esu.readObjectStream( id, null );
        BufferedReader br = new BufferedReader( new InputStreamReader( in, "UTF-8" ) );
        String content = br.readLine();
        br.close();
        Assert.assertEquals( "object content wrong", "hello", content );

        // Read back only 2 bytes
        Extent extent = new Extent( 1, 2 );
        in = this.esu.readObjectStream( id, extent );
        br = new BufferedReader( new InputStreamReader( in, "UTF-8" ) );
        content = br.readLine();
        br.close();
        Assert.assertEquals( "partial object content wrong", "el", content );
    }

    @Test
    public void testCreateChecksum() throws Exception {
        Checksum ck = new Checksum( Algorithm.SHA0 );
        ObjectId id = this.esu.createObject( null, null, "hello".getBytes( "UTF-8" ), "text/plain", ck );
        l4j.debug( "Checksum: " + ck );
        cleanup.add( id );
    }

    /**
     * Note, to test read checksums, see comment in testReadChecksum
     *
     * @throws Exception
     */
    @Test
    public void testUploadDownloadChecksum() throws Exception {
        // Create a byte array to test
        int size = 10 * 1024 * 1024;
        byte[] testData = new byte[size];
        for ( int i = 0; i < size; i++ ) {
            testData[i] = (byte) (i % 0x93);
        }
        MetadataList mlist = new MetadataList();
        Metadata policy = new Metadata( "policy", "erasure", false );
        mlist.addMetadata( policy );
        UploadHelper uh = new UploadHelper( this.esu, null );
        uh.setChecksumming( true );

        ObjectId id = uh.createObject( new ByteArrayInputStream( testData ), null, mlist, true );
        cleanup.add( id );

        ByteArrayOutputStream baos = new ByteArrayOutputStream( size );

        DownloadHelper dl = new DownloadHelper( this.esu, new byte[4 * 1024 * 1024] );
        dl.setChecksumming( true );
        dl.readObject( id, baos, true );

        Assert.assertFalse( "Download should have been OK", dl.isFailed() );
        Assert.assertNull( "Error should have been null", dl.getError() );

        byte[] outData = baos.toByteArray();

        // Check the files
        Assert.assertEquals( "File lengths differ", testData.length, outData.length );

        Assert.assertArrayEquals( "Data contents differ", testData, outData );

    }

    @Test
    public void testUnicodeMetadata() throws Exception {
        MetadataList mlist = new MetadataList();
        Metadata nbspValue = new Metadata( "nbspvalue", "Nobreak\u00A0Value", false );
        Metadata nbspName = new Metadata( "Nobreak\u00A0Name", "regular text here", false );
        Metadata cryllic = new Metadata( "cryllic", "спасибо", false );
        l4j.debug( "NBSP Value: " + nbspValue );
        l4j.debug( "NBSP Name: " + nbspName );

        mlist.addMetadata( nbspValue );
        mlist.addMetadata( nbspName );
        mlist.addMetadata( cryllic );

        ObjectId id = this.esu.createObject( null, mlist, null, null );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read and validate the metadata
        MetadataList meta = this.esu.getUserMetadata( id, null );
        l4j.debug( "Read Back:" );
        l4j.debug( "NBSP Value: " + meta.getMetadata( "nbspvalue" ) );
        l4j.debug( "NBSP Name: " + meta.getMetadata( "Nobreak\u00A0Name" ) );
        Assert.assertEquals( "value of 'nobreakvalue' wrong", "Nobreak\u00A0Value", meta.getMetadata( "nbspvalue" ).getValue() );
        Assert.assertEquals( "Value of cryllic wrong", "спасибо", meta.getMetadata( "cryllic" ).getValue() );
    }

    @Test
    public void testUtf8Metadata() throws Exception {
        String oneByteCharacters = "Hello! ";
        String twoByteCharacters = "\u0410\u0411\u0412\u0413"; // Cyrillic letters
        String fourByteCharacters = "\ud841\udf0e\ud841\udf31\ud841\udf79\ud843\udc53"; // Chinese symbols
        String utf8String = oneByteCharacters + twoByteCharacters + fourByteCharacters;

        MetadataList metaList = new MetadataList();
        metaList.addMetadata( new Metadata( "utf8Key", utf8String, false ) );
        metaList.addMetadata( new Metadata( utf8String, "utf8Value", false ) );

        ObjectId id = this.esu.createObject( null, metaList, null, null );
        cleanup.add( id );

        // list all tags and make sure the UTF8 tag is in the list
        MetadataTags tags = this.esu.listUserMetadataTags( id );
        Assert.assertTrue( "UTF8 key not found in tag list", tags.contains( utf8String ) );

        // get the user metadata and make sure all UTF8 characters are accurate
        metaList = this.esu.getUserMetadata( id, null );
        Metadata meta = metaList.getMetadata( utf8String );
        Assert.assertEquals( "UTF8 key does not match", meta.getName(), utf8String );
        Assert.assertEquals( "UTF8 key value does not match", meta.getValue(), "utf8Value" );
        Assert.assertEquals( "UTF8 value does not match", metaList.getMetadata( "utf8Key" ).getValue(), utf8String );

        // test set metadata with UTF8
        metaList = new MetadataList();
        metaList.addMetadata( new Metadata( "newKey", utf8String + "2", false ) );
        metaList.addMetadata( new Metadata( utf8String + "2", "newValue", false ) );
        this.esu.setUserMetadata( id, metaList );

        // verify set metadata call (also testing getAllMetadata)
        ObjectMetadata objMeta = this.esu.getAllMetadata( id );
        metaList = objMeta.getMetadata();
        meta = metaList.getMetadata( utf8String + "2" );
        Assert.assertEquals( "UTF8 key does not match", meta.getName(), utf8String + "2" );
        Assert.assertEquals( "UTF8 key value does not match", meta.getValue(), "newValue" );
        Assert.assertEquals( "UTF8 value does not match", metaList.getMetadata( "newKey" ).getValue(), utf8String + "2" );
    }

    @Test
    public void testUtf8MetadataFilter() throws Exception {
        String oneByteCharacters = "Hello! ";
        String twoByteCharacters = "\u0410\u0411\u0412\u0413"; // Cyrillic letters
        String fourByteCharacters = "\ud841\udf0e\ud841\udf31\ud841\udf79\ud843\udc53"; // Chinese symbols
        String utf8String = oneByteCharacters + twoByteCharacters + fourByteCharacters;

        MetadataList metaList = new MetadataList();
        metaList.addMetadata( new Metadata( "utf8Key", utf8String, false ) );
        metaList.addMetadata( new Metadata( utf8String, "utf8Value", false ) );

        ObjectId id = this.esu.createObject( null, metaList, null, null );
        cleanup.add( id );

        // apply a filter that includes the UTF8 tag
        MetadataTags tags = new MetadataTags();
        tags.addTag( new MetadataTag( utf8String, false ) );
        metaList = this.esu.getUserMetadata( id, tags );
        Assert.assertEquals( "UTF8 filter was not honored", metaList.count(), 1 );
        Assert.assertNotNull( "UTF8 key was not found in filtered results", metaList.getMetadata( utf8String ) );
    }

    @Test
    public void testUtf8DeleteMetadata() throws Exception {
        String oneByteCharacters = "Hello! ";
        String twoByteCharacters = "\u0410\u0411\u0412\u0413"; // Cyrillic letters
        String fourByteCharacters = "\ud841\udf0e\ud841\udf31\ud841\udf79\ud843\udc53"; // Chinese symbols
        String utf8String = oneByteCharacters + twoByteCharacters + fourByteCharacters;

        MetadataList metaList = new MetadataList();
        metaList.addMetadata( new Metadata( "utf8Key", utf8String, false ) );
        metaList.addMetadata( new Metadata( utf8String, "utf8Value", false ) );

        ObjectId id = this.esu.createObject( null, metaList, null, null );
        cleanup.add( id );

        // delete the UTF8 tag
        MetadataTags tags = new MetadataTags();
        tags.addTag( new MetadataTag( utf8String, false ) );
        this.esu.deleteUserMetadata( id, tags );

        // verify delete was successful
        tags = this.esu.listUserMetadataTags( id );
        Assert.assertFalse( "UTF8 key was not deleted", tags.contains( utf8String ) );
    }

    @Test
    public void testUtf8ListableMetadata() throws Exception {
        String oneByteCharacters = "Hello! ";
        String twoByteCharacters = "\u0410\u0411\u0412\u0413"; // Cyrillic letters
        String fourByteCharacters = "\ud841\udf0e\ud841\udf31\ud841\udf79\ud843\udc53"; // Chinese symbols
        String utf8String = oneByteCharacters + twoByteCharacters + fourByteCharacters;

        MetadataList metaList = new MetadataList();
        metaList.addMetadata( new Metadata( utf8String, "utf8Value", true ) );

        ObjectId id = this.esu.createObject( null, metaList, null, null );
        cleanup.add( id );

        metaList = this.esu.getUserMetadata( id, null );
        Metadata meta = metaList.getMetadata( utf8String );
        Assert.assertEquals( "UTF8 key does not match", meta.getName(), utf8String );
        Assert.assertEquals( "UTF8 key value does not match", meta.getValue(), "utf8Value" );
        Assert.assertTrue( "UTF8 metadata is not listable", meta.isListable() );

        // verify we can list the tag and see our object
        boolean found = false;
        for ( ObjectResult result : this.esu.listObjects( utf8String, null ) ) {
            if ( result.getId().equals( id ) ) {
                found = true;
                break;
            }
        }
        Assert.assertTrue( "UTF8 tag listing did not contain the correct object ID", found );

        // verify we can list child tags of the UTF8 tag
        MetadataTags tags = this.esu.getListableTags( new MetadataTag( utf8String, true ) );
        Assert.assertNotNull( "UTF8 child tag listing was null", tags );
    }

    //@Test
    public void testUtf8ListableTagWithComma() {
        String stringWithComma = "Hello, you!";

        MetadataList metaList = new MetadataList();
        metaList.addMetadata( new Metadata( stringWithComma, "value", true ) );

        ObjectId id = this.esu.createObject( null, metaList, null, null );
        cleanup.add( id );

        metaList = this.esu.getUserMetadata( id, null );
        Metadata meta = metaList.getMetadata( stringWithComma );
        Assert.assertEquals( "key does not match", meta.getName(), stringWithComma );
        Assert.assertTrue( "metadata is not listable", meta.isListable() );

        boolean found = false;
        for ( ObjectResult result : this.esu.listObjects( stringWithComma, null ) ) {
            if ( result.getId().equals( id ) ) {
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

        ObjectId id = this.esu.createObjectOnPath( op1, null, null, "Four score and seven years ago".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Rename
        this.esu.rename( op1, op2, false );

        // Read back the content
        String content = new String( this.esu.readObject( op2, null, null ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "Four score and seven years ago", content );

    }

    @Test
    public void testRenameOverwrite() throws Exception {
        ObjectPath op1 = new ObjectPath( "/" + rand8char() + ".tmp" );
        ObjectPath op2 = new ObjectPath( "/" + rand8char() + ".tmp" );

        ObjectId id = this.esu.createObjectOnPath( op1, null, null, "Four score and seven years ago".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        ObjectId id2 = this.esu.createObjectOnPath( op2, null, null, "You should not see this".getBytes( "UTF-8" ), "text/plain" );
        cleanup.add( id2 );

        // Rename
        this.esu.rename( op1, op2, true );

        // Wait for overwrite to complete
        Thread.sleep( 5000 );

        // Read back the content
        String content = new String( this.esu.readObject( op2, null, null ), "UTF-8" );
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
        ObjectPath parentDir = createTestDir("Utf8Rename");
        String normalName = parentDir + rand8char() + ".tmp";
        String crazyName = parentDir + oneByteCharacters + twoByteCharacters + fourByteCharacters;
        byte[] content = "This is a really crazy name.".getBytes( "UTF-8" );

        // normal name
        this.esu.createObjectOnPath( new ObjectPath( normalName ), null, null, content, "text/plain" );

        // crazy multi-byte character name
        this.esu.rename( new ObjectPath( normalName ), new ObjectPath( crazyName ), true );

        // Wait for overwrite to complete
        Thread.sleep( 5000 );

        // verify name in directory list
        boolean found = false;
        for ( DirectoryEntry entry : this.esu.listDirectory( parentDir, null ) ) {
            if ( entry.getPath().toString().equals( crazyName ) ) {
                found = true;
                break;
            }
        }
        Assert.assertTrue( "crazyName not found in directory listing", found );

        // Read back the content
        Assert.assertTrue( "object content wrong", Arrays.equals( content, this.esu.readObject( new ObjectPath( crazyName ), null, null ) ) );
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
        MetadataList mlist = new MetadataList();
        Metadata policy = new Metadata( "policy", "erasure", false );
        mlist.addMetadata( policy );
        Checksum createChecksum = new Checksum( Algorithm.SHA0 );
        ObjectId id = this.esu.createObject( null, mlist, "hello".getBytes( "UTF-8" ), "text/plain", createChecksum );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read back the content
        Checksum readChecksum = new Checksum( Algorithm.SHA0 );
        String content = new String( this.esu.readObject( id, null, null, readChecksum ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "hello", content );
    }

    /**
     * Tests getting the service information
     */
    @Test
    public void testGetServiceInformation() throws Exception {
        ServiceInformation si = this.esu.getServiceInformation();

        Assert.assertNotNull( "Atmos version is null", si.getAtmosVersion() );
    }

    /**
     * Test getting object info.  Note to fully run this testcase, you should
     * create a policy named 'retaindelete' that keys off of the metadata
     * policy=retaindelete that includes a retention and deletion criteria.
     */
    @Test
    public void testGetObjectInfo() throws Exception {
        MetadataList mlist = new MetadataList();
        Metadata policy = new Metadata( "policy", "retaindelete", false );
        mlist.addMetadata( policy );
        ObjectId id = this.esu.createObject( null, mlist, "hello".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read back the content
        String content = new String( this.esu.readObject( id, null, null ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "hello", content );

        // Get the object info
        ObjectInfo oi = this.esu.getObjectInfo( id );
        Assert.assertNotNull( "ObjectInfo null", oi );
        Assert.assertNotNull( "ObjectInfo expiration null", oi.getExpiration() );
        Assert.assertNotNull( "ObjectInfo objectid null", oi.getObjectId() );
        Assert.assertNotNull( "ObjectInfo raw xml null", oi.getRawXml() );
        Assert.assertNotNull( "ObjectInfo replicas null", oi.getReplicas() );
        Assert.assertNotNull( "ObjectInfo retention null", oi.getRetention() );
        Assert.assertNotNull( "ObjectInfo selection null", oi.getSelection() );
        Assert.assertTrue( "ObjectInfo should have at least one replica", oi.getReplicas().size() > 0 );

    }

    @Test
    public void testHmac() throws Exception {
        // Compute the signature hash
        String input = "Hello World";
        byte[] secret = Base64.decodeBase64( "D7qsp4j16PBHWSiUbc/bt3lbPBY=".getBytes( "UTF-8" ) );
        Mac mac = Mac.getInstance( "HmacSHA1" );
        SecretKeySpec key = new SecretKeySpec( secret, "HmacSHA1" );
        mac.init( key );
        l4j.debug( "Hashing: \n" + input.toString() );

        byte[] hashData = mac.doFinal( input.toString().getBytes( "ISO-8859-1" ) );

        // Encode the hash in Base64.
        String hashOut = new String( Base64.encodeBase64( hashData ), "UTF-8" );

        l4j.debug( "Hash: " + hashOut );
    }

    @Test
    public void testDirectoryMetadata() throws Exception {
    	ObjectPath dir = new ObjectPath("/" + rand8char() + "/");
        MetadataList mlist = new MetadataList();
        Metadata listable = new Metadata( "listable", "foo", true );
        Metadata unlistable = new Metadata( "unlistable", "bar", false );
        Metadata listable2 = new Metadata( "listable2", "foo2 foo2", true );
        Metadata unlistable2 = new Metadata( "unlistable2", "bar2 bar2", false );
        Metadata listable3 = new Metadata( "listable3", null, true );
        //Metadata withCommas = new Metadata( "withcommas", "I, Robot", false );
        //Metadata withEquals = new Metadata( "withequals", "name=value", false );
        mlist.addMetadata( listable );
        mlist.addMetadata( unlistable );
        mlist.addMetadata( listable2 );
        mlist.addMetadata( unlistable2 );
        mlist.addMetadata( listable3 );
        //mlist.addMetadata( withCommas );
        //mlist.addMetadata( withEquals );
        ObjectId id = this.esu.createObjectOnPath( dir, null, mlist, null, null );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        // Read and validate the metadata
        MetadataList meta = this.esu.getAllMetadata( dir ).getMetadata();
        Assert.assertEquals( "value of 'listable' wrong", "foo", meta.getMetadata( "listable" ).getValue() );
        Assert.assertEquals( "value of 'listable2' wrong", "foo2 foo2", meta.getMetadata( "listable2" ).getValue() );
        Assert.assertEquals( "value of 'unlistable' wrong", "bar", meta.getMetadata( "unlistable" ).getValue() );
        Assert.assertEquals( "value of 'unlistable2' wrong", "bar2 bar2", meta.getMetadata( "unlistable2" ).getValue() );
        Assert.assertNotNull( "listable3 missing", meta.getMetadata( "listable3" ) );
        Assert.assertTrue( "Value of listable3 should be empty", meta.getMetadata( "listable3" ).getValue() == null || meta.getMetadata( "listable3" ).getValue().length() == 0 );
        //Assert.assertEquals( "Value of withcommas wrong", "I, Robot", meta.getMetadata( "withcommas" ).getValue() );
        //Assert.assertEquals( "Value of withequals wrong", "name=value", meta.getMetadata( "withequals" ).getValue() );

        // Check listable flags
        Assert.assertEquals( "'listable' is not listable", true, meta.getMetadata( "listable" ).isListable() );
        Assert.assertEquals( "'listable2' is not listable", true, meta.getMetadata( "listable2" ).isListable() );
        Assert.assertEquals( "'listable3' is not listable", true, meta.getMetadata( "listable3" ).isListable() );
        Assert.assertEquals( "'unlistable' is listable", false, meta.getMetadata( "unlistable" ).isListable() );
        Assert.assertEquals( "'unlistable2' is listable", false, meta.getMetadata( "unlistable2" ).isListable() );

    }

    /**
     * Tests fetching data with a MultiExtent.
     */
    //@Test
    public void testMultiExtent() throws Exception {
    	String input = "Four score and seven years ago";
    	ObjectId id = esu.createObject(null, null, input.getBytes("UTF-8"), "text/plain");
    	cleanup.add(id);
    	Assert.assertNotNull("Object null", id);

    	MultiExtent me = new MultiExtent();
    	me.add(new Extent(27,2)); //ag
    	me.add(new Extent(9,1)); // e
    	me.add(new Extent(5,1)); // s
    	me.add(new Extent(4,1)); // ' '
    	me.add(new Extent(27,3)); // ago

    	Assert.assertEquals("Extent string wrong",
    			"bytes=27-28,9-9,5-5,4-4,27-29", me.toString());
    	byte[] data = esu.readObject(id, me, null);
    	String out = new String(data, "UTF-8");
    	Assert.assertEquals("Content incorrect", "ages ago", out);
    }

    //---------- Features supported by the Atmos 2.0 REST API. ----------\\

    // TODO: If this is not supported, remove it
    @Ignore("Turned off by default.")
    @Test
    public void testHardLink() throws Exception {
        ObjectPath op1 = new ObjectPath("/" + rand8char() + ".tmp");
        ObjectPath op2 = new ObjectPath("/" + rand8char() + ".tmp");

        ObjectId id = this.esu.createObjectOnPath( op1, null, null, "Four score and seven years ago".getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        l4j.info("nlink after create: " + this.esu.getSystemMetadata(
                op1, null).getMetadata("nlink").getValue());

        // Rename
        this.esu.hardLink( op1, op2 );

        l4j.info("nlink after hardlink: " + this.esu.getSystemMetadata(
                op1, null).getMetadata("nlink").getValue());

        // Read back the content
        String content = new String( this.esu.readObject( op2, null, null ), "UTF-8" );
        Assert.assertEquals( "object content wrong", "Four score and seven years ago", content );

        this.esu.deleteObject(op2);

        l4j.info("nlink after delete: " + this.esu.getSystemMetadata(
                op1, null).getMetadata("nlink").getValue());

    }

    @Test
    public void testGetShareableUrlAndDisposition() throws Exception {
        // Create an object with content.
        String str = "Four score and twenty years ago";
        ObjectId id = this.esu.createObject( null, null, str.getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        cleanup.add( id );

        String disposition="attachment; filename=\"foo bar.txt\"";

        Calendar c = Calendar.getInstance();
        c.add( Calendar.HOUR, 4 );
        Date expiration = c.getTime();
        URL u = this.esu.getShareableUrl( id, expiration, disposition );

        l4j.debug( "Sharable URL: " + u );

        InputStream stream = (InputStream)u.getContent();
        BufferedReader br = new BufferedReader( new InputStreamReader( stream ) );
        String content = br.readLine();
        l4j.debug( "Content: " + content );
        Assert.assertEquals( "URL does not contain proper content",
                             str, content.toString() );
    }

    @Test
    public void testGetShareableUrlWithPathAndDisposition() throws Exception {
        // Create an object with content.
        String str = "Four score and twenty years ago";
        ObjectPath op = new ObjectPath( "/" + rand8char() + ".txt" );
        ObjectId id = this.esu.createObjectOnPath( op, null, null, str.getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        //cleanup.add( op );

        String disposition="attachment; filename=\"foo bar.txt\"";

        Calendar c = Calendar.getInstance();
        c.add( Calendar.HOUR, 4 );
        Date expiration = c.getTime();
        URL u = this.esu.getShareableUrl( op, expiration, disposition );

        l4j.debug( "Sharable URL: " + u );

        InputStream stream = (InputStream)u.getContent();
        BufferedReader br = new BufferedReader( new InputStreamReader( stream ) );
        String content = br.readLine();
        l4j.debug( "Content: " + content );
        Assert.assertEquals( "URL does not contain proper content",
                             str, content.toString() );
    }

    @Test
    public void testGetShareableUrlWithPathAndUTF8Disposition() throws Exception {
        // Create an object with content.
        String str = "Four score and twenty years ago";
        ObjectPath op = new ObjectPath( "/" + rand8char() + ".txt" );
        ObjectId id = this.esu.createObjectOnPath( op, null, null, str.getBytes( "UTF-8" ), "text/plain" );
        Assert.assertNotNull( "null ID returned", id );
        //cleanup.add( op );

        // One cryllic, one accented, and one japanese character
        // RFC5987
        String disposition="attachment; filename=\"no UTF support.txt\"; filename*=UTF-8''" + URLEncoder.encode("бöｼ.txt", "UTF-8");

        Calendar c = Calendar.getInstance();
        c.add( Calendar.HOUR, 4 );
        Date expiration = c.getTime();
        URL u = this.esu.getShareableUrl( op, expiration, disposition );

        l4j.debug( "Sharable URL: " + u );

        InputStream stream = (InputStream)u.getContent();
        BufferedReader br = new BufferedReader( new InputStreamReader( stream ) );
        String content = br.readLine();
        l4j.debug( "Content: " + content );
        Assert.assertEquals( "URL does not contain proper content",
                             str, content.toString() );
    }

    @Test
    public void testGetServiceInformationFeatures() throws Exception {
        ServiceInformation info = this.esu.getServiceInformation();
        l4j.info("Supported features: " + info.getFeatures());

        Assert.assertTrue("Expected at least one feature", info.getFeatures().size()>0);

    }

    @Test
    public void testBug23750() throws Exception {
        byte[] data = new byte[1000];
        Arrays.fill( data, (byte) 0 );
        MetadataList mdList = new MetadataList();
        mdList.addMetadata( new Metadata( "test", null, true ) );

        Checksum sha1 = new Checksum( Checksum.Algorithm.SHA1 );
        ObjectId oid = this.esu.createObject( null, mdList, data, null, sha1 );

        try {
            Extent extent = new Extent( 1000, 1000 );
            Checksum sha0 = new Checksum( Checksum.Algorithm.SHA0 );
            sha0.update( data, 0, 1000 );
            this.esu.updateObject( oid, null, mdList, extent, data, null, sha0 );

            Assert.fail("Should have triggered an exception");
        } catch (EsuException e) {
            // expected
        }
    }

    @Test
    public void testCrudKeys() throws Exception {
        String keyPool = "Test_key-pool#@!$%^..";
        String key = "KEY_TEST";
        String content = "Hello World!";
        byte[] data = content.getBytes("UTF-8");

        ObjectId oid = this.esu.createObjectWithKey( keyPool, key, null, null, data, data.length, "text/plain" );

        Assert.assertNotNull( "Null object ID returned", oid );

        String readContent = new String( this.esu.readObjectWithKey( keyPool, key, null, null ), "UTF-8" );
        Assert.assertEquals( "content mismatch", content, readContent );

        content = "Hello Waldo!";
        data = content.getBytes( "UTF-8" );
        this.esu.updateObjectWithKey( keyPool, key, null, null, null, data, null );

        readContent = new String( this.esu.readObjectWithKey( keyPool, key, null, null ), "UTF-8" );
        Assert.assertEquals( "content mismatch", content, readContent );

        this.esu.deleteObjectWithKey( keyPool, key );

        try {
            this.esu.readObjectWithKey( keyPool, key, null, null );
            Assert.fail("Object still exists");
        } catch (EsuException e) {
            if (e.getHttpCode() != 404) throw e;
        }
    }

    @Test
    public void testIssue9() throws Exception {
        int threadCount = 10;

        final int objectSize = 10 * 1000 * 1000; // size is not a power of 2.
        final MetadataList list = new MetadataList();
        list.addMetadata( new Metadata( "test-data", null, true ) );
        final EsuApi api = esu;
        final List<Identifier> cleanupList = cleanup;
        ThreadPoolExecutor executor = new ThreadPoolExecutor( threadCount, threadCount, 0, TimeUnit.SECONDS,
                                                              new LinkedBlockingQueue<Runnable>() );
        try {
            for ( int i = 0; i < threadCount; i++ ) {
                executor.execute( new Thread() {
                    public void run() {
                        ObjectId oid = api.createObjectFromStream( null, list, new RandomInputStream( objectSize ),
                                                                   objectSize, null );
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
        }
    }
}
