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

import com.emc.atmos.api.Range;
import com.emc.atmos.api.multipart.MultipartEntity;
import com.emc.atmos.api.multipart.MultipartException;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.Arrays;

public class MultipartEntityTest {
    private static final String BOUNDARY = "--bound0508812b8a8ad7";

    @Test
    public void testEmptyStream() throws Exception {
        try {
            MultipartEntity.fromStream( new ByteArrayInputStream( new byte[]{} ), BOUNDARY );
            Assert.fail( "empty stream should throw a parse exception" );
        } catch ( MultipartException e ) {
            // expected
        }
    }

    @Test
    public void testOnePart() throws Exception {
        String eol = "\n";
        String partString = eol +
                            BOUNDARY + eol +
                            "Content-Type: text/plain" + eol +
                            "Content-Range: bytes 27-28/30" + eol +
                            eol +
                            "ag" + eol +
                            BOUNDARY + "--" + eol;
        MultipartEntity entity = MultipartEntity.fromStream( new ByteArrayInputStream( partString.getBytes( "UTF-8" ) ),
                                                             BOUNDARY );
        Assert.assertEquals( "Wrong number of parts", entity.size(), 1 );
        Assert.assertEquals( "Part 1 content type is wrong", entity.get( 0 ).getContentType(), "text/plain" );
        Assert.assertEquals( "Part 1 range is wrong", entity.get( 0 ).getContentRange(), new Range( 27, 28 ) );
        Assert.assertTrue( "Part 1 data is wrong",
                           Arrays.equals( entity.get( 0 ).getData(), "ag".getBytes( "UTF-8" ) ) );
    }

    @Test
    public void testPartsNoCR() throws Exception {
        String eol = "\n";
        String partString = eol +
                            BOUNDARY + eol +
                            "Content-Type: text/plain" + eol +
                            "Content-Range: bytes 27-28/30" + eol +
                            eol +
                            "ag" + eol +
                            BOUNDARY + eol +
                            "Content-Type: text/plain" + eol +
                            "Content-Range: bytes 9-9/30" + eol +
                            eol +
                            "e" + eol +
                            BOUNDARY + eol +
                            "Content-Type: text/plain" + eol +
                            "Content-Range: bytes 5-5/30" + eol +
                            eol +
                            "s" + eol +
                            BOUNDARY + eol +
                            "Content-Type: text/plain" + eol +
                            "Content-Range: bytes 4-4/30" + eol +
                            eol +
                            " " + eol +
                            BOUNDARY + eol +
                            "Content-Type: text/plain" + eol +
                            "Content-Range: bytes 27-29/30" + eol +
                            eol +
                            "ago" + eol +
                            BOUNDARY + "--" + eol;
        MultipartEntity entity = MultipartEntity.fromStream( new ByteArrayInputStream( partString.getBytes( "UTF-8" ) ),
                                                             BOUNDARY );
        Assert.assertEquals( "Wrong number of parts", entity.size(), 5 );
        Assert.assertEquals( "Part 1 content type is wrong", entity.get( 0 ).getContentType(), "text/plain" );
        Assert.assertEquals( "Part 1 range is wrong", entity.get( 0 ).getContentRange(), new Range( 27, 28 ) );
        Assert.assertTrue( "Part 1 data is wrong",
                           Arrays.equals( entity.get( 0 ).getData(), "ag".getBytes( "UTF-8" ) ) );
        Assert.assertEquals( "Part 2 content type is wrong", entity.get( 1 ).getContentType(), "text/plain" );
        Assert.assertEquals( "Part 2 range is wrong", entity.get( 1 ).getContentRange(), new Range( 9, 9 ) );
        Assert.assertTrue( "Part 2 data is wrong",
                           Arrays.equals( entity.get( 1 ).getData(), "e".getBytes( "UTF-8" ) ) );
        Assert.assertEquals( "Part 3 content type is wrong", entity.get( 2 ).getContentType(), "text/plain" );
        Assert.assertEquals( "Part 3 range is wrong", entity.get( 2 ).getContentRange(), new Range( 5, 5 ) );
        Assert.assertTrue( "Part 3 data is wrong",
                           Arrays.equals( entity.get( 2 ).getData(), "s".getBytes( "UTF-8" ) ) );
        Assert.assertEquals( "Part 4 content type is wrong", entity.get( 3 ).getContentType(), "text/plain" );
        Assert.assertEquals( "Part 4 range is wrong", entity.get( 3 ).getContentRange(), new Range( 4, 4 ) );
        Assert.assertTrue( "Part 4 data is wrong",
                           Arrays.equals( entity.get( 3 ).getData(), " ".getBytes( "UTF-8" ) ) );
        Assert.assertEquals( "Part 5 content type is wrong", entity.get( 4 ).getContentType(), "text/plain" );
        Assert.assertEquals( "Part 5 range is wrong", entity.get( 4 ).getContentRange(), new Range( 27, 29 ) );
        Assert.assertTrue( "Part 5 data is wrong",
                           Arrays.equals( entity.get( 4 ).getData(), "ago".getBytes( "UTF-8" ) ) );
    }

    @Test
    public void testPartsCR() throws Exception {
        String eol = "\r\n";
        String partString = eol +
                            BOUNDARY + eol +
                            "Content-Type: text/plain" + eol +
                            "Content-Range: bytes 27-28/30" + eol +
                            eol +
                            "ag" + eol +
                            BOUNDARY + eol +
                            "Content-Type: text/plain" + eol +
                            "Content-Range: bytes 9-9/30" + eol +
                            eol +
                            "e" + eol +
                            BOUNDARY + eol +
                            "Content-Type: text/plain" + eol +
                            "Content-Range: bytes 5-5/30" + eol +
                            eol +
                            "s" + eol +
                            BOUNDARY + eol +
                            "Content-Type: text/plain" + eol +
                            "Content-Range: bytes 4-4/30" + eol +
                            eol +
                            " " + eol +
                            BOUNDARY + eol +
                            "Content-Type: text/plain" + eol +
                            "Content-Range: bytes 27-29/30" + eol +
                            eol +
                            "ago" + eol +
                            BOUNDARY + "--" + eol;
        MultipartEntity entity = MultipartEntity.fromStream( new ByteArrayInputStream( partString.getBytes( "UTF-8" ) ),
                                                             BOUNDARY );
        Assert.assertEquals( "Wrong number of parts", entity.size(), 5 );
        Assert.assertEquals( "Part 1 content type is wrong", entity.get( 0 ).getContentType(), "text/plain" );
        Assert.assertEquals( "Part 1 range is wrong", entity.get( 0 ).getContentRange(), new Range( 27, 28 ) );
        Assert.assertTrue( "Part 1 data is wrong",
                           Arrays.equals( entity.get( 0 ).getData(), "ag".getBytes( "UTF-8" ) ) );
        Assert.assertEquals( "Part 2 content type is wrong", entity.get( 1 ).getContentType(), "text/plain" );
        Assert.assertEquals( "Part 2 range is wrong", entity.get( 1 ).getContentRange(), new Range( 9, 9 ) );
        Assert.assertTrue( "Part 2 data is wrong",
                           Arrays.equals( entity.get( 1 ).getData(), "e".getBytes( "UTF-8" ) ) );
        Assert.assertEquals( "Part 3 content type is wrong", entity.get( 2 ).getContentType(), "text/plain" );
        Assert.assertEquals( "Part 3 range is wrong", entity.get( 2 ).getContentRange(), new Range( 5, 5 ) );
        Assert.assertTrue( "Part 3 data is wrong",
                           Arrays.equals( entity.get( 2 ).getData(), "s".getBytes( "UTF-8" ) ) );
        Assert.assertEquals( "Part 4 content type is wrong", entity.get( 3 ).getContentType(), "text/plain" );
        Assert.assertEquals( "Part 4 range is wrong", entity.get( 3 ).getContentRange(), new Range( 4, 4 ) );
        Assert.assertTrue( "Part 4 data is wrong",
                           Arrays.equals( entity.get( 3 ).getData(), " ".getBytes( "UTF-8" ) ) );
        Assert.assertEquals( "Part 5 content type is wrong", entity.get( 4 ).getContentType(), "text/plain" );
        Assert.assertEquals( "Part 5 range is wrong", entity.get( 4 ).getContentRange(), new Range( 27, 29 ) );
        Assert.assertTrue( "Part 5 data is wrong",
                           Arrays.equals( entity.get( 4 ).getData(), "ago".getBytes( "UTF-8" ) ) );
    }

    @Test
    public void testCorruptedBoundary() throws Exception {
        String eol = "\r\n";
        String partString = eol +
                            BOUNDARY + eol +
                            "Content-Type: text/plain" + eol +
                            "Content-Range: bytes 27-28/30" + eol +
                            eol +
                            "ago" + eol +
                            BOUNDARY + "- -" + eol;
        try {
            MultipartEntity.fromStream( new ByteArrayInputStream( partString.getBytes( "UTF-8" ) ), BOUNDARY );
            Assert.fail( "corrupted boundary should throw a parse exception" );
        } catch ( MultipartException e ) {
            // expected
        }
    }

    @Test
    public void testCorruptedByteRange() throws Exception {
        String eol = "\r\n";
        String partString = eol +
                            BOUNDARY + eol +
                            "Content-Type: text/plain" + eol +
                            "Content-Range: bytes 27-28/30" + eol +
                            eol +
                            "ag" + eol +
                            BOUNDARY + eol +
                            "Content-Type: text/plain" + eol +
                            "Content-Range: bytes 9-9/30" + eol +
                            eol +
                            "e" + eol +
                            BOUNDARY + eol +
                            "Content-Type: text/plain" + eol +
                            "Content-Range: bytes 27-32/30" + eol +
                            eol +
                            "ago" + eol +
                            BOUNDARY + "--" + eol;
        try {
            MultipartEntity.fromStream( new ByteArrayInputStream( partString.getBytes( "UTF-8" ) ), BOUNDARY );
            Assert.fail( "corrupted byte range should throw a parse exception" );
        } catch ( MultipartException e ) {
            // expected
        }
    }
}
